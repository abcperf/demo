use std::{collections::HashMap, fmt::Debug, marker::PhantomData, num::NonZeroU64, time::Duration};

use abcperf::{
    atomic_broadcast::{AtomicBroadcast, AtomicBroadcastChannels, AtomicBroadcastConfiguration},
    MessageType,
};
use anyhow::{Error, Result};
use minbft::{timeout::TimeoutType, Config as MinBftConfig, MinBft, PeerMessage, RequestPayload};
use output_handler::OutputHandler;
use serde::{Deserialize, Serialize};
use shared_ids::{ClientId, ReplicaId, RequestId};
use timeout_handler::TimeoutHandler;
use tokio::{select, sync::mpsc, task::JoinHandle};
use tracing::Instrument;
use usig::Usig;

mod output_handler;
mod timeout_handler;

pub struct ABCperfMinbft<P, U> {
    phantom_data: PhantomData<P>,
    usig: U,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConfigurationExtension {
    batch_timeout: f64,
    max_batch_size: Option<NonZeroU64>,
    backoff_multiplier: f64,
    initial_timeout_duration: f64,
    checkpoint_period: NonZeroU64,
}

impl From<ConfigurationExtension> for HashMap<String, String> {
    fn from(config: ConfigurationExtension) -> Self {
        let ConfigurationExtension {
            batch_timeout,
            max_batch_size,
            backoff_multiplier,
            initial_timeout_duration,
            checkpoint_period,
        } = config;

        let mut map = HashMap::new();

        map.insert("batch_timeout".to_owned(), batch_timeout.to_string());
        if let Some(max_batch_size) = max_batch_size {
            map.insert("max_batch_size".to_owned(), max_batch_size.to_string());
        }
        map.insert(
            "backoff_multiplier".to_owned(),
            backoff_multiplier.to_string(),
        );
        map.insert(
            "initial_timeout_duration".to_owned(),
            initial_timeout_duration.to_string(),
        );
        map.insert(
            "checkpoint_period".to_owned(),
            checkpoint_period.to_string(),
        );

        map
    }
}

impl<P, U> ABCperfMinbft<P, U> {
    pub fn new(usig: U) -> Self {
        Self {
            usig,
            phantom_data: PhantomData::default(),
        }
    }
}

impl<
        P: RequestPayload + 'static + Unpin + Send + Sync + AsRef<RequestId>,
        U: Usig + 'static + Send,
    > AtomicBroadcast for ABCperfMinbft<P, U>
where
    U::Attestation: Serialize + for<'a> Deserialize<'a> + Debug + Unpin + Send + Clone + Sync,
    U::Signature: Debug + Serialize + for<'a> Deserialize<'a> + Unpin + Send + Clone + Sync,
{
    type Config = ConfigurationExtension;
    type ReplicaMessage = PeerMessage<U::Attestation, P, U::Signature>;
    type Transaction = P;
    type Decision = (ClientId, P);

    fn start(
        self,
        config: AtomicBroadcastConfiguration<Self::Config>,
        channels: AtomicBroadcastChannels<Self::ReplicaMessage, Self::Transaction, Self::Decision>,
        ready_for_clients: impl Send + 'static + FnOnce(),
    ) -> JoinHandle<Result<(), Error>> {
        let config = MinBftConfig {
            id: config.replica_id,
            n: config.n,
            t: config.t,
            max_batch_size: config
                .extension
                .max_batch_size
                .map(|n| n.try_into().expect("number should not get that big")),
            batch_timeout: Duration::from_secs_f64(config.extension.batch_timeout),
            initial_timeout_duration: Duration::from_secs_f64(
                config.extension.initial_timeout_duration,
            ),
            checkpoint_period: config.extension.checkpoint_period,
        };
        let (minbft, initial_output) = MinBft::new(self.usig, config).unwrap();

        let AtomicBroadcastChannels {
            incoming_replica_messages,
            outgoing_replica_messages,
            requests,
            responses,
        } = channels;

        tokio::spawn(
            async move {
                let (timeout_handler_task, set_timeout, timeouts) = TimeoutHandler::start();

                let (mut output_handler, hello_done_recv) =
                    OutputHandler::new(outgoing_replica_messages, responses, set_timeout);

                output_handler.handle_output(initial_output).await.unwrap();

                let minbft_task = tokio::spawn(
                    run_minbft(
                        minbft,
                        incoming_replica_messages,
                        requests,
                        timeouts,
                        output_handler,
                    )
                    .in_current_span(),
                );

                hello_done_recv
                    .await
                    .expect("hello done channel sender should not be dropped");
                ready_for_clients();

                minbft_task.await.unwrap().unwrap();
                timeout_handler_task.await.unwrap().unwrap();
                Ok(())
            }
            .in_current_span(),
        )
    }
}

type IncomingPeerMessageChannel<A, P, S> =
    mpsc::Receiver<(MessageType, ReplicaId, PeerMessage<A, P, S>)>;

async fn run_minbft<P: Send + Sync + RequestPayload + 'static, U: Usig>(
    mut minbft: MinBft<P, U>,
    mut replica_messages: IncomingPeerMessageChannel<U::Attestation, P, U::Signature>,
    mut client_messages: mpsc::Receiver<(ClientId, P)>,
    mut timeouts: mpsc::Receiver<TimeoutType>,
    mut output_handler: OutputHandler<P, U>,
) -> Result<()>
where
    U::Attestation: Send + Sync + Clone + Debug + 'static,
    U::Signature: Send + Sync + Clone + Serialize + Debug + 'static,
{
    let mut replica_open = true;
    let mut client_open = true;

    loop {
        let output = select! {
            msg = replica_messages.recv(), if replica_open => {
                if let Some((_msg_type, replica, message)) = msg {
                    minbft.handle_peer_message(replica, message)
                } else {
                    replica_open = false;
                    continue
                }
            }
            msg = client_messages.recv(), if output_handler.is_hello_done() && client_open => {
                if let Some((client_id, request)) = msg {
                    minbft.handle_client_message(client_id, request)
                } else {
                    client_open = false;
                    continue
                }
            }
            Some(timeout_type) = timeouts.recv(), if replica_open || client_open => {
                minbft.handle_timeout(timeout_type)
            }
            else => break
        };

        output_handler.handle_output(output).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use shared_ids::{AnyId, RequestId};

    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct DummyPayload {}

    impl RequestPayload for DummyPayload {
        fn id(&self) -> RequestId {
            *self.as_ref()
        }

        fn verify(&self, _id: ClientId) -> Result<()> {
            Ok(())
        }
    }

    impl AsRef<RequestId> for DummyPayload {
        fn as_ref(&self) -> &RequestId {
            &RequestId::FIRST
        }
    }

    #[tokio::test]
    async fn exit_replica_first() {
        let abcperf_minbft = ABCperfMinbft::<DummyPayload, _>::new(usig::noop::UsigNoOp::default());

        let (from_replica_send, from_replica_recv) = mpsc::channel(1000);
        let (to_replica_send, _to_replica_recv) = mpsc::channel(1000);
        let (from_client_send, from_client_recv) = mpsc::channel(1000);
        let (to_client_send, _to_client_recv) = mpsc::channel(1000);

        let task = abcperf_minbft.start(
            AtomicBroadcastConfiguration {
                replica_id: ReplicaId::from_u64(0),
                extension: ConfigurationExtension {
                    batch_timeout: 1_000_000f64,
                    max_batch_size: None,
                    backoff_multiplier: 1f64,
                    initial_timeout_duration: 1_000_000f64,
                    checkpoint_period: NonZeroU64::new(2).unwrap(),
                },
                n: 1.try_into().expect("> 0"),
                t: 0,
            },
            AtomicBroadcastChannels {
                incoming_replica_messages: from_replica_recv,
                outgoing_replica_messages: to_replica_send,
                requests: from_client_recv,
                responses: to_client_send,
            },
            || {},
        );

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!task.is_finished());

        drop(from_replica_send);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!task.is_finished());

        drop(from_client_send);
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(task.is_finished());

        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn exit_client_first() {
        let abcperf_minbft = ABCperfMinbft::<DummyPayload, _>::new(usig::noop::UsigNoOp::default());

        let (from_replica_send, from_replica_recv) = mpsc::channel(1000);
        let (to_replica_send, _to_replica_recv) = mpsc::channel(1000);
        let (from_client_send, from_client_recv) = mpsc::channel(1000);
        let (to_client_send, _to_client_recv) = mpsc::channel(1000);

        let task = abcperf_minbft.start(
            AtomicBroadcastConfiguration {
                replica_id: ReplicaId::from_u64(0),
                extension: ConfigurationExtension {
                    batch_timeout: 1_000_000f64,
                    max_batch_size: None,
                    backoff_multiplier: 1f64,
                    initial_timeout_duration: 1_000_000f64,
                    checkpoint_period: NonZeroU64::new(2).unwrap(),
                },
                n: 1.try_into().expect("> 0"),
                t: 0,
            },
            AtomicBroadcastChannels {
                incoming_replica_messages: from_replica_recv,
                outgoing_replica_messages: to_replica_send,
                requests: from_client_recv,
                responses: to_client_send,
            },
            || {},
        );

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!task.is_finished());

        drop(from_client_send);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!task.is_finished());

        drop(from_replica_send);
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(task.is_finished());

        task.await.unwrap().unwrap();
    }
}
