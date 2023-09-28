use std::{fmt::Debug, time::Instant};

use abcperf::MessageDestination;
use anyhow::Result;
use minbft::{output::TimeoutRequest, Output, PeerMessage};
use shared_ids::ClientId;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;
use usig::Usig;

type ToReplicaChannel<A, P, U> = mpsc::Sender<(MessageDestination, PeerMessage<A, P, U>)>;

pub(super) struct OutputHandler<P, U: Usig> {
    to_replica: ToReplicaChannel<U::Attestation, P, U::Signature>,
    to_client: mpsc::Sender<(ClientId, P)>,
    set_timeout: mpsc::Sender<(TimeoutRequest, Instant)>,
    hello_done_send: Option<oneshot::Sender<()>>,
}

impl<P, U: Usig> OutputHandler<P, U> {
    pub(super) fn new(
        to_replica: ToReplicaChannel<U::Attestation, P, U::Signature>,
        to_client: mpsc::Sender<(ClientId, P)>,
        set_timeout: mpsc::Sender<(TimeoutRequest, Instant)>,
    ) -> (Self, oneshot::Receiver<()>) {
        let (hello_done_send, hello_done_recv) = oneshot::channel();

        (
            Self {
                to_replica,
                to_client,
                set_timeout,
                hello_done_send: Some(hello_done_send),
            },
            hello_done_recv,
        )
    }

    pub(super) fn is_hello_done(&self) -> bool {
        self.hello_done_send.is_none()
    }
}

impl<P: Send + Sync + Debug + 'static, U: Usig> OutputHandler<P, U>
where
    U::Attestation: Send + Sync + Debug + 'static,
    U::Signature: Send + Sync + Debug + 'static,
{
    pub(super) async fn handle_output(
        &mut self,
        Output {
            broadcasts,
            responses,
            errors,
            timeout_requests,
            ready_for_client_requests,
            primary: _,
        }: Output<P, U>,
    ) -> Result<()> {
        let now = Instant::now();
        let broadcasts = Vec::from(broadcasts);
        for message in broadcasts {
            self.to_replica
                .send((MessageDestination::Broadcast, message))
                .await?;
        }
        let responses = Vec::from(responses);
        for response in responses {
            self.to_client.send(response).await?;
        }
        // TODO: how should errors be handled?
        for error in errors.iter() {
            warn!("error during processing: {}", error);
        }
        let timeout_requests = Vec::from(timeout_requests);
        for timeout_request in timeout_requests {
            self.set_timeout.send((timeout_request, now)).await?;
        }
        if ready_for_client_requests {
            if let Some(hello_done_send) = self.hello_done_send.take() {
                hello_done_send.send(()).map_err(|_| {
                    anyhow::anyhow!("hello done channel receiver should not be dropped")
                })?;
            }
        }

        Ok(())
    }
}
