use std::{collections::HashMap, mem, pin::Pin, time::Instant};

use anyhow::{Ok, Result};
use minbft::{
    output::TimeoutRequest,
    timeout::{StopClass, Timeout, TimeoutType},
};
use tokio::{
    select,
    sync::mpsc,
    task::JoinHandle,
    time::{self, Sleep},
};
use tracing::Instrument;

pub(super) struct TimeoutHandler {
    earliest_to_handle: Option<(Pin<Box<Sleep>>, TimeoutEntry)>,
    current_timeouts: HashMap<TimeoutType, TimeoutEntry>,
}

type StartTuple = (
    JoinHandle<Result<()>>,
    mpsc::Sender<(TimeoutRequest, Instant)>,
    mpsc::Receiver<TimeoutType>,
);

impl TimeoutHandler {
    fn new() -> Self {
        Self {
            earliest_to_handle: None,
            current_timeouts: HashMap::new(),
        }
    }

    pub(super) fn start() -> StartTuple {
        let (set_timeout_send, set_timeout_recv) = mpsc::channel(1000);
        let (timeouts_send, timeouts_recv) = mpsc::channel(1000);

        (
            tokio::spawn(
                Self::new()
                    .run(set_timeout_recv, timeouts_send)
                    .in_current_span(),
            ),
            set_timeout_send,
            timeouts_recv,
        )
    }

    async fn run(
        mut self,
        mut set_timeout: mpsc::Receiver<(TimeoutRequest, Instant)>,
        timeouts: mpsc::Sender<TimeoutType>,
    ) -> Result<()> {
        loop {
            if let Some((ref mut sleep, _)) = &mut self.earliest_to_handle {
                select! {
                    biased;
                    maybe_timeout_req = set_timeout.recv() => {
                        if let Some((new_timeout_req, time_of_arrival)) = maybe_timeout_req {
                            if let TimeoutRequest::Start(new_timeout) = new_timeout_req {
                                if self.current_timeouts.contains_key(&new_timeout.timeout_type) {
                                    continue;
                                }
                                let new_entry = TimeoutEntry::from((new_timeout, time_of_arrival));
                                self.current_timeouts.insert(new_timeout.timeout_type, new_entry);
                                self.set_earliest_to_handle();
                            }
                            else if let TimeoutRequest::Stop(timeout) = new_timeout_req {
                                if let Some(current_timeout) = self.current_timeouts.get(&timeout.timeout_type) {
                                    if current_timeout.stop_class == timeout.stop_class {
                                        self.current_timeouts.remove(&timeout.timeout_type);
                                        self.set_earliest_to_handle();
                                    }
                                } else {
                                    continue;
                                }
                            }
                        } else {
                            // quit
                            return Ok(());
                        }

                    }
                    () = sleep => {
                        if let Some((_, to_handle)) = mem::take(
                            &mut self.earliest_to_handle,
                        ) {
                            self.current_timeouts.remove(&to_handle.timeout_type);
                            self.set_earliest_to_handle();
                            timeouts.send(to_handle.timeout_type).await?;
                        }
                    }
                }
            } else if let Some((new_timeout_req, time_of_arrival)) = set_timeout.recv().await {
                assert!(self.current_timeouts.is_empty());
                if let TimeoutRequest::Start(timeout) = new_timeout_req {
                    let new_entry = TimeoutEntry::from((timeout, time_of_arrival));
                    self.current_timeouts
                        .insert(new_entry.timeout_type, new_entry);
                    self.set_earliest_to_handle();
                }
            } else {
                // quit
                return Ok(());
            }
        }
    }

    fn set_earliest_to_handle(&mut self) {
        let earliest_to_handle = self
            .current_timeouts
            .values()
            .min_by(|x, y| x.timeout_deadline.cmp(&y.timeout_deadline))
            .copied();

        self.earliest_to_handle = earliest_to_handle.map(|e| e.sleep());
    }
}

#[derive(Debug, Clone, Copy)]
struct TimeoutEntry {
    timeout_type: TimeoutType,
    timeout_deadline: Instant,
    stop_class: StopClass,
}

impl TimeoutEntry {
    fn sleep(self) -> (Pin<Box<Sleep>>, TimeoutEntry) {
        (
            Box::pin(time::sleep_until(self.timeout_deadline.into())),
            self,
        )
    }
}

impl From<(Timeout, Instant)> for TimeoutEntry {
    fn from((timeout, time_of_arrival): (Timeout, Instant)) -> Self {
        Self {
            timeout_type: timeout.timeout_type,
            timeout_deadline: time_of_arrival + timeout.duration,
            stop_class: timeout.stop_class,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exit() {
        let (timeout_handler_task, set_timeout, _timeouts) = TimeoutHandler::start();

        drop(set_timeout);
        timeout_handler_task.await.unwrap().unwrap();
    }
}
