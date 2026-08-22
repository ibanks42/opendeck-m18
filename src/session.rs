use std::{collections::HashMap, sync::Arc};

use tokio_util::sync::CancellationToken;

pub enum SessionMatch<'a> {
    Token(&'a Arc<CancellationToken>),
    Generation(u64),
}

enum SessionPhase<D, O> {
    Reserved,
    Registering {
        device: Option<D>,
        output: Option<O>,
    },
    Published {
        device: D,
        output: O,
    },
    Cleaning,
}

struct SessionRegistration<D, O> {
    token: Arc<CancellationToken>,
    generation: Option<u64>,
    phase: SessionPhase<D, O>,
}

pub struct RemovedSession<D, O> {
    pub token: Arc<CancellationToken>,
    pub device: Option<D>,
    pub output: Option<O>,
    pub cleanup_pending: bool,
}

pub enum Removal<D, O> {
    Ready(RemovedSession<D, O>),
    RegistrationPending,
}

pub struct SessionRegistry<D, O> {
    registrations: HashMap<String, SessionRegistration<D, O>>,
}

impl<D, O> Default for SessionRegistry<D, O> {
    fn default() -> Self {
        Self {
            registrations: HashMap::new(),
        }
    }
}

impl<D, O> SessionRegistry<D, O> {
    pub fn insert_task(&mut self, id: String, token: Arc<CancellationToken>) {
        self.registrations.insert(
            id,
            SessionRegistration {
                token,
                generation: None,
                phase: SessionPhase::Reserved,
            },
        );
    }

    pub fn reserve(&mut self, id: String, generation: u64) -> Option<Arc<CancellationToken>> {
        if self.registrations.contains_key(&id) {
            return None;
        }

        let token = Arc::new(CancellationToken::new());
        self.registrations.insert(
            id,
            SessionRegistration {
                token: token.clone(),
                generation: Some(generation),
                phase: SessionPhase::Reserved,
            },
        );
        Some(token)
    }

    pub fn is_current(&self, id: &str, token: &Arc<CancellationToken>) -> bool {
        self.registrations
            .get(id)
            .is_some_and(|registered| Arc::ptr_eq(&registered.token, token))
    }

    pub fn begin_registration(
        &mut self,
        id: &str,
        token: &Arc<CancellationToken>,
        device: D,
        output: O,
    ) -> Result<(), (D, O)> {
        let Some(registered) = self.registrations.get_mut(id) else {
            return Err((device, output));
        };
        if !Arc::ptr_eq(&registered.token, token)
            || token.is_cancelled()
            || !matches!(registered.phase, SessionPhase::Reserved)
        {
            return Err((device, output));
        }

        registered.phase = SessionPhase::Registering {
            device: Some(device),
            output: Some(output),
        };
        Ok(())
    }

    pub fn finish_registration(&mut self, id: &str, token: &Arc<CancellationToken>) -> bool {
        let Some(registered) = self.registrations.get_mut(id) else {
            return false;
        };
        if !Arc::ptr_eq(&registered.token, token) || token.is_cancelled() {
            return false;
        }

        let SessionPhase::Registering { device, output } = &mut registered.phase else {
            return false;
        };
        let (Some(device), Some(output)) = (device.take(), output.take()) else {
            return false;
        };
        registered.phase = SessionPhase::Published { device, output };
        true
    }

    pub fn discard_registration(&mut self, id: &str, token: &Arc<CancellationToken>) {
        let should_remove = self.registrations.get(id).is_some_and(|registered| {
            Arc::ptr_eq(&registered.token, token)
                && matches!(registered.phase, SessionPhase::Registering { .. })
        });
        if should_remove {
            self.registrations.remove(id);
            token.cancel();
        }
    }

    pub fn output(&self, id: &str) -> Option<&O> {
        match &self.registrations.get(id)?.phase {
            SessionPhase::Registering {
                output: Some(output),
                ..
            }
            | SessionPhase::Published { output, .. } => Some(output),
            _ => None,
        }
    }

    pub fn begin_removal(&mut self, id: &str, expected: SessionMatch<'_>) -> Option<Removal<D, O>> {
        let registered = self.registrations.get_mut(id)?;
        let matches = match expected {
            SessionMatch::Token(token) => Arc::ptr_eq(&registered.token, token),
            SessionMatch::Generation(generation) => registered.generation == Some(generation),
        };
        if !matches {
            return None;
        }

        registered.token.cancel();
        let token = registered.token.clone();
        match std::mem::replace(&mut registered.phase, SessionPhase::Cleaning) {
            SessionPhase::Reserved => {
                self.registrations.remove(id);
                Some(Removal::Ready(RemovedSession {
                    token,
                    device: None,
                    output: None,
                    cleanup_pending: false,
                }))
            }
            SessionPhase::Registering { .. } => {
                registered.phase = SessionPhase::Registering {
                    device: None,
                    output: None,
                };
                Some(Removal::RegistrationPending)
            }
            SessionPhase::Published { device, output } => Some(Removal::Ready(RemovedSession {
                token,
                device: Some(device),
                output: Some(output),
                cleanup_pending: true,
            })),
            SessionPhase::Cleaning => {
                registered.phase = SessionPhase::Cleaning;
                None
            }
        }
    }

    pub fn finish_cleanup(&mut self, id: &str, token: &Arc<CancellationToken>) {
        let should_remove = self.registrations.get(id).is_some_and(|registered| {
            Arc::ptr_eq(&registered.token, token)
                && matches!(registered.phase, SessionPhase::Cleaning)
        });
        if should_remove {
            self.registrations.remove(id);
        }
    }

    pub fn cancel_all(&self) {
        for registration in self.registrations.values() {
            registration.token.cancel();
        }
    }
}
