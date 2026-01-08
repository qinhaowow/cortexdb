use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Session {
    id: String,
    created_at: Instant,
    last_accessed: Instant,
    timeout: Duration,
    state: Arc<Mutex<SessionState>>,
}

#[derive(Debug)]
pub struct SessionState {
    active: bool,
    transaction_id: Option<String>,
    client_info: Option<ClientInfo>,
}

#[derive(Debug, Clone)]
pub struct ClientInfo {
    client_id: String,
    client_version: String,
    remote_addr: String,
}

impl Session {
    pub fn new(id: String, timeout: Duration) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            timeout,
            state: Arc::new(Mutex::new(SessionState {
                active: true,
                transaction_id: None,
                client_info: None,
            })),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn is_active(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.active && !self.is_expired()
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() - self.last_accessed > self.timeout
    }

    pub fn renew(&mut self) {
        self.last_accessed = Instant::now();
    }

    pub fn close(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.active = false;
    }

    pub fn set_transaction_id(&self, transaction_id: Option<String>) {
        let mut state = self.state.lock().unwrap();
        state.transaction_id = transaction_id;
    }

    pub fn transaction_id(&self) -> Option<String> {
        let state = self.state.lock().unwrap();
        state.transaction_id.clone()
    }

    pub fn set_client_info(&self, client_info: ClientInfo) {
        let mut state = self.state.lock().unwrap();
        state.client_info = Some(client_info);
    }

    pub fn client_info(&self) -> Option<ClientInfo> {
        let state = self.state.lock().unwrap();
        state.client_info.clone()
    }
}

#[derive(Debug, Clone)]
pub struct SessionManager {
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    default_timeout: Duration,
}

use std::collections::HashMap;

impl SessionManager {
    pub fn new(default_timeout: Duration) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            default_timeout,
        }
    }

    pub fn create_session(&self, id: String) -> Session {
        let session = Session::new(id.clone(), self.default_timeout);
        let mut sessions = self.sessions.lock().unwrap();
        sessions.insert(id, session.clone());
        session
    }

    pub fn get_session(&self, id: &str) -> Option<Session> {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get(id) {
            if session.is_expired() {
                sessions.remove(id);
                None
            } else {
                let mut session = session.clone();
                session.renew();
                sessions.insert(id.to_string(), session.clone());
                Some(session)
            }
        } else {
            None
        }
    }

    pub fn remove_session(&self, id: &str) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.remove(id);
    }

    pub fn cleanup_expired(&self) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|_, session| !session.is_expired());
    }

    pub fn session_count(&self) -> usize {
        let sessions = self.sessions.lock().unwrap();
        sessions.len()
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new(Duration::from_secs(3600))
    }
}
