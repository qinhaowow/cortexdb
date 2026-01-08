#[derive(Debug, Clone, PartialEq)]
pub enum ConsistencyLevel {
    Serializable,
    RepeatableRead,
    ReadCommitted,
    ReadUncommitted,
    EventuallyConsistent,
}

#[derive(Debug, thiserror::Error)]
pub enum ConsistencyError {
    #[error("Invalid consistency level")]
    InvalidLevel,
    #[error("Consistency violation")]
    Violation,
    #[error("Timeout waiting for consistency")]
    Timeout,
}

pub struct ConsistencyManager {
    default_level: ConsistencyLevel,
}

impl ConsistencyManager {
    pub fn new(default_level: ConsistencyLevel) -> Self {
        Self { default_level }
    }

    pub fn validate_level(&self, level: &ConsistencyLevel) -> Result<(), ConsistencyError> {
        match level {
            ConsistencyLevel::Serializable
            | ConsistencyLevel::RepeatableRead
            | ConsistencyLevel::ReadCommitted
            | ConsistencyLevel::ReadUncommitted
            | ConsistencyLevel::EventuallyConsistent => Ok(()),
            _ => Err(ConsistencyError::InvalidLevel),
        }
    }

    pub fn get_isolation_level(&self, consistency: &ConsistencyLevel) -> IsolationLevel {
        match consistency {
            ConsistencyLevel::Serializable => IsolationLevel::Serializable,
            ConsistencyLevel::RepeatableRead => IsolationLevel::RepeatableRead,
            ConsistencyLevel::ReadCommitted => IsolationLevel::ReadCommitted,
            ConsistencyLevel::ReadUncommitted => IsolationLevel::ReadUncommitted,
            ConsistencyLevel::EventuallyConsistent => IsolationLevel::ReadUncommitted,
        }
    }

    pub fn get_default_level(&self) -> &ConsistencyLevel {
        &self.default_level
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    Serializable,
    RepeatableRead,
    ReadCommitted,
    ReadUncommitted,
}

impl IsolationLevel {
    pub fn is_stricter_than(&self, other: &Self) -> bool {
        use IsolationLevel::*;
        match (self, other) {
            (Serializable, _) => true,
            (RepeatableRead, ReadCommitted) | (RepeatableRead, ReadUncommitted) => true,
            (ReadCommitted, ReadUncommitted) => true,
            _ => false,
        }
    }
}