pub mod etcd;
pub mod consul;

pub use etcd::{EtcdClient, EtcdConfig, EtcdService, ServiceRegistration, ServiceInstance, EtcdStats, EtcdError};
pub use consul::{ConsulClient, ConsulConfig, ConsulService, ConsulAgent, ConsulCatalog, ConsulHealth, ConsulStats};
