use std::net::SocketAddr;
use std::path::PathBuf;

/// Layer 0: Discovery Service
/// Simple bootnode-based discovery (DHT support can be added later)
pub struct DiscoveryService {
    /// Bootnode addresses
    bootnodes: Vec<SocketAddr>,
}

impl DiscoveryService {
    /// Create a new discovery service (loads bootnodes from file or env)
    pub fn new() -> Self {
        // Try to load from environment variable first
        let bootnodes = if let Ok(env_bootnodes) = std::env::var("POS_BOOTNODES") {
            env_bootnodes
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .filter_map(|s| s.parse().ok())
                .collect()
        } else if let Ok(addrs) = Self::load_bootnodes_from_file("bootnodes.txt") {
            addrs
        } else {
            // Default to localhost for development
            vec![
                "127.0.0.1:8081".parse().unwrap(),
                "127.0.0.1:8082".parse().unwrap(),
            ]
        };

        Self { bootnodes }
    }

    /// Create discovery service with explicit bootnode list
    pub fn with_bootnodes(bootnode_addrs: Vec<String>) -> Self {
        let bootnodes = bootnode_addrs
            .iter()
            .filter_map(|addr| addr.parse().ok())
            .collect();

        Self { bootnodes }
    }

    /// Load bootnodes from config file (one address per line)
    pub fn load_bootnodes_from_file(path: impl Into<PathBuf>) -> Result<Vec<SocketAddr>, String> {
        let path = path.into();
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read bootnode file: {}", e))?;

        let bootnodes = content
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.trim().starts_with('#'))
            .filter_map(|line| line.trim().parse().ok())
            .collect();

        Ok(bootnodes)
    }

    /// Find peers responsible for a specific sector (hash ring position)
    /// For now, returns all bootnodes (DHT routing can be added later)
    pub async fn find_peers(&self, _sector: u64) -> Vec<SocketAddr> {
        self.bootnodes.clone()
    }

    /// Get all bootnode addresses
    pub fn get_bootnodes(&self) -> Vec<SocketAddr> {
        self.bootnodes.clone()
    }
}

/// Simple bootnode-only discovery (no DHT overhead)
pub struct BootnodeDiscovery {
    bootnodes: Vec<SocketAddr>,
}

impl BootnodeDiscovery {
    /// Create from list of IP:PORT strings
    pub fn new(bootnode_addrs: Vec<String>) -> Self {
        let bootnodes = bootnode_addrs
            .iter()
            .filter_map(|addr| addr.parse().ok())
            .collect();

        Self { bootnodes }
    }

    /// Load from config file (one address per line)
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read bootnode file: {}", e))?;

        let bootnodes = content
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.trim().starts_with('#'))
            .filter_map(|line| line.trim().parse().ok())
            .collect();

        Ok(Self { bootnodes })
    }

    /// Load from environment variable (comma-separated)
    pub fn from_env(var_name: &str) -> Result<Self, String> {
        let env_value = std::env::var(var_name)
            .map_err(|_| format!("Environment variable {} not set", var_name))?;

        let bootnodes = env_value
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.parse().ok())
            .collect();

        Ok(Self { bootnodes })
    }

    /// Get all bootnode addresses
    pub fn get_bootnodes(&self) -> Vec<SocketAddr> {
        self.bootnodes.clone()
    }

    /// Find peers (returns all bootnodes for simplicity)
    pub async fn find_peers(&self, _sector: u64) -> Vec<SocketAddr> {
        self.bootnodes.clone()
    }
}