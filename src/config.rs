use serde::{Deserialize, Serialize};
use std::{
    net::IpAddr,
    path::{Path, PathBuf},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub nanonis_ip: IpAddr,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            nanonis_ip: IpAddr::from([127, 0, 0, 1]),
        }
    }
}
pub fn paths() -> Result<(PathBuf, PathBuf), String> {
    let dirs = directories::ProjectDirs::from("", "", "nanonis-proxy")
        .ok_or("Cannot determine application directories")?;
    Ok((
        dirs.config_dir().join("config.json"),
        dirs.data_local_dir().join("logs"),
    ))
}
pub fn load() -> Result<Config, String> {
    let (path, _) = paths()?;
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| e.to_string()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(e.to_string()),
    }
}
pub fn save_to(config: &Config, path: &Path) -> Result<(), String> {
    std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(config).unwrap()).map_err(|e| e.to_string())?;
    std::fs::rename(tmp, path).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn saves_and_replaces_ipv4_and_ipv6_configuration() {
        let dir = std::env::temp_dir().join(format!("nanonis-config-test-{}", std::process::id()));
        let path = dir.join("config.json");
        save_to(&Config::default(), &path).unwrap();
        let ip = "2001:db8::1".parse().unwrap();
        save_to(&Config { nanonis_ip: ip }, &path).unwrap();
        let loaded: Config = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(loaded.nanonis_ip, ip);
        assert!(!path.with_extension("tmp").exists());
        assert!(save_to(&Config::default(), &path.join("impossible.json")).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
