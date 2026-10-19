use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(alias = "nanonis_ip")]
    pub nanonis_host: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            nanonis_host: "127.0.0.1".into(),
        }
    }
}
/// Validate the address without resolving DNS, so offline hosts can be saved.
pub fn parse_host(value: &str) -> Result<String, String> {
    let host = value.trim();
    if host.parse::<std::net::IpAddr>().is_ok()
        || (!host.is_empty()
            && host.bytes().any(|b| b.is_ascii_alphanumeric())
            && host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b)))
    {
        Ok(host.into())
    } else {
        Err("Enter a hostname or IPv4/IPv6 address, without a port or URL".into())
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
    fn saves_hosts_and_loads_legacy_ip_configuration() {
        let dir = std::env::temp_dir().join(format!("nanonis-config-test-{}", std::process::id()));
        let path = dir.join("config.json");
        save_to(&Config::default(), &path).unwrap();
        for host in ["127.0.0.1", "2001:db8::1", "nanonis.lab.local"] {
            save_to(
                &Config {
                    nanonis_host: host.into(),
                },
                &path,
            )
            .unwrap();
            let loaded: Config = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            assert_eq!(loaded.nanonis_host, host);
        }
        let legacy: Config = serde_json::from_str(r#"{"nanonis_ip":"192.168.1.20"}"#).unwrap();
        assert_eq!(legacy.nanonis_host, "192.168.1.20");
        assert!(!path.with_extension("tmp").exists());
        assert!(save_to(&Config::default(), &path.join("impossible.json")).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn accepts_hosts_and_ips_without_urls_or_ports() {
        for host in [
            "localhost",
            "nanonis.lab.local",
            "lab-pc",
            "127.0.0.1",
            "::1",
            "2001:db8::1",
        ] {
            assert_eq!(parse_host(&format!(" {host} ")).unwrap(), host);
        }
        for host in [
            "",
            " ",
            "...",
            "host name",
            "https://nanonis",
            "nanonis:6501",
            "127.0.0.1:6501",
        ] {
            assert!(parse_host(host).is_err(), "{host}");
        }
    }
}
