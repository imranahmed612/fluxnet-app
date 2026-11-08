use serde_json::Value;

pub fn configuration_is_ready(config: Option<&Value>) -> bool {
    let Some(config) = config.and_then(Value::as_object) else {
        return false;
    };

    let has_public_key = config
        .get("pubkey")
        .and_then(Value::as_str)
        .is_some_and(|key| !key.trim().is_empty());
    let has_secure_endpoints = config
        .get("endpoints")
        .and_then(Value::as_array)
        .is_some_and(|endpoints| {
            !endpoints.is_empty()
                && endpoints.iter().all(|endpoint| {
                    endpoint
                        .as_str()
                        .is_some_and(|url| url.starts_with("https://"))
                })
        });
    let requires_signed_version =
        config.get("requireSignedVersion").and_then(Value::as_bool) == Some(true);
    let permits_no_downgrades =
        config.get("allowDowngrades").and_then(Value::as_bool) != Some(true);
    let permits_secure_transport_only = [
        "dangerousInsecureTransportProtocol",
        "dangerousAcceptInvalidCerts",
        "dangerousAcceptInvalidHostnames",
    ]
    .iter()
    .all(|key| config.get(*key).and_then(Value::as_bool) != Some(true));

    has_public_key
        && has_secure_endpoints
        && requires_signed_version
        && permits_no_downgrades
        && permits_secure_transport_only
}

#[tauri::command]
pub fn is_updater_configured(app: tauri::AppHandle) -> bool {
    configuration_is_ready(app.config().plugins.0.get("updater"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ready_config() -> Value {
        json!({
            "pubkey": "public verification key",
            "endpoints": ["https://updates.example.invalid/latest.json"],
            "requireSignedVersion": true,
            "allowDowngrades": false
        })
    }

    #[test]
    fn updater_requires_a_public_key_and_secure_endpoint() {
        let config = ready_config();
        assert!(configuration_is_ready(Some(&config)));
        assert!(!configuration_is_ready(None));

        let mut missing_key = ready_config();
        missing_key["pubkey"] = json!("");
        assert!(!configuration_is_ready(Some(&missing_key)));

        let mut missing_endpoint = ready_config();
        missing_endpoint["endpoints"] = json!([]);
        assert!(!configuration_is_ready(Some(&missing_endpoint)));

        let mut insecure_endpoint = ready_config();
        insecure_endpoint["endpoints"] = json!(["http://updates.example.invalid/latest.json"]);
        assert!(!configuration_is_ready(Some(&insecure_endpoint)));
    }

    #[test]
    fn updater_requires_signed_versions_and_rejects_dangerous_options() {
        let mut unsigned_version = ready_config();
        unsigned_version["requireSignedVersion"] = json!(false);
        assert!(!configuration_is_ready(Some(&unsigned_version)));

        let mut downgrade = ready_config();
        downgrade["allowDowngrades"] = json!(true);
        assert!(!configuration_is_ready(Some(&downgrade)));

        let mut invalid_certificates = ready_config();
        invalid_certificates["dangerousAcceptInvalidCerts"] = json!(true);
        assert!(!configuration_is_ready(Some(&invalid_certificates)));
    }
}
