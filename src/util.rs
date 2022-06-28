use crate::{args::NamespaceFilter, pulsar::Data};
use anyhow::Result;
use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use regex::RegexSet;
use std::collections::HashMap;
use std::convert::AsRef;
use std::env;
use std::ffi::OsStr;

/// Build es index based on pulsar messages topic and
/// publish_time,
/// es index name has this format: `topic+publish_date`, i.e. test-2021.01.01
pub fn es_index_and_timestamp(
    msg: &pulsar::consumer::Message<Data>,
) -> (String, String) {
    let topic = extract_topic_part(&msg.topic);
    let (es_timestamp, date_str) =
        publish_timestamp_and_date(msg.metadata().publish_time);
    let index = format!("{}-{}", topic, date_str);
    (index, es_timestamp)
}

/// topic is used to apply indices rewrite rules
/// publish_time is used to fill es @timestamp
/// date_str to generate es index(date part, i.e test-2021.01.01)
pub fn topic_publish_time_and_date(
    msg: &pulsar::consumer::Message<Data>,
) -> (String, String, String) {
    let topic = extract_topic_part(&msg.topic);
    let (publish_time, date_str) =
        publish_timestamp_and_date(msg.metadata().publish_time);
    (topic.into(), publish_time, date_str)
}

/// Build message publish time and date
/// publish_time is the publish time of pulsar message
/// date can be considered as date part in elasticsearch index
pub fn publish_timestamp_and_date(publish_time: u64) -> (String, String) {
    let publish_time = publish_time / 1000;
    let publish_time_nsec = publish_time % 1000;
    let naive_datetime = NaiveDateTime::from_timestamp(
        publish_time as i64,
        publish_time_nsec as u32,
    );
    let date_time: DateTime<Local> =
        Local.from_local_datetime(&naive_datetime).unwrap();
    (date_time.to_rfc3339(), naive_datetime.format("%Y.%m.%d").to_string())
}

/// Extract topic part from pular topic URI: {type}://{tenant}/{namespace}/{topic}
/// The input topic has the format of: `persistent://public/default/test`.
/// Here we try to parse the last part of input string, ie. `test`
pub fn extract_topic_part(topic: &str) -> &str {
    let v: Vec<_> = topic.split('/').collect();
    assert!(v.len() == 5);
    v[4]
}

// Create a regex::RegexSet from patterns
pub fn create_regexset(
    patterns: Option<Vec<String>>,
) -> Result<Option<RegexSet>, regex::Error> {
    if let Some(patterns) = patterns {
        match RegexSet::new(patterns) {
            Ok(set) => return Ok(Some(set)),
            Err(e) => {
                log::info!("create regexset failed: {}", e);
                return Err(e);
            }
        }
    }
    Ok(None)
}

// Create a regex::RegexSet from patterns
pub fn create_namespace_filters(
    namespace_filters_vec: Option<Vec<NamespaceFilter>>,
) -> Result<Option<HashMap<String, RegexSet>>, regex::Error> {
    if let Some(namespace_filters_vec) = namespace_filters_vec {
        let mut filters = HashMap::new();
        for i in namespace_filters_vec {
            // create regexset for namespace
            if !i.filters.is_empty() {
                if let Ok(Some(regexset)) =
                    create_regexset(Some(i.filters.clone()))
                {
                    log::info!(
                        "creating regexset for namespace {:?} from {:?}",
                        i.namespace,
                        i.filters
                    );
                    filters.insert(i.namespace, regexset);
                }
            }
        }
        if !filters.is_empty() {
            return Ok(Some(filters));
        }
    }
    Ok(None)
}

// Create a regex::Regex from pattern
pub fn create_regex(
    pattern: Option<String>,
) -> Result<Option<regex::Regex>, regex::Error> {
    if let Some(pattern) = pattern {
        match regex::Regex::new(&pattern) {
            Ok(r) => return Ok(Some(r)),
            Err(e) => {
                log::info!(
                    "create regex pattern error: {}, source: {}",
                    e,
                    pattern,
                );
                return Err(e);
            }
        }
    }
    Ok(None)
}

/// get value from env, if there's no key in env, use the value computed from the closure
pub fn env_or_else<K, F>(key: K, f: F) -> String
where
    K: AsRef<OsStr>,
    F: FnOnce() -> String,
{
    env::var(key).ok().unwrap_or_else(f)
}

/// get value from env, if there's no key in env, use input value
pub fn env_or<K>(key: K, value: String) -> String
where
    K: AsRef<OsStr>,
{
    env::var(key).unwrap_or(value)
}

/// Check if raw_log contains debug log, using regexset to do pattern match
pub fn is_debug_log(raw_log: &str, regexset: Option<&RegexSet>) -> bool {
    if let Some(regexset) = regexset {
        if regexset.is_match(raw_log) {
            return true;
        }
    }
    false
}

/// Return key len of serde_json::Value when type is serde_json::Value::Object
pub fn get_key_len(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::Object(map) => map.keys().len(),
        _ => 0,
    }
}

/// Check if json log contains debug level log, checking if level key exists and value equels to `debug`
pub fn is_debug_log_in_json(v: &serde_json::Value) -> bool {
    match v.get("level") {
        Some(serde_json::Value::String(ref level)) => level == "debug",
        _ => false,
    }
}

/// Check if json log contains debug level log, checking if level key exists and value equels to `debug`
pub fn get_app_in_json(v: &serde_json::Value) -> Option<&str> {
    match v.get("app") {
        Some(serde_json::Value::String(ref app_value)) => Some(app_value),
        _ => None,
    }
}

#[test]
fn test_is_debug_log() {
    let debug_log_patterns = vec![r"\[DEBU\]".into(), r"\[Gin-Debug\]".into()];
    let regexset = create_regexset(Some(debug_log_patterns)).unwrap().unwrap();
    let logs = vec!["[DEBU]: xxx", "[Gin-Debug]: xxx"];
    for log in logs {
        assert!(is_debug_log(log, Some(&regexset)));
    }

    let logs_notmatch = vec!["DEBU: xxx", "Gin-Debug: xxx"];
    for log in logs_notmatch {
        assert!(!is_debug_log(log, Some(&regexset)));
    }
}
