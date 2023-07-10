use crate::{
    args::{IndicesRewriteRules, RateLimits},
    prometheus::{
        elasticsearch_index_field_count, elasticsearch_write_failed_total,
        elasticsearch_write_failed_with_date_total,
        elasticsearch_write_success_total,
        elasticsearch_write_success_with_date_total,
        pulsar_received_debug_messages_inc_by,
    },
    pulsar::ChannelPayload,
    ratelimiter::RateLimitMap,
    util::{get_app_in_json, get_key_len, is_debug_log, is_debug_log_in_json},
};
use chrono::{DateTime, NaiveDateTime, Utc};
use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    BulkOperation, BulkParts, Elasticsearch, Error,
};
use lazy_static::lazy_static;
use regex::{Regex, RegexSet};
use serde_json::{json, Value};
use std::{collections::HashMap, time::Duration, time::Instant};
use tokio::{sync::mpsc::Receiver, time};
use url::Url;

lazy_static! {
    static ref RE_PARTITION_TOPIC: Regex =
        Regex::new(r#"(.*)-partition-(\d+)"#).unwrap();
}

type BufferMap =
    HashMap<String, HashMap<String, Vec<BulkOperation<serde_json::Value>>>>;

pub struct BufferMapValue {
    /// publish time of the pulsar message
    pub publish_time: String,
    /// pulsar topic
    pub topic: String,
    /// pulsar raw message
    pub raw_log: String,
    /// injected data, ie. UUID, for debug purpose
    pub injected_data: Option<String>,
}

/// Split es index into tuple.
/// The first element is kubernetes namespace, the second is  date_str. i.e (kube-system, 2021.01.01)
pub fn split_index_and_date_str(s: &str) -> Option<(&str, &str)> {
    s.rsplit_once('-')
}

fn f64_to_datetime(t: f64) -> DateTime<Utc> {
    let secs = (t as u64) / 1000;
    let nsecs = (t as u64) % 1000 * 1_000_000
        + ((t - t as u64 as f64) * 1_000_000f64) as u64;
    let naive_datetime =
        NaiveDateTime::from_timestamp(secs as i64, nsecs as u32);
    DateTime::<Utc>::from_utc(naive_datetime, Utc)
}

// convert time_key as datetime string, return None if key's value is not valid
fn get_time_key(
    m: &serde_json::Map<String, serde_json::Value>, time_key: &str,
) -> Option<String> {
    match m.get(time_key) {
        Some(serde_json::Value::Number(_)) => {
            let t = m.get(time_key).unwrap().clone();
            let t = t.as_f64().unwrap();
            let date_time = f64_to_datetime(t);
            Some(date_time.to_rfc3339())
        }
        Some(_) | None => None,
    }
}

/// transform will try to convert raw log into elasticsearch document by replacing dot to
/// underscore for the keys, and will use custome specific key `time_key` as `@timestamp` field
fn transform(
    value: &serde_json::Value, publish_time: Option<&str>,
    time_key: Option<&str>,
) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut m = serde_json::Map::new();
            let mut use_time_key = false;
            if let Some(time_key) = time_key {
                // get time_key(ts) value(type of serde_json::Value::Number), create a timestamp
                if let Some(timestamp) = get_time_key(map, time_key) {
                    // use custom time key
                    m.insert("@timestamp".into(), timestamp.into());
                    use_time_key = true;
                }
            }
            if !use_time_key {
                if let Some(publish_time) = publish_time {
                    m.insert("@timestamp".into(), publish_time.into());
                }
            }

            for (k, v) in map.iter() {
                let replaced_key = k.replace('.', "_");
                m.insert(replaced_key, transform(v, None, None));
            }
            serde_json::Value::Object(m)
        }
        _ => value.clone(),
    }
}

pub async fn bulkwrite_and_clear_new(
    client: &Elasticsearch, buffer_map: &mut BufferMap,
) {
    for (app, map) in buffer_map.iter() {
        for (index, buf) in map.iter() {
            let len = buf.len();
            log::debug!("bulkwrite, app: {app}, index: {index}, buflen: {len}");
            if let Err(err) = bulkwrite_new(client, app, index, buf).await {
                log::error!("bulkwrite error: {app}, {index}, {err:?}");
            }
        }
    }
    buffer_map.clear();
}

fn deserialize_raw_log(raw_log: &str) -> Result<serde_json::Value, Error> {
    let v: serde_json::Value = serde_json::from_str(raw_log)?;
    Ok(v)
}

/// Reads messages from pulsar topic and indexes
/// them into Elasticsearch using the bulk API. An index with explicit mapping
/// is created for search in other examples.
// TODO: Concurrent bulk requests
pub async fn bulkwrite_new(
    client: &Elasticsearch, app: &str, index: &str,
    body: &[BulkOperation<serde_json::Value>],
) -> Result<(), Error> {
    let (topic, date_str) = match split_index_and_date_str(index) {
        Some(v) => v,
        None => {
            log::info!("bad index format, app: {app}, index: {index}");
            return Ok(());
        }
    };

    let now = Instant::now();

    let ok_len = body.len();
    log::trace!("serde OK : {}", ok_len);

    let response =
        client.bulk(BulkParts::Index(index)).body(body.to_vec()).send().await?;

    // calculate avg field count

    let json: Value = response.json().await?;

    if json["errors"].as_bool().unwrap_or(false) {
        let count = json["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| !v["error"].is_null())
            .count();

        // TODO: retry failures
        log::info!("error : {:?}", json);
        log::info!("index error: app: {app}, index: {index} count: {count}",);
        elasticsearch_write_failed_total(topic, count as u64);
        elasticsearch_write_failed_with_date_total(
            topic,
            date_str,
            count as u64,
        );
    }

    let duration = now.elapsed();
    let secs = duration.as_secs_f64();

    let taken = if secs >= 60f64 {
        format!("{}m", secs / 60f64)
    } else {
        format!("{:?}", duration)
    };

    log::trace!("Indexed {} logs in {}", ok_len, taken);

    elasticsearch_write_success_total(topic, ok_len as u64);
    elasticsearch_write_success_with_date_total(topic, date_str, ok_len as u64);

    Ok(())
}

pub fn create_client(addr: &str) -> Result<Elasticsearch, Error> {
    let url = Url::parse(addr)?;

    let conn_pool = SingleNodeConnectionPool::new(url);
    let builder = TransportBuilder::new(conn_pool);

    let transport = builder.build()?;
    Ok(Elasticsearch::new(transport))
}

/// build rule mappings for es index rewrite based on user's config
fn build_rules(
    rules: Option<IndicesRewriteRules>,
) -> (Option<RegexSet>, Option<Vec<(String, String)>>) {
    match rules {
        Some(rules) => {
            let mut rules_mapping = Vec::new();
            let patterns =
                rules.rules.iter().map(|(pat, _)| format!("^{}", pat));
            let set = RegexSet::new(patterns).unwrap();
            for (key, value) in rules.rules.into_iter() {
                rules_mapping.push((key, value));
            }
            (Some(set), Some(rules_mapping))
        }
        None => (None, None),
    }
}

/// Get rewrite index based on user's config
///
/// First try to match input pulsar topic with rewrite rules, then get rewrite index from rules_mapping
///
/// Currently, only patterns like `a.*` is supported
///
/// TODO: return &str instead of String?
fn get_rewrite_index(
    topic: &str, set: Option<&RegexSet>,
    rules_mapping: &Option<Vec<(String, String)>>,
) -> String {
    // topic extraction goes first, then rules matching
    let topic = extract_pulsar_partition_topic(topic);
    if set.is_none() || rules_mapping.is_none() {
        return topic.into();
    }
    let matches: Vec<_> = set.unwrap().matches(topic).into_iter().collect();
    if matches.is_empty() {
        // No rewrite rule matched, keep index name as it is
        return topic.into();
    }

    // Return the first match, and return rewritten index
    let matched_index = matches[0];
    let (_, rule_target) = &rules_mapping.as_ref().unwrap()[matched_index];
    rule_target.replace(".*", "")
}

/// Extrace topic part from pulsar partitioned topic
/// return input if topic is not a partitioned topic
fn extract_pulsar_partition_topic(topic: &str) -> &str {
    if let Some(cap) = RE_PARTITION_TOPIC.captures(topic) {
        if let Some(topic) = cap.get(1).map(|m| m.as_str()) {
            return topic;
        }
    }
    topic
}

/// Read pulsar messages from Receiver and write to elasticsearch
///
/// Here is the main logic of the sinking progress:
///
/// 1) read messages from pulsar topics through channel receiver(sent by consuming loop)
/// 2) build BufferMap
///   a) deserialize messages, because we need to group message by `app` key, then to index into elasticsearch in batch
///   b) apply rewrite rules to index name
///   c) inject injected_data if provided
///   d) extract app to group messages by app
///   e) group messages, fill BufferMap with serde_json::Value
/// 3) sink data in BufferMap to elasticsearch
pub async fn sink_elasticsearch_loop(
    client: &elasticsearch::Elasticsearch, rx: &mut Receiver<ChannelPayload>,
    buffer_size: usize, flush_interval: u32, time_key: Option<&str>,
    indices_rewrite_rules: Option<IndicesRewriteRules>,
    debug_log_regexset: Option<&RegexSet>, rate_limits: Option<RateLimits>,
) {
    let mut total = 0;

    let (rules_set, rules_mapping) = build_rules(indices_rewrite_rules);

    // offload raw logs, and merge same logs belong to specific topic, and bulk write to es
    // key is es index, ie. kube-system-2020.01.01, value is (publish_time, data) tuple
    let mut buffer_map = BufferMap::new();

    // consume messages or timeout
    let mut interval =
        time::interval(Duration::from_millis(flush_interval as u64));

    // init rate limiter based on config
    let rate_limiter = RateLimitMap::from_config(rate_limits);

    loop {
        tokio::select! {
            Some(payload) = rx.recv() => {
                total += 1;

                build_buffer_map(&mut buffer_map, &payload, rules_set.as_ref(), &rules_mapping, time_key, debug_log_regexset, &rate_limiter);

                // every buffer_size number of logs, sink to elasticsearch
                if total % buffer_size == 0 {
                    // sink and clear map
                    bulkwrite_and_clear_new(client, &mut buffer_map).await;
                }
            },
            _ = interval.tick() => {
               log::debug!("{}ms passed", flush_interval);
               if !buffer_map.is_empty() {
                   log::trace!("buffer_map is not emptry, len: {}", buffer_map.len());
                   bulkwrite_and_clear_new(client, &mut buffer_map).await;
               }
            }
        }
    }
}

/// Build BufferMap struct from raw log
fn build_buffer_map(
    buffer_map: &mut BufferMap, payload: &ChannelPayload,
    rules_set: Option<&RegexSet>,
    rules_mapping: &Option<Vec<(String, String)>>, time_key: Option<&str>,
    debug_log_regexset: Option<&RegexSet>, rate_limiter: &Option<RateLimitMap>,
) {
    let ChannelPayload {
        ref topic,
        ref publish_time,
        ref date_str,
        ref data,
        ref injected_data,
    } = payload;
    // get rewrite index based on user config
    let index = get_rewrite_index(topic, rules_set, rules_mapping);
    let index = format!("{}-{}", &index, date_str);

    let mut errors = Vec::new();

    if let Ok(log) = deserialize_raw_log(data) {
        // increase prometheus counter, when log level is debug, then check raw log using regexset
        // TODO: optimize by collect counter under topic and call inc_by only once
        if is_debug_log_in_json(&log) || is_debug_log(data, debug_log_regexset)
        {
            pulsar_received_debug_messages_inc_by(topic, 1);
        }
        let mut log = transform(&log, Some(publish_time), time_key);
        if let Some(injected_data) = injected_data {
            log["__INJECTED_DATA__"] = json!(injected_data.clone());
        }

        // use app key in log, otherwise use default value
        let app = get_app_in_json(&log).unwrap_or("__DEFAULT_APP__");

        // record field_count metric
        let field_count = get_key_len(&log);
        elasticsearch_index_field_count(&index, app, field_count as u64);

        let map = buffer_map.entry(app.into()).or_insert_with(HashMap::new);

        // load rate limit config and apply rate limit logic
        if let Some(rate_limiter) = rate_limiter {
            match rate_limiter.check_key(app) {
                Ok(_) => {
                    let buf = map.entry(index).or_insert_with(Vec::new);
                    buf.push(BulkOperation::index(log).into());
                }
                Err(err) => {
                    log::debug!("rate limit triggered, error: {err}");
                }
            }
        } else {
            let buf = map.entry(index).or_insert_with(Vec::new);
            buf.push(BulkOperation::index(log).into());
        }
    } else {
        log::debug!("deserialize log error: {data}");
        errors.push(data);
    }
}

#[test]
fn trasform_ts_as_time_key() {
    let s = r#"
    {"ts": 1626057993894.9734, "name": "hi"}
    "#;
    let v: serde_json::Value = serde_json::from_str(s).unwrap();
    let publish_time = "2046-10-04T03:33:33.233323332+08:00";
    let res = transform(&v, None, Some("ts"));
    assert!(res["@timestamp"].to_string().starts_with("\"2021-07-12T02:46:33"));
    let res = transform(&v, Some(publish_time), None);
    assert!(res["@timestamp"]
        .to_string()
        .starts_with(&format!("\"{}", publish_time)));
}

#[test]
fn test_get_rewrite_index() {
    // {"rules": {"app-.*":"app", "etcd.*": "eks-logstash", "istio-system.*": "eks-logstash", "kube-system.*": "eks-logstash", "kong.*": "eks-logstash"}}
    let rules = vec![
        ("app-biz.*", "app"),
        ("app-mutants.*", "mutants"),
        ("app-biz1.*", "app"),
        ("app-.*", "app"),
        ("etcd.*", "eks-logstash"),
        ("istio-system.*", "eks-logstash"),
        ("kube-system.*", "eks-logstash"),
        ("kong.*", "eks-logstash"),
        ("pular.*", "pulsar"),
    ];
    let rules = rules.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
    let indices_rewrite_rules = Some(IndicesRewriteRules { rules });
    let (rules_set, rules_mapping) = build_rules(indices_rewrite_rules);

    let topics = vec![
        ("app-biz", "app"),
        ("app-mutants", "mutants"),
        ("app-biz1", "app"),
        ("app-biz2", "app"),
        ("app-foo", "app"),
        ("logstash", "logstash"),
        ("etcd", "eks-logstash"),
        ("kube-system", "eks-logstash"),
        ("istio-system", "eks-logstash"),
        ("kong", "eks-logstash"),
        ("pulsar-partition-0", "pulsar"),
    ];

    for (topic, rewrite_index) in topics {
        let index =
            get_rewrite_index(topic, rules_set.as_ref(), &rules_mapping);
        assert_eq!(index, rewrite_index);
    }
}
