use crate::{
    args::IndicesRewriteRules,
    prometheus::{
        elasticsearch_write_failed_total,
        elasticsearch_write_failed_with_date_total,
        elasticsearch_write_success_total,
        elasticsearch_write_success_with_date_total,
    },
    pulsar::ChannelPayload,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    BulkOperation, BulkParts, Elasticsearch, Error,
};
use regex::RegexSet;
use serde_json::{json, Value};
use std::time::Instant;
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc::Receiver;
use tokio::time;
use url::Url;

pub struct BufferMapValue {
    /// publish time of the pulsar message
    pub publish_time: String,
    /// pulsar raw message
    pub raw_log: String,
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
                let replaced_key = k.replace(".", "_");
                m.insert(replaced_key, transform(v, None, None));
            }
            serde_json::Value::Object(m)
        }
        _ => value.clone(),
    }
}

pub async fn bulkwrite_and_clear(
    client: &Elasticsearch,
    buffer_map: &mut HashMap<String, Vec<BufferMapValue>>,
    time_key: Option<&str>,
) {
    for (index, buf) in buffer_map.iter() {
        if let Err(err) = bulkwrite(client, index, buf, time_key).await {
            log::error!("bulkwrite error: {:?}", err);
        }
    }
    buffer_map.clear();
}

fn split_buffer<'a, 'b>(
    buf: &'a [BufferMapValue], time_key: Option<&'b str>,
) -> (Vec<BulkOperation<serde_json::Value>>, Vec<&'a String>) {
    let mut oks = Vec::new();
    let mut errors = Vec::new();

    for BufferMapValue { publish_time, raw_log, injected_data } in buf.iter() {
        if let Ok(log) = serde_json::from_str(raw_log) {
            let mut log = transform(&log, Some(publish_time), time_key);
            if let Some(injected_data) = injected_data {
                log["__INJECTED_DATA__"] = json!(injected_data.clone());
            }
            oks.push(BulkOperation::index(log).into());
        } else {
            errors.push(raw_log);
        }
    }
    (oks, errors)
}

/// Reads messages from pulsar topic and indexes
/// them into Elasticsearch using the bulk API. An index with explicit mapping
/// is created for search in other examples.
// TODO: Concurrent bulk requests
pub async fn bulkwrite(
    client: &Elasticsearch, index: &str, buf: &[BufferMapValue],
    time_key: Option<&str>,
) -> Result<(), Error> {
    let (topic, date_str) = match split_index_and_date_str(index) {
        Some(v) => v,
        None => {
            log::info!("bad index format: {}", index);
            return Ok(());
        }
    };

    let now = Instant::now();
    // add publish_time as `@timestamp` to raw log by calling transform
    let (body, errors) = split_buffer(buf, time_key);

    let ok_len = body.len();
    log::trace!("serde OK : {}", ok_len);

    for i in errors {
        log::error!("{}", i);
    }

    let response =
        client.bulk(BulkParts::Index(index)).body(body).send().await?;

    let json: Value = response.json().await?;

    if json["errors"].as_bool().unwrap_or(false) {
        let failed_count = json["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| !v["error"].is_null())
            .count();

        // TODO: retry failures
        log::info!("error : {:?}", json);
        log::info!("Errors whilst indexing. Failures: {}", failed_count);
        elasticsearch_write_failed_total(topic, failed_count as u64);
        elasticsearch_write_failed_with_date_total(
            topic,
            date_str,
            failed_count as u64,
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

/// first try to match rewrite rules, then get rewrite index from rules_mapping
fn get_rewrite_index(
    topic: &str, set: Option<&RegexSet>,
    rules_mapping: &Option<Vec<(String, String)>>,
) -> String {
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

/// Read pulsar messages from Receiver and write theme to elasticsearch
pub async fn sink_elasticsearch_loop(
    client: &elasticsearch::Elasticsearch, rx: &mut Receiver<ChannelPayload>,
    buffer_size: usize, flush_interval: u32, time_key: Option<&str>,
    indices_rewrite_rules: Option<IndicesRewriteRules>,
) {
    let mut total = 0;

    let (rules_set, rules_mapping) = build_rules(indices_rewrite_rules);

    // offload raw logs, and merge same logs belong to specific topic, and bulk write to es
    // key is es index, ie. kube-system-2020.01.01, value is (publish_time, data) tuple
    let mut buffer_map = HashMap::<String, Vec<BufferMapValue>>::new();

    // consume messages or timeout
    let mut interval =
        time::interval(Duration::from_millis(flush_interval as u64));

    loop {
        tokio::select! {
            Some(payload) = rx.recv() => {
                 total += 1;

                 // save payload to buffer map
                 let topic = payload.topic;
                 // rewrite index based on config
                 let index = get_rewrite_index(&topic, rules_set.as_ref(), &rules_mapping);
                 let index = format!("{}-{}", &index, &payload.date_str);

                 let publish_time = payload.publish_time;
                 let raw_log = payload.data;
                 let injected_data = payload.injected_data;
                 let buf = buffer_map.entry(index).or_insert_with(Vec::new);
                 buf.push(BufferMapValue{publish_time, raw_log, injected_data});

                 // every buffer_size number of logs, sink to elasticsearch
                 if total % buffer_size == 0 {
                     // sink and clear map
                     bulkwrite_and_clear(client, &mut buffer_map, time_key).await;
                 }
            },
            _ = interval.tick() => {
                log::debug!("{}ms passed", flush_interval);
                if !buffer_map.is_empty() {
                    log::trace!("buffer_map is not emptry, len: {}", buffer_map.len());
                    bulkwrite_and_clear(client, &mut buffer_map, time_key).await;
                }
            }
        }
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
