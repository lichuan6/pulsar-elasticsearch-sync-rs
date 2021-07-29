use crate::prometheus::{
    elasticsearch_write_failed_total,
    elasticsearch_write_failed_with_date_total,
    elasticsearch_write_success_total,
    elasticsearch_write_success_with_date_total,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    BulkOperation, BulkParts, Elasticsearch, Error,
};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Instant;
use url::Url;

pub fn split_index_and_date_str(s: &str) -> Option<(&str, &str)> {
    s.rsplit_once('-')
}

/// Reads messages from pulsar topic and indexes
/// them into Elasticsearch using the bulk API. An index with explicit mapping
/// is created for search in other examples.
// TODO: Concurrent bulk requests
pub async fn index_json_from_str(
    client: &Elasticsearch, index: &str, publish_time: &str, data: &[&str],
) -> Result<(), Error> {
    let body: Vec<BulkOperation<_>> = data
        .iter()
        .filter_map(|p| serde_json::from_str(p).ok())
        .map(|p: Option<serde_json::Value>| {
            let p = transform(&p.unwrap(), Some(publish_time), None);
            BulkOperation::index(p).into()
        })
        .collect();

    let error_data: Vec<String> = data
        .iter()
        .filter(|p| serde_json::from_str::<serde_json::Value>(p).is_err())
        .map(|p| p.to_string())
        .collect();

    log::debug!("serde OK : {}, ERR : {}", body.len(), error_data.len());

    for i in error_data {
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
        log::info!("Errors whilst indexing. Failures: {}", failed_count)
    }

    Ok(())
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
                m.insert(replaced_key, transform(&v, None, None));
            }
            serde_json::Value::Object(m)
        }
        _ => value.clone(),
    }
}

pub async fn bulkwrite_and_clear(
    client: &Elasticsearch,
    buffer_map: &mut HashMap<String, Vec<(String, String)>>,
    time_key: Option<&str>,
) {
    for (index, buf) in buffer_map.iter() {
        let _ = bulkwrite(client, index, buf, time_key).await;
    }
    buffer_map.clear();
}

fn split_buffer<'a, 'b>(
    buf: &'a [(String, String)], time_key: Option<&'b str>,
) -> (Vec<BulkOperation<serde_json::Value>>, Vec<&'a String>) {
    let mut oks = Vec::new();
    let mut errors = Vec::new();

    for (publish_time, raw_log) in buf.iter() {
        if let Ok(log) = serde_json::from_str(raw_log) {
            let log = transform(&log, Some(publish_time), time_key);
            oks.push(BulkOperation::index(log).into());
        } else {
            errors.push(raw_log);
        }
    }
    (oks, errors)
}

pub async fn bulkwrite(
    client: &Elasticsearch, index: &str, buf: &[(String, String)],
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
    log::debug!("serde OK : {}", ok_len);

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

    log::debug!("Indexed {} logs in {}", ok_len, taken);

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
