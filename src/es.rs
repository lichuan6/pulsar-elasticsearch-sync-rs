use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    BulkOperation, BulkParts, Elasticsearch, Error,
};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Instant;
use url::Url;

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
            let p = transform(&p.unwrap(), Some(publish_time));
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

fn transform(
    value: &serde_json::Value, publish_time: Option<&str>,
) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut m = serde_json::Map::new();
            if let Some(publish_time) = publish_time {
                m.insert("@timestamp".into(), publish_time.into());
            }
            for (k, v) in map.iter() {
                let replaced_key = k.replace(".", "_");
                m.insert(replaced_key, transform(&v, None));
            }
            serde_json::Value::Object(m)
        }
        _ => value.clone(),
    }
}

pub async fn bulkwrite_and_clear(
    client: &Elasticsearch,
    buffer_map: &mut HashMap<String, Vec<(String, String)>>,
) {
    for (index, buf) in buffer_map.iter() {
        let _ = bulkwrite(client, index, buf).await;
    }
    buffer_map.clear();
}

fn split_buffer(
    buf: &[(String, String)],
) -> (Vec<BulkOperation<serde_json::Value>>, Vec<&String>) {
    let mut oks = Vec::new();
    let mut errors = Vec::new();

    for (publish_time, raw_log) in buf.iter() {
        if let Ok(log) = serde_json::from_str(raw_log) {
            let log = transform(&log, Some(publish_time));
            oks.push(BulkOperation::index(log).into());
        } else {
            errors.push(raw_log);
        }
    }
    (oks, errors)
}

pub async fn bulkwrite(
    client: &Elasticsearch, index: &str, buf: &[(String, String)],
) -> Result<(), Error> {
    let now = Instant::now();
    // add publish_time as `@timestamp` to raw log by calling transform
    let (body, errors) = split_buffer(buf);

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
        log::info!("Errors whilst indexing. Failures: {}", failed_count)
    }

    let duration = now.elapsed();
    let secs = duration.as_secs_f64();

    let taken = if secs >= 60f64 {
        format!("{}m", secs / 60f64)
    } else {
        format!("{:?}", duration)
    };

    log::debug!("Indexed {} logs in {}", ok_len, taken);

    Ok(())
}

pub fn create_client(addr: &str) -> Result<Elasticsearch, Error> {
    let url = Url::parse(addr)?;

    let conn_pool = SingleNodeConnectionPool::new(url);
    let builder = TransportBuilder::new(conn_pool);

    let transport = builder.build()?;
    Ok(Elasticsearch::new(transport))
}
