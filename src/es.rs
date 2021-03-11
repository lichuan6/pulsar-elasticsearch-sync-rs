// NOTE: 从 elaticsearch/examples/index_questions_answers/main.rs copy 过来做了一些修改
// use serde::{Deserialize, Serialize};
// use serde_json::json;

// use clap::{App, Arg};

use std::time::Instant;

use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    BulkOperation, BulkParts, Elasticsearch, Error,
};
use serde_json::Value;
use url::Url;

/// Reads messages from pulsar topic and indexes
/// them into Elasticsearch using the bulk API. An index with explicit mapping is created
/// for search in other examples.
///
// TODO: Concurrent bulk requests
pub async fn index_json_from_str(
    client: &Elasticsearch,
    index: &str,
    publish_time: &str,
    data: &[&str],
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

    let response = client
        .bulk(BulkParts::Index(index))
        .body(body)
        .send()
        .await?;

    let json: Value = response.json().await?;

    if json["errors"].as_bool().unwrap_or(false) {
        let failed: Vec<&Value> = json["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| !v["error"].is_null())
            .collect();

        // TODO: retry failures
        log::info!("error : {:?}", json);
        // println!("Errors whilst indexing. Failures: {}", failed.len());
        log::info!("Errors whilst indexing. Failures: {}", failed.len())
    }

    Ok(())
}

fn transform(value: &serde_json::Value, publish_time: Option<&str>) -> serde_json::Value {
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

pub async fn bulkwrite(
    client: &Elasticsearch,
    index: &str,
    publish_time: &str,
    buf: &mut (Vec<String>, Instant),
) {
    let now = Instant::now();
    let data: Vec<&str> = buf.0.iter().map(AsRef::as_ref).collect();
    match index_json_from_str(&client, &index, publish_time, &data).await {
        Ok(_) => {}
        Err(e) => {
            log::error!("Index json error : {:?}", e)
        }
    }
    let duration = now.elapsed();
    let secs = duration.as_secs_f64();

    let taken = if secs >= 60f64 {
        format!("{}m", secs / 60f64)
    } else {
        format!("{:?}", duration)
    };

    log::debug!("Indexed {} logs in {}", data.len(), taken);

    buf.0.clear();
    buf.1 = Instant::now();
}

pub fn create_client(addr: &str) -> Result<Elasticsearch, Error> {
    // fn cluster_addr() -> String {
    //     match std::env::var("ELASTICSEARCH_URL") {
    //         Ok(server) => server,
    //         Err(_) => DEFAULT_ADDRESS.into(),
    //     }
    // }

    // let url = Url::parse(cluster_addr().as_ref()).unwrap();
    let url = Url::parse(addr)?;

    let conn_pool = SingleNodeConnectionPool::new(url);
    let builder = TransportBuilder::new(conn_pool);

    let transport = builder.build()?;
    Ok(Elasticsearch::new(transport))
}
