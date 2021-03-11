use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use futures::TryStreamExt;
use pulsar::{
    Authentication, Consumer, ConsumerOptions, DeserializeMessage, Payload, Pulsar, SubType,
    TokioExecutor,
};
use regex::Regex;
use serde;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, string::FromUtf8Error, time::Instant};
use structopt::StructOpt;

#[derive(Serialize, Deserialize)]
struct Data;

impl DeserializeMessage for Data {
    type Output = Result<String, FromUtf8Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        // serde_json::from_slice(&payload.data)
        String::from_utf8(payload.data.clone())
    }
}
use pulsar_elasticsearch_sync_rs::{args::Opt, es};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Opt::from_args();

    let addr = env::var("PULSAR_ADDRESS")
        .ok()
        .unwrap_or_else(|| opt.pulsar_addr.clone());

    let es_addr = env::var("ELASTICSEARCH_ADDRESS")
        .ok()
        .unwrap_or_else(|| opt.elasticsearch_addr.clone());

    let client = es::create_client(&es_addr).unwrap();

    let mut builder = Pulsar::builder(addr, TokioExecutor);

    if let Ok(token) = env::var("PULSAR_TOKEN") {
        let authentication = Authentication {
            name: "token".to_string(),
            data: token.into_bytes(),
        };

        builder = builder.with_auth(authentication);
    }

    let pulsar_namespace = env::var("PULSAR_NAMESPACE")
        .ok()
        .unwrap_or_else(|| opt.pulsar_namespace.clone());

    let pulsar: Pulsar<_> = builder.build().await?;

    let mut consumer: Consumer<Data, _> = pulsar
        .consumer()
        // NOTE:
        // 默认采用 public/default namespace，所以这里 regex 写 r".*" 就可以匹配
        // public/default 下所有 topics
        .with_lookup_namespace(pulsar_namespace)
        .with_topic_regex(Regex::new(&opt.topic_regex).unwrap())
        .with_consumer_name("consumer-pulsar-elasticsearch-sync-rs")
        .with_subscription_type(SubType::Shared)
        .with_subscription("pulsar-elasticsearch-sync-rs")
        // get latest messages(Some(0)), earliest is Some(1)
        .with_options(ConsumerOptions {
            initial_position: Some(0),
            durable: Some(false),
            ..Default::default()
        })
        .with_batch_size(opt.batch_size)
        .build()
        .await?;

    let size = opt.buffer_size;
    let mut total = 0;

    log::info!("pulsar elssticserach sync started, begin to consume messages...");

    // TODO: use &str as key?
    let mut buffer_map = HashMap::<String, (Vec<String>, Instant)>::new();
    while let Some(msg) = consumer.try_next().await? {
        total += 1;
        consumer.ack(&msg).await?;
        // log::info!("metadata: {:?}", msg.metadata());
        let data = match msg.deserialize() {
            Ok(data) => data,
            Err(e) => {
                log::error!("could not deserialize message: {:?}", e);
                break;
            }
        };

        if data.len() == 0 {
            continue;
        }

        // build es index name based on pulsar messages topic and publish_time
        // es index name = `topic+publish_date`, i.e. test-2021.01.01

        let topic = extract_topic_part(&msg.topic);
        let (es_timestamp, date_str) = es_timestamp_and_date(msg.metadata().publish_time);
        let index = format!("{}-{}", topic, date_str);
        log::trace!("index: {}, MSG: {}, Len: {}", index, &data, &data.len());

        let buf = if let Some(buf) = buffer_map.get_mut(&index) {
            buf
        } else {
            buffer_map.insert(index.clone(), (Vec::new(), Instant::now()));
            buffer_map.get_mut(&index).unwrap()
        };
        buf.0.push(data);

        if total % size == 0 {
            es::bulkwrite(&client, &index, &es_timestamp, buf).await;
        } else {
            // TODO:
            // clean outdated buffer_map
            let now = Instant::now();
            if now.elapsed().as_secs() - buf.1.elapsed().as_secs() > 10 {
                es::bulkwrite(&client, &index, &es_timestamp, buf).await;
            }
        }
    }

    Ok(())
}

fn es_timestamp_and_date(publish_time: u64) -> (String, String) {
    let publish_time = publish_time / 1000;
    let publish_time_nsec = publish_time % 1000;
    let naive_datetime =
        NaiveDateTime::from_timestamp(publish_time as i64, publish_time_nsec as u32);
    let date_time: DateTime<Local> = Local.from_local_datetime(&naive_datetime).unwrap();
    (
        date_time.to_rfc3339(),
        naive_datetime.format("%Y.%m.%d").to_string(),
    )
}

// The input topic has the format of: `persistent://public/default/test`.
// Here we try to parse the last part of input string, ie. `test`
fn extract_topic_part(topic: &str) -> &str {
    let v: Vec<_> = topic.split("/").collect();
    assert!(v.len() == 5);
    v[4]
}
