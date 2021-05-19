use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use futures::TryStreamExt;
use pulsar::{
    Authentication, Consumer, ConsumerOptions, DeserializeMessage, Payload,
    Pulsar, SubType, TokioExecutor,
};
use pulsar_elasticsearch_sync_rs::prometheus::run_warp_server;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env,
    string::FromUtf8Error,
    time::{Duration, Instant},
};
use structopt::StructOpt;
#[derive(Serialize, Deserialize)]
struct Data;

impl DeserializeMessage for Data {
    type Output = Result<String, FromUtf8Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        String::from_utf8(payload.data.clone())
    }
}
use pulsar_elasticsearch_sync_rs::{args::Opt, es};

fn run_metric_server() {
    tokio::task::spawn(run_warp_server());
}

async fn create_consumer(
    pulsar: &Pulsar<TokioExecutor>, name: &str, namespace: &str,
    topic_regex: &str, subscription_name: &str, batch_size: u32,
) -> Result<Consumer<Data, TokioExecutor>, pulsar::Error> {
    Ok(pulsar
        .consumer()
        .with_lookup_namespace(namespace)
        .with_topic_regex(Regex::new(topic_regex).unwrap())
        //.with_consumer_name("consumer-pulsar-elasticsearch-sync-rs")
        .with_consumer_name(name)
        .with_subscription_type(SubType::Shared)
        .with_subscription(subscription_name)
        .with_options(ConsumerOptions {
            // get latest messages(Some(0)), earliest is Some(1)
            initial_position: Some(0),
            durable: Some(false),
            ..Default::default()
        })
        .with_batch_size(batch_size)
        .build()
        .await?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    run_metric_server();

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

    let consumer_name = "consumer-pulsar-elasticsearch-sync-rs";
    let subscription_name = "pulsar-elasticsearch-sync-rs";
    log::info!(
        "pulsar elasticserach sync started, begin to consume messages..."
    );

    consume_loop(
        &pulsar,
        &client,
        consumer_name,
        subscription_name,
        &pulsar_namespace,
        &opt.topic_regex,
        opt.batch_size,
        opt.buffer_size,
    )
    .await?;

    Ok(())
}

async fn consume_loop(
    pulsar: &Pulsar<TokioExecutor>, client: &elasticsearch::Elasticsearch,
    name: &str, subscription_name: &str, namespace: &str, topic_regex: &str,
    batch_size: u32, buffer_size: usize,
) -> Result<(), pulsar::Error> {
    let mut consumer: Consumer<Data, _> = create_consumer(
        &pulsar,
        name,
        namespace,
        topic_regex,
        subscription_name,
        batch_size,
    )
    .await?;

    let mut total = 0;
    let mut buffer_map = HashMap::new();

    while let Ok(msg) = consumer.try_next().await {
        if let Some(msg) = msg {
            total += 1;
            consumer.ack(&msg).await?;
            let data = match msg.deserialize() {
                Ok(data) => data,
                Err(e) => {
                    log::error!("could not deserialize message: {:?}", e);
                    break;
                }
            };

            if data.is_empty() {
                continue;
            }

            // build es index nameased on pulsar messages topic and
            // publish_time es index name =
            // `topic+publish_date`, i.e. test-2021.01.01
            let topic = extract_topic_part(&msg.topic);
            let (es_timestamp, date_str) =
                es_timestamp_and_date(msg.metadata().publish_time);
            let index = format!("{}-{}", topic, date_str);
            log::trace!(
                "index: {}, MSG: {}, Len: {}",
                index,
                &data,
                &data.len()
            );

            let buf = buffer_map
                .entry(index.clone())
                .or_insert_with(|| (Vec::new(), Instant::now()));
            buf.0.push(data);

            if total % buffer_size == 0 {
                es::bulkwrite(&client, &index, &es_timestamp, buf).await;
            } else {
                // TODO:
                // clean outdated buffer_map
                let now = Instant::now();
                if now.duration_since(buf.1).as_secs() > 10 {
                    es::bulkwrite(&client, &index, &es_timestamp, buf).await;
                }
            }
        } else {
            // NOTE: try_next return None if stream is closed, reconnect
            println!("fail to connect to pulsar broker, sleep 10s ...");
            tokio::time::sleep(Duration::from_secs(10)).await;

            consumer = match create_consumer(
                &pulsar,
                name,
                namespace,
                topic_regex,
                subscription_name,
                batch_size,
            )
            .await
            {
                Ok(consumer) => consumer,
                Err(e) => {
                    // sleep and continue
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    println!("create consumer error : {:?}", e);
                    continue;
                }
            }
        }
    }

    Ok(())
}

fn es_timestamp_and_date(publish_time: u64) -> (String, String) {
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

// The input topic has the format of: `persistent://public/default/test`.
// Here we try to parse the last part of input string, ie. `test`
fn extract_topic_part(topic: &str) -> &str {
    let v: Vec<_> = topic.split('/').collect();
    assert!(v.len() == 5);
    v[4]
}
