use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use futures::TryStreamExt;
use pulsar::{
    Authentication, Consumer, ConsumerOptions, DeserializeMessage, Payload,
    Pulsar, SubType, TokioExecutor,
};
use pulsar_elasticsearch_sync_rs::prometheus::run_warp_server;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, string::FromUtf8Error, time::Duration};
use structopt::StructOpt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time;

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

type ChannelPayload = (String, String, String);

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

    let (tx, mut rx) = channel::<ChannelPayload>(2048);

    let buffer_size = opt.buffer_size;
    let interval = opt.flush_interval;
    tokio::spawn(async move {
        // sink log to elasticsearch
        sink_elasticsearch_loop(&client, &mut rx, buffer_size, interval).await;
    });

    consume_loop(
        &pulsar,
        consumer_name,
        subscription_name,
        &pulsar_namespace,
        &opt.topic_regex,
        opt.batch_size,
        tx,
    )
    .await?;

    Ok(())
}

async fn sink_elasticsearch_loop(
    client: &elasticsearch::Elasticsearch, rx: &mut Receiver<ChannelPayload>,
    buffer_size: usize, flush_interval: u32,
) {
    let mut total = 0;

    // offload raw logs, and merge same logs belong to specific topic, and bulk write to es
    // TODO: use HashMap<&str, (&str, &str)> ?
    let mut buffer_map = HashMap::<String, Vec<(String, String)>>::new();

    // consume messages or timeout
    let mut interval =
        time::interval(Duration::from_millis(flush_interval as u64));

    loop {
        tokio::select! {
            Some(payload) = rx.recv() => {
                 total += 1;

                 // save payload to buffer map
                 let (index, es_timestamp, data) = payload;
                 let buf = buffer_map.entry(index).or_insert_with(Vec::new);
                 buf.push((es_timestamp, data));

                 // every buffer_size number of logs, sink to elasticsearch
                 if total % buffer_size == 0 {
                     // sink and clear map
                     es::bulkwrite_and_clear(&client, &mut buffer_map).await;
                 }
            },
            _ = interval.tick() => {
                log::debug!("{}ms passed", flush_interval);
                if !buffer_map.is_empty() {
                    log::debug!("buffer_map is not emptry, len: {}", buffer_map.len());
                    es::bulkwrite_and_clear(&client, &mut buffer_map).await;
                }
            }
        }
    }
}

async fn consume_loop(
    pulsar: &Pulsar<TokioExecutor>, name: &str, subscription_name: &str,
    namespace: &str, topic_regex: &str, batch_size: u32,
    tx: Sender<ChannelPayload>,
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

    // TODO: export consumed messages count metrics
    // i.e let mut total = 0;
    while let Ok(msg) = consumer.try_next().await {
        if let Some(msg) = msg {
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

            let (index, es_timestamp) = index_and_es_timestamp(&msg);
            let payload = (index, es_timestamp, data);

            // Send messages to channel, for sinking to elasticsearch
            let _ = tx.send(payload).await;
        } else {
            // NOTE: try_next return None if stream is closed, reconnect
            log::info!("fail to connect to pulsar broker, sleep 10s ...");
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
                    log::info!("create consumer error : {:?}", e);
                    continue;
                }
            }
        }
    }

    Ok(())
}

fn index_and_es_timestamp(
    msg: &pulsar::consumer::Message<Data>,
) -> (String, String) {
    // build es index based on pulsar messages topic and
    // publish_time es index name
    // `topic+publish_date`, i.e. test-2021.01.01
    let topic = extract_topic_part(&msg.topic);
    let (es_timestamp, date_str) =
        es_timestamp_and_date(msg.metadata().publish_time);
    let index = format!("{}-{}", topic, date_str);
    (index, es_timestamp)
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
