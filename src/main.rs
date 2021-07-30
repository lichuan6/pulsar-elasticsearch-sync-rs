use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use futures::TryStreamExt;
use pulsar_elasticsearch_sync_rs::{
    es::split_index_and_date_str,
    prometheus::{
        pulsar_received_messages_inc_by,
        pulsar_received_messages_with_date_inc_by, run_warp_server,
    },
};
use std::{
    collections::{HashMap, HashSet},
    env,
    time::Duration,
};
use structopt::StructOpt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time;

use pulsar_elasticsearch_sync_rs::{args::Opt, es, pulsar};

fn run_metric_server() {
    tokio::task::spawn(run_warp_server());
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

    // let pulsar = create_pulsar

    let consumer_name = "consumer-pulsar-elasticsearch-sync-rs";
    let subscription_name = "pulsar-elasticsearch-sync-rs";
    log::info!(
        "pulsar elasticserach sync started, begin to consume messages..."
    );

    let (tx, mut rx) = channel::<pulsar::ChannelPayload>(2048);

    let buffer_size = opt.buffer_size;
    let interval = opt.flush_interval;
    let time_key = opt.time_key;
    let debug_topics = opt.debug_topics;
    tokio::spawn(async move {
        // sink log to elasticsearch
        sink_elasticsearch_loop(
            &client,
            &mut rx,
            buffer_size,
            interval,
            time_key.as_ref().map(String::as_ref),
        )
        .await;
    });

    consume_loop(
        &pulsar,
        consumer_name,
        subscription_name,
        &pulsar_namespace,
        &opt.topic_regex,
        opt.batch_size,
        tx,
        debug_topics.as_ref().map(String::as_ref),
    )
    .await?;

    Ok(())
}

/// Read pulsar messages from Receiver and write theme to elasticsearch
async fn sink_elasticsearch_loop(
    client: &elasticsearch::Elasticsearch, rx: &mut Receiver<ChannelPayload>,
    buffer_size: usize, flush_interval: u32, time_key: Option<&str>,
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
                     es::bulkwrite_and_clear(&client, &mut buffer_map, time_key).await;
                 }
            },
            _ = interval.tick() => {
                log::debug!("{}ms passed", flush_interval);
                if !buffer_map.is_empty() {
                    log::debug!("buffer_map is not emptry, len: {}", buffer_map.len());
                    es::bulkwrite_and_clear(&client, &mut buffer_map, time_key).await;
                }
            }
        }
    }
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
