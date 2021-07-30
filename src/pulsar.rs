use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use futures::TryStreamExt;
use pulsar::{
    Authentication, Consumer, ConsumerOptions, DeserializeMessage, Payload,
    Pulsar, SubType, TokioExecutor,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::string::FromUtf8Error;
use std::{
    collections::{HashMap, HashSet},
    env,
    time::Duration,
};
use structopt::StructOpt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time;

type ChannelPayload = (String, String, String);

#[derive(Serialize, Deserialize)]
struct Data;

impl DeserializeMessage for Data {
    type Output = Result<String, FromUtf8Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        String::from_utf8(payload.data.clone())
    }
}

// pub fn create_pulsar<S: Into<String>>(url: S, executor: Exe) -> Pulsar<_> {
pub async fn create_pulsar(
    url: &str, token: &str,
) -> Result<Pulsar<TokioExecutor>, Error> {
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

    pulsar
}

pub async fn create_consumer(
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

pub async fn consume_loop(
    pulsar: &Pulsar<TokioExecutor>, name: &str, subscription_name: &str,
    namespace: &str, topic_regex: &str, batch_size: u32,
    tx: Sender<ChannelPayload>, debug_topics: Option<&str>,
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

    let debug_topics: HashSet<_> = debug_topics
        .unwrap_or("")
        .split(',')
        .filter(|x| !x.is_empty())
        .into_iter()
        .collect();

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
            if !debug_topics.is_empty() && debug_topics.contains(index.as_str())
            {
                log::info!("Namespace: {}, data: {:?}", index, data);
            }
            if let Some((topic, date_str)) = split_index_and_date_str(&index) {
                pulsar_received_messages_with_date_inc_by(topic, date_str, 1);
                pulsar_received_messages_inc_by(topic, 1);
            }
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
