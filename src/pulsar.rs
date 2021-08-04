use crate::{
    es::split_index_and_date_str,
    prometheus::{
        pulsar_received_messages_inc_by,
        pulsar_received_messages_with_date_inc_by,
    },
    util::index_and_es_timestamp,
};
use futures::TryStreamExt;
use pulsar::{
    Authentication, Consumer, ConsumerOptions, DeserializeMessage, Payload,
    Pulsar, SubType, TokioExecutor,
};
use regex::{Regex, RegexSet};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env,
    string::FromUtf8Error,
    time::Duration,
};
use tokio::sync::mpsc::Sender;

pub type ChannelPayload = (String, String, String);
pub type Message<T> = pulsar::consumer::Message<T>;

#[derive(Serialize, Deserialize)]
pub struct Data;

impl DeserializeMessage for Data {
    type Output = Result<String, FromUtf8Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        String::from_utf8(payload.data.clone())
    }
}

// pub fn create_pulsar<S: Into<String>>(url: S, executor: Exe) -> Pulsar<_> {
pub async fn create_pulsar(
    url: &str,
) -> Result<Pulsar<TokioExecutor>, pulsar::error::Error> {
    let mut builder = Pulsar::builder(url, TokioExecutor);

    if let Ok(token) = env::var("PULSAR_TOKEN") {
        let authentication = Authentication {
            name: "token".to_string(),
            data: token.into_bytes(),
        };

        builder = builder.with_auth(authentication);
    }

    let pulsar: Pulsar<_> = builder.build().await?;

    Ok(pulsar)
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
    global_filters: Option<&RegexSet>,
    namespace_filters: Option<&HashMap<String, RegexSet>>,
) -> Result<(), pulsar::Error> {
    let mut consumer: Consumer<Data, _> = create_consumer(
        pulsar,
        name,
        namespace,
        topic_regex,
        subscription_name,
        batch_size,
    )
    .await?;

    log::info!("consumerd created, name: {}, namespace: {}, topic_regex: {}, subscription: {}, namespace_filters: {:?}", name, namespace, topic_regex, subscription_name, namespace_filters);

    let debug_topics: HashSet<_> = debug_topics
        .unwrap_or("")
        .split(',')
        .filter(|x| !x.is_empty())
        .into_iter()
        .collect();

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

            // filter messages using namespace_filters
            if let Some(namespace_filters) = namespace_filters {
                log::debug!(
                    "checking match namespace filters, namespace: {}, namespace_filters: {:?}, data: {}",
                    namespace,
                    namespace_filters,
                    data
                );
                if let Some(regexset) = namespace_filters.get(namespace) {
                    if regexset.is_match(&data) {
                        log::debug!(
                            "data match namespace filters: {}, skip",
                            data
                        );
                        continue;
                    }
                }
            }

            // filter messages using global_filters
            if let Some(global_filters) = global_filters {
                if global_filters.is_match(&data) {
                    log::debug!("data match global filters: {}, skip", data);
                    continue;
                }
            }

            let (index, es_timestamp) = index_and_es_timestamp(&msg);
            if !debug_topics.is_empty() && debug_topics.contains(index.as_str())
            {
                log::info!("Namespace: {}, data: {:?}", index, data);
            }
            // export consumed messages count metrics
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
                pulsar,
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
