use crate::{es::sink_elasticsearch_loop, pulsar::consume_loop};
use pulsar_elasticsearch_sync_rs::{
    args::Opt,
    es,
    prometheus::run_metric_server,
    pulsar,
    util::{create_namespace_filters, create_regexset, env_or},
};
use structopt::StructOpt;
use tokio::sync::mpsc::channel;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const CONSUMER_NAME: &str = "consumer-pulsar-elasticsearch-sync-rs";
const SUBSCRIPTION_NAME: &str = "pulsar-elasticsearch-sync-rs";

const ELASTICSEARCH_ADDRESS: &str = "ELASTICSEARCH_ADDRESS";
const PULSAR_ADDRESS: &str = "PULSAR_ADDRESS";
const PULSAR_NAMESPACE: &str = "PULSAR_NAMESPACE";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    run_metric_server();

    let opt = Opt::from_args();
    let pulsar_addr = env_or(PULSAR_ADDRESS, opt.pulsar_addr.clone());
    let es_addr = env_or(ELASTICSEARCH_ADDRESS, opt.elasticsearch_addr.clone());
    let client = es::create_client(&es_addr).unwrap();
    log::info!("pulsar elasticsearch sync started, begin to consume messages");
    log::info!("command line args: {:?}", opt);

    let buffer_size = opt.buffer_size;
    let interval = opt.flush_interval;
    let time_key = opt.time_key;
    let debug_topics = opt.debug_topics;
    let pulsar_namespace = opt.pulsar_namespace;
    let channel_buffer_size = opt.channel_buffer_size;
    let global_filters = opt.global_filters;
    let namespace_filters = opt.namespace_filters;
    let global_filter_set = create_regexset(global_filters).unwrap_or(None);
    let namespace_filter_set =
        create_namespace_filters(namespace_filters).unwrap_or(None);
    let inject_key = opt.inject_key;
    let injected_namespaces = opt.injected_namespaces;
    let pulsar_namespace = env_or(PULSAR_NAMESPACE, pulsar_namespace);
    let indices_rewrite_rules = opt.indices_rewrite_rules;
    let debug_log_regexset =
        create_regexset(opt.debug_log_patterns).unwrap_or(None);
    let consumer_name = opt.consumer_name.unwrap_or(CONSUMER_NAME.to_string());
    let subscription_name =
        opt.subscription_name.unwrap_or(SUBSCRIPTION_NAME.to_string());
    let (tx, mut rx) = channel::<pulsar::ChannelPayload>(channel_buffer_size);
    tokio::spawn(async move {
        // sink log to elasticsearch
        sink_elasticsearch_loop(
            &client,
            &mut rx,
            buffer_size,
            interval,
            time_key.as_ref().map(String::as_ref),
            indices_rewrite_rules,
            debug_log_regexset.as_ref(),
        )
        .await;
    });

    consume_loop(
        &pulsar_addr,
        &consumer_name,
        &subscription_name,
        &pulsar_namespace,
        &opt.topic_regex,
        opt.batch_size,
        tx,
        debug_topics.as_ref().map(String::as_ref),
        global_filter_set.as_ref(),
        namespace_filter_set.as_ref(),
        inject_key,
        injected_namespaces,
    )
    .await?;

    Ok(())
}
