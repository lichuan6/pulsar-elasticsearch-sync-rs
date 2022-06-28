use lazy_static::lazy_static;
use prometheus::{Encoder, GaugeVec, IntCounterVec, Opts, Registry};
use std::convert::Infallible;
use warp::Filter;

// TODO: add more application metrics
lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref PULSAR_MESSAGE_CONSUMED_TOTAL: IntCounterVec =
        IntCounterVec::new(
            Opts::new(
                "pulsar_message_consumed_total",
                "consumed messages from pulsar topics"
            ),
            &["topic"]
        )
        .expect("metric can be created");
    pub static ref PULSAR_MESSAGE_CONSUMED_DEBUG_TOTAL: IntCounterVec =
        IntCounterVec::new(
            Opts::new(
                "pulsar_message_consumed_debug_total",
                "consumed debug messages from pulsar topics"
            ),
            &["topic"]
        )
        .expect("metric can be created");
    pub static ref ELASTICSEARCH_WRITE_SUCCESS_TOTAL: IntCounterVec =
        IntCounterVec::new(
            Opts::new(
                "elasticsearch_write_success_total",
                "total messages successfully written to elasticsearch"
            ),
            &["topic"]
        )
        .expect("metric can be created");
    pub static ref ELASTICSEARCH_WRITE_FAILED_TOTAL: IntCounterVec =
        IntCounterVec::new(
            Opts::new(
                "elasticsearch_write_failed_total",
                "total messages failed to written to elasticsearch"
            ),
            &["topic"]
        )
        .expect("metric can be created");
    pub static ref PULSAR_MESSAGE_CONSUMED_WITH_DATE_TOTAL: IntCounterVec =
        IntCounterVec::new(
            Opts::new(
                "pulsar_message_consumed_with_date_total",
                "consumed messages from pulsar topics"
            ),
            &["topic", "date"]
        )
        .expect("metric can be created");
    pub static ref ELASTICSEARCH_WRITE_SUCCESS_WITH_DATE_TOTAL: IntCounterVec =
        IntCounterVec::new(
            Opts::new(
                "elasticsearch_write_success_with_date_total",
                "total messages successfully written to elasticsearch"
            ),
            &["topic", "date"]
        )
        .expect("metric can be created");
    pub static ref ELASTICSEARCH_WRITE_FAILED_WITH_DATE_TOTAL: IntCounterVec =
        IntCounterVec::new(
            Opts::new(
                "elasticsearch_write_failed_with_date_total",
                "total messages failed to written to elasticsearch"
            ),
            &["topic", "date"]
        )
        .expect("metric can be created");
    pub static ref ELASTICSEARCH_INDEX_FIELD_COUNT: GaugeVec = GaugeVec::new(
        Opts::new(
            "elasticsearch_index_field_count",
            "elasticsearch field count for index"
        ),
        &["index", "app"]
    )
    .expect("metric can be created");
}

pub fn pulsar_received_messages_inc_by(topic: &str, v: u64) {
    PULSAR_MESSAGE_CONSUMED_TOTAL.with_label_values(&[topic]).inc_by(v);
}

pub fn pulsar_received_debug_messages_inc_by(topic: &str, v: u64) {
    PULSAR_MESSAGE_CONSUMED_DEBUG_TOTAL.with_label_values(&[topic]).inc_by(v);
}

pub fn elasticsearch_write_success_total(topic: &str, v: u64) {
    ELASTICSEARCH_WRITE_SUCCESS_TOTAL.with_label_values(&[topic]).inc_by(v);
}

pub fn elasticsearch_write_failed_total(topic: &str, v: u64) {
    ELASTICSEARCH_WRITE_FAILED_TOTAL.with_label_values(&[topic]).inc_by(v);
}

pub fn pulsar_received_messages_with_date_inc_by(
    topic: &str, date: &str, v: u64,
) {
    PULSAR_MESSAGE_CONSUMED_WITH_DATE_TOTAL
        .with_label_values(&[topic, date])
        .inc_by(v);
}

pub fn elasticsearch_write_success_with_date_total(
    topic: &str, date: &str, v: u64,
) {
    ELASTICSEARCH_WRITE_SUCCESS_WITH_DATE_TOTAL
        .with_label_values(&[topic, date])
        .inc_by(v);
}

pub fn elasticsearch_write_failed_with_date_total(
    topic: &str, date: &str, v: u64,
) {
    ELASTICSEARCH_WRITE_FAILED_WITH_DATE_TOTAL
        .with_label_values(&[topic, date])
        .inc_by(v);
}

pub fn elasticsearch_index_field_count(index: &str, key: &str, v: u64) {
    ELASTICSEARCH_INDEX_FIELD_COUNT
        .with_label_values(&[index, key])
        .set(v as f64);
}

/// With the metrics defined(above), the next step is to register them with the
/// REGISTRY
pub fn register_custom_metrics() {
    REGISTRY
        .register(Box::new(PULSAR_MESSAGE_CONSUMED_TOTAL.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(PULSAR_MESSAGE_CONSUMED_DEBUG_TOTAL.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(ELASTICSEARCH_WRITE_SUCCESS_TOTAL.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(ELASTICSEARCH_WRITE_FAILED_TOTAL.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(PULSAR_MESSAGE_CONSUMED_WITH_DATE_TOTAL.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(ELASTICSEARCH_WRITE_SUCCESS_WITH_DATE_TOTAL.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(ELASTICSEARCH_WRITE_FAILED_WITH_DATE_TOTAL.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(ELASTICSEARCH_INDEX_FIELD_COUNT.clone()))
        .expect("collector can be registered");
}

fn metrics_buffer(
    metric_families: &[prometheus::proto::MetricFamily],
) -> Vec<u8> {
    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();
    encoder.encode(metric_families, &mut buffer).unwrap();
    buffer
}

pub async fn metric_handler() -> Result<impl warp::Reply, Infallible> {
    // A default ProcessCollector is registered automatically.
    let mut default_metrics = metrics_buffer(&prometheus::gather());
    let mut custom_metrics = metrics_buffer(&REGISTRY.gather());

    // Format output to String and return as http response
    let res = format!(
        "{}{}",
        String::from_utf8(default_metrics.clone()).unwrap(),
        String::from_utf8(custom_metrics.clone()).unwrap()
    );
    default_metrics.clear();
    custom_metrics.clear();

    Ok(res)
}

pub async fn run_warp_server() {
    register_custom_metrics();
    // prometheus will call `$host:3030/metrics` to fetch metrics
    let metric =
        warp::path!("metrics").and(warp::get()).and_then(metric_handler);

    warp::serve(metric).run(([0, 0, 0, 0], 3030)).await
}

pub fn run_metric_server() {
    tokio::task::spawn(run_warp_server());
}
