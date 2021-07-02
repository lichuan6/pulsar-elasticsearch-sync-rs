// use lazy_static::lazy_static;
use prometheus::Encoder;
use std::convert::Infallible;
use warp::Filter;

// TODO: add more application metrics
// lazy_static! {
//     pub static ref REGISTRY: Registry = Registry::new();
//     pub static ref INCOMING_REQUESTS: IntCounter =
//         IntCounter::new("incoming_requests", "Incoming Requests")
//             .expect("metric can be created");
//     pub static ref CONNECTED_CLIENTS: IntGauge =
//         IntGauge::new("connected_clients", "Connected Clients")
//             .expect("metric can be created");
//     pub static ref RESPONSE_CODE_COLLECTOR: IntCounterVec = IntCounterVec::new(
//         Opts::new("response_code", "Response Codes"),
//         &["env", "statuscode", "type"]
//     )
//     .expect("metric can be created");
//     pub static ref RESPONSE_TIME_COLLECTOR: HistogramVec = HistogramVec::new(
//         HistogramOpts::new("response_time", "Response Times"),
//         &["env"]
//     )
//     .expect("metric can be created");
// }
//
// /// With the metrics defined(above), the next step is to register them with the
// /// REGISTRY
// pub fn register_custom_metrics() {
//     REGISTRY
//         .register(Box::new(INCOMING_REQUESTS.clone()))
//         .expect("collector can be registered");
//
//     REGISTRY
//         .register(Box::new(CONNECTED_CLIENTS.clone()))
//         .expect("collector can be registered");
//
//     REGISTRY
//         .register(Box::new(RESPONSE_CODE_COLLECTOR.clone()))
//         .expect("collector can be registered");
//
//     REGISTRY
//         .register(Box::new(RESPONSE_TIME_COLLECTOR.clone()))
//         .expect("collector can be registered");
// }

pub async fn metric_handler() -> Result<impl warp::Reply, Infallible> {
    // A default ProcessCollector is registered automatically.
    let mut buffer = Vec::new();
    let encoder = prometheus::TextEncoder::new();

    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    // Format output to String and return as http response
    let res = String::from_utf8(buffer.clone()).unwrap();
    buffer.clear();

    Ok(res)
}

pub async fn run_warp_server() {
    // register_custom_metrics();
    // prometheus will call `$host:3030/metrics` to fetch metrics
    let metric =
        warp::path!("metrics").and(warp::get()).and_then(metric_handler);

    warp::serve(metric).run(([0, 0, 0, 0], 3030)).await
}
