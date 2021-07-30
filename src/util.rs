use crate::pulsar::Data;
use chrono::{DateTime, Local, NaiveDateTime, TimeZone};

pub fn index_and_es_timestamp(
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

pub fn es_timestamp_and_date(publish_time: u64) -> (String, String) {
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
pub fn extract_topic_part(topic: &str) -> &str {
    let v: Vec<_> = topic.split('/').collect();
    assert!(v.len() == 5);
    v[4]
}
