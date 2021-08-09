use serde::{Deserialize, Serialize};
use std::str::FromStr;
use structopt::StructOpt;

#[derive(Serialize, Deserialize, Debug)]
pub struct NamespaceFilter {
    pub namespace: String,
    pub filters: Vec<String>,
}

impl FromStr for NamespaceFilter {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str::<Self>(s)
            .map_err(|e| format!("parse input error: {}, input: {}", e, s))
    }
}

/// A pulsar messages to elasticsearch sync program
#[derive(StructOpt, Debug)]
#[structopt(name = "pulsar-elasticsearch-sync-rs")]
pub struct Opt {
    // A flag, true if used in the command line. Note doc comment will
    // be used for the help message of the flag. The name of the
    // argument will be, by default, based on the name of the field.
    /// Activate debug mode
    #[structopt(short, long)]
    pub debug: bool,

    // The number of occurrences of the `v/verbose` flag
    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    pub verbose: u8,

    /// Pulsar address
    #[structopt(short, long, default_value = "pulsar://127.0.0.1:6650")]
    pub pulsar_addr: String,

    /// Pulsar token
    #[structopt(short = "t", long)]
    pub pulsar_token: Option<String>,

    /// Pulsar namespace, ie. public/default
    #[structopt(short = "n", long, default_value = "public/default")]
    pub pulsar_namespace: String,

    /// Pulsar topic regex
    #[structopt(short = "r", long, default_value = ".*")]
    pub topic_regex: String,

    /// Pulsar topics for debug output, comma separated
    #[structopt(long)]
    pub debug_topics: Option<String>,

    /// Pulsar consumer batch size
    #[structopt(short = "b", long, default_value = "1000")]
    pub batch_size: u32,

    /// Elasticsearch buffer size
    #[structopt(short = "s", long, default_value = "1000")]
    pub buffer_size: usize,

    /// key in log as timestamp, default is to use pulsar message publishtime
    #[structopt(long)]
    pub time_key: Option<String>,

    // TODO: use may need custom time format
    // timestamp format
    // #[structopt(long)]
    // pub time_format: String,
    /// Flush interval for checking messages, default is 5000(5s)
    #[structopt(short = "f", long, default_value = "5000")]
    pub flush_interval: u32,

    /// Channel buffer size for receiving messages, default is 2048
    #[structopt(long, default_value = "2048")]
    pub channel_buffer_size: usize,

    /// Global filters for pulsar message
    #[structopt(long)]
    pub global_filters: Option<Vec<String>>,

    /// filters for namespaces
    #[structopt(long)]
    pub namespace_filters: Option<Vec<NamespaceFilter>>,

    /// inject key to message, value is uuid string
    #[structopt(long)]
    pub inject_key: bool,

    /// injected key name to message
    #[structopt(long)]
    pub injected_key_name: Option<String>,

    /// debug filename when injecting key
    #[structopt(long)]
    pub injected_logfile: Option<String>,

    /// Elasticsearch address
    #[structopt(short, long, default_value = "http://localhost:9200")]
    pub elasticsearch_addr: String,
}
