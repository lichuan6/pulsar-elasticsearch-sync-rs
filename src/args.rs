use structopt::StructOpt;

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

    /// Pulsar consumer batch size
    #[structopt(short = "b", long, default_value = "1000")]
    pub batch_size: u32,

    /// Elasticsearch buffer size
    #[structopt(short = "s", long, default_value = "1000")]
    pub buffer_size: usize,

    /// Elasticsearch address
    #[structopt(short, long, default_value = "http://localhost:9200")]
    pub elasticsearch_addr: String,
}
