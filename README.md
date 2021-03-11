# pulsar-elasticsearch-sync-rs

This project will consume messages from pulsar topic and write them to elasticsearch using bulk api.

It's written in Rust, which means it has low memory footprint, specifically it need only 10M+ memory after consuming and syncing 2,000,000 messages.

# build

```shell
cargo build --release
```

# run local

In order to run this program, you need to set up both elasticsearch and pulsar services. One easy way is to download binary from official website, unzip and run.

## run elasticsearch

You can download elasticsearch from https://www.elastic.co/downloads/elasticsearch, or run by:

```shell
wget -c https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.11.2-darwin-x86_64.tar.gz
tar -xf elasticsearch-7.11.2-darwin-x86_64.tar.gz
cd elasticsearch-7.11.2/
bin/elasticsearch
```

check elasticsearch is running `curl -s localhost:9200/_cluster/health | jq`:


```json
{
  "cluster_name": "elasticsearch",
  "status": "green",
  "timed_out": false,
  "number_of_nodes": 1,
  "number_of_data_nodes": 1,
  "active_primary_shards": 7,
  "active_shards": 7,
  "relocating_shards": 0,
  "initializing_shards": 0,
  "unassigned_shards": 1,
  "delayed_unassigned_shards": 0,
  "number_of_pending_tasks": 0,
  "number_of_in_flight_fetch": 0,
  "task_max_waiting_in_queue_millis": 0,
  "active_shards_percent_as_number": 87.5
}
```

## run pulsar in standalone

You can download pulsar from https://pulsar.apache.org/en/download/, or run by:

```shell
wget -c https://archive.apache.org/dist/pulsar/pulsar-2.7.0/apache-pulsar-2.7.0-bin.tar.gz
tar -xf apache-pulsar-2.7.0-bin.tar.gz
cd apache-pulsar-2.7.0
bin/pulsar standalone
```

Next you can [start pulsar server in standalone mode](https://pulsar.apache.org/docs/en/standalone/):

```shell
bin/pulsar standalone
```

Pulsar standalone mode

> The standalone mode includes a Pulsar broker, the necessary ZooKeeper and BookKeeper components running inside of a single Java Virtual Machine (JVM) process

## run pulsar-elasticsearch-sync-rs

After running `cargo build --release`, you can run pulsar-elasticsearch-sync-rs locally:

```shell
RUST_LOG=trace ./target/release/pulsar-elasticsearch-sync-rs
```

You can also pass `--help` to see all the command line arguments:

```shell
RUST_LOG=trace ./target/release/pulsar-elasticsearch-sync-rs --help
pulsar-elasticsearch-sync-rs 0.1.0
A pulsar messages to elasticsearch sync program

USAGE:
    pulsar-elasticsearch-sync-rs [FLAGS] [OPTIONS]

FLAGS:
    -d, --debug      Activate debug mode
    -h, --help       Prints help information
    -V, --version    Prints version information
    -v, --verbose    Verbose mode (-v, -vv, -vvv, etc.)

OPTIONS:
    -b, --batch-size <batch-size>                    Pulsar consumer batch size [default: 1000]
    -s, --buffer-size <buffer-size>                  Elasticsearch buffer size [default: 1000]
    -e, --elasticsearch-addr <elasticsearch-addr>    Elasticsearch address [default: http://localhost:9200]
    -p, --pulsar-addr <pulsar-addr>                  Pulsar address [default: pulsar://127.0.0.1:6650]
    -n, --pulsar-namespace <pulsar-namespace>        Pulsar namespace, ie. public/default [default: public/default]
    -t, --pulsar-token <pulsar-token>                Pulsar token
    -r, --topic-regex <topic-regex>                  Pulsar topic regex [default: .*]
```

After pulsar-elasticsearch-sync-rs is started, you can produce some messsages to pulsar topic using `pulsar-client`:

```shell
bin/pulsar-client produce public/default/test --messages "{\"data\": \"123\"}" -n 1000
```

This command will produce 1000 messages to pulsar topic `public/default/test`, you can also inspect the topic stats using `bin/pulsar-admin topics stats public/default/test`:

```json
Warning: Nashorn engine is planned to be removed from a future JDK release
{
  "msgRateIn" : 0.0,
  "msgThroughputIn" : 0.0,
  "msgRateOut" : 0.0,
  "msgThroughputOut" : 0.0,
  "bytesInCounter" : 120336,
  "msgInCounter" : 1040,
  "bytesOutCounter" : 69616,
  "msgOutCounter" : 1000,
  "averageMsgSize" : 0.0,
  "msgChunkPublished" : false,
  "storageSize" : 69549,
  "backlogSize" : 0,
  "publishers" : [ ],
  "subscriptions" : {
    "pulsar-elasticsearch-sync-rs" : {
      "msgRateOut" : 0.0,
      "msgThroughputOut" : 0.0,
      "bytesOutCounter" : 69616,
      "msgOutCounter" : 1000,
      "msgRateRedeliver" : 0.0,
      "chuckedMessageRate" : 0,
      "msgBacklog" : 0,
      "msgBacklogNoDelayed" : 0,
      "blockedSubscriptionOnUnackedMsgs" : false,
      "msgDelayed" : 0,
      "unackedMessages" : 0,
      "type" : "Shared",
      "msgRateExpired" : 0.0,
      "lastExpireTimestamp" : 0,
      "lastConsumedFlowTimestamp" : 1615520112751,
      "lastConsumedTimestamp" : 1615520113935,
      "lastAckedTimestamp" : 1615520113945,
      "consumers" : [ {
        "msgRateOut" : 0.0,
        "msgThroughputOut" : 0.0,
        "bytesOutCounter" : 69616,
        "msgOutCounter" : 1000,
        "msgRateRedeliver" : 0.0,
        "chuckedMessageRate" : 0.0,
        "consumerName" : "consumer-pulsar-elasticsearch-sync-rs",
        "availablePermits" : 501,
        "unackedMessages" : 0,
        "avgMessagesPerEntry" : 6,
        "blockedConsumerOnUnackedMsgs" : false,
        "lastAckedTimestamp" : 1615520113945,
        "lastConsumedTimestamp" : 1615520113935,
        "metadata" : { },
        "address" : "/127.0.0.1:55300",
        "connectedSince" : "2021-03-12T11:34:55.896013+08:00",
        "clientVersion" : "2.0.1-incubating"
      } ],
      "isDurable" : false,
      "isReplicated" : false,
      "consumersAfterMarkDeletePosition" : { }
    }
  },
  "replication" : { },
  "deduplicationStatus" : "Enabled"
}
```

You can see from output, we have a subscription with name `pulsar-elasticsearch-sync-rs`, who consumed 1000 messages from topic `public/default/test`:

    "msgOutCounter" : 1000,

Check messages are written to elaseticsearch `curl localhost:9200/_cat/indices`:

    green  open .kibana-event-log-7.8.0-000001 YNwaq7qGTLuL83b7o6sDaA 1 0    3 0  15.4kb  15.4kb
    green  open .apm-custom-link               DHe2C4iASouv-14d_wDHGA 1 0    0 0    208b    208b
    green  open .kibana_task_manager_1         ad1S36zXRB-KpuI0iA0MMA 1 0    5 1  30.5kb  30.5kb
    green  open .apm-agent-configuration       Vf_nD4DpQQ2QvIMGOrHFhw 1 0    0 0    208b    208b
    yellow open test-2021.03.12                VxpIi-_0R_adcqA0C_OVcw 1 1 1000 0  35.2kb  35.2kb
    green  open .kibana_1                      czSbQ-NEQi2o30LXLuQK2Q 1 0   35 6 107.6kb 107.6kb

You can see the elasticsearch index `test-2021.03.12` is created.

Check doc count of this index `curl -s localhost:9200/test-2021.03.12/_count | jq`:

```shell
{
  "count": 1000,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  }
}
```

You can see all 1000 messages we produced to pulsar topic are synced to elaseticsearch.

# run in kubernetes

pulsar-elasticsearch-sync-rs can be also deployed in kubernetes.

You can define a [kubernetes deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) to deploy it to kubernetes.

Before creating the deployment, you need to setup several ENV variables(defined in `src/arg.rs`):

- `PULSAR_ADDRESS`: default to `pulsar://localhost:6650`
- `ELASTICSEARCH_ADDRESS`: default to `http://localhost:9200`
- `PULSAR_TOKEN`: optional, default to empty
- `PULSAR_NAMESPACE`: default to `public/default`

You can also choose your own args for:

- `--batch-size`: Pulsar consumer batch size, default to `1000`
- `--buffer-size`: Elasticsearch bulk batch size, default to `1000`

Then you can create the deployment by running the following command:

```shell
kubectl apply -f kubernetes/deployment.yaml
```

You can check whether indices are created or the doc count:

```shell
curl $ELASTICSEARCH_ADDRESS/_cat/indices
curl $ELASTICSEARCH_ADDRESS/test-2021.03.12/_count
```
