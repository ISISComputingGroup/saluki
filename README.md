![](https://github.com/ISISComputingGroup/saluki/blob/main/resources/logo.png)

ISIS-specific Kafka tools.
Deserialises [the ESS flatbuffers blobs](https://github.com/ess-dmsc/python-streaming-data-types) from Kafka. 

Also allows replaying data in a topic. 

# Usage

To run the latest version, install [uv](https://docs.astral.sh/uv/getting-started/installation/) and use `uvx saluki <args>`.

alternatively you can `pip install saluki` and run it from a `venv`. 

See `saluki --help` for all options. 

## `listen` - Listen to a topic for updates
`saluki listen mybroker:9092/mytopic` - This will listen for updates for `mytopic` on `mybroker`. 

### Filter to specific schemas

`saluki listen mybroker:9092/mytopic -f f144 -f f142` - This will listen for updates but ignore messages with schema IDs of `f142` or `f144`

## `consume`- Consume from a topic
`saluki consume mybroker:9092/mytopic -p 1 -o 123456 -m 10` - This will print 9 messages before (and inclusively the offset specified) offset `123456` of `mytopic` on `mybroker`, in partition 1.

Use the `-g` flag to go the other way, ie. in the above example to consume the 9 messages _after_ offset 123456

You can also filter out messages to specific schema(s) with the `-f` flag, like the example above for `listen`.

## `sniff` - List all topics and their high, low watermarks and number of messages
`saluki sniff mybroker:9092`

Output looks as follows:

```
$ saluki sniff mybroker:9092

INFO:saluki:Cluster ID: redpanda.0faa4595-7298-407e-9db7-7e2758d1af1f
INFO:saluki:Brokers:
INFO:saluki:    192.168.0.111:9092/1
INFO:saluki:    192.168.0.112:9092/2
INFO:saluki:    192.168.0.113:9092/0
INFO:saluki:Topics:
INFO:saluki:    MERLIN_events:
INFO:saluki:            0 - low:262322729, high:302663378, num_messages:40340649
INFO:saluki:    MERLIN_runInfo:
INFO:saluki:            0 - low:335, high:2516, num_messages:2181
INFO:saluki:    MERLIN_monitorHistograms:
INFO:saluki:            0 - low:7515, high:7551, num_messages:36
```

## `play` - Replay data from one topic to another

### Between offsets

`saluki play mybroker:9092/source_topic mybroker:9092/dest_topic -o 123 125` - This will forward messages at offset 123, 124 and 125 in the `source_topic` to the `dest_topic`

### Between timestamps 

`saluki play mybroker:9092/source_topic mybroker:9092/dest_topic -t 1762209990 1762209992` - This will forward messages between the two given timestamps.

# Developer setup 
`pip install -e .[dev]`

