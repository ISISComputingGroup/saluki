![](https://github.com/ISISComputingGroup/saluki/blob/main/resources/logo.png)

ISIS-specific Kafka tools.
Deserialises [the ISIS flatbuffers blobs](https://github.com/ISISComputingGroup/streaming-data-types) from Kafka. 

Also allows replaying data in a topic. 

# Usage

To run the latest version, use `docker run ghcr.io/isiscomputinggroup/saluki:main <args>`

alternatively you can run it locally using `cargo run -- <args>`

See `saluki --help` for all options. 

## `consume`- Consume from a topic
> [!NOTE]
> An alias for `saluki listen` exists for `consume` - the two commands were merged when `saluki` was rewritten in rust. 

This continuously listens to a topic, deserialises messages and prints them.

It can also be given a `--messages` flag to limit the number of messages to print:
`saluki consume mybroker:9092/mytopic -p 1 -o 123456 -m 10` - This will print 9 messages before (and inclusively the offset specified) offset `123456` of `mytopic` on `mybroker`, in partition 1.

Use the `--last` flag to consume the last `x` messages on the topic, ie. `saluki consume mybroker:9092/mytopic --last 5` will print the last 5 messages.

### Filter to specific schemas

`saluki consume mybroker:9092/mytopic -f f144` - This will listen for updates but ignore messages with schema IDs of `f144`

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

> [!IMPORTANT]
> This functionality was not ported over when `saluki` was rewritten in rust. [this issue](https://github.com/ISISComputingGroup/saluki/issues/50) exists to do so. 

### Between offsets

`saluki play mybroker:9092/source_topic mybroker:9092/dest_topic -o 123 125` - This will forward messages at offset 123, 124 and 125 in the `source_topic` to the `dest_topic`

### Between timestamps 

`saluki play mybroker:9092/source_topic mybroker:9092/dest_topic -t 1762209990 1762209992` - This will forward messages between the two given timestamps.


## `count` - Count topic data rates

`count` is used for viewing the current data rate in a given topic. 

An example of using this could be:

`saluki count mybroker:9092/mytopic --message-interval 3` - this listens to the `mytopic` topic and prints the data rate between 3 second intervals. 

# Developer setup

some system dependencies are required. On Windows these are built-in, but on a debian-based linux distro you will need `libcurl4-openssl-dev`

To build run `cargo build`



