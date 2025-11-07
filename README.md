![](https://github.com/ISISComputingGroup/saluki/blob/main/resources/logo.png)

ISIS-specific Kafka tools.
Deserialises [the ESS flatbuffers blobs](https://github.com/ess-dmsc/python-streaming-data-types) from Kafka. 

Also allows replaying data in a topic. 

# Usage

To run the latest version, use `uvx saluki <args>`.

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

## `play` - Replay data from one topic to another

### Between offsets

`saluki play mybroker:9092/source_topic mybroker:9092/dest_topic -o 123 125` - This will forward messages at offset 123, 124 and 125 in the `source_topic` to the `dest_topic`

### Between timestamps 

`saluki play mybroker:9092/source_topic mybroker:9092/dest_topic -t 1762209990 1762209992` - This will forward messages between the two given timestamps.

# Developer setup 
`pip install -e .[dev]`

