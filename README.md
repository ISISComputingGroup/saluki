![](https://github.com/ISISComputingGroup/saluki/blob/main/resources/logo.png)

ISIS-specific Kafka tools.
Deserialises [the ESS flatbuffers blobs](https://github.com/ess-dmsc/python-streaming-data-types) from Kafka. 

Also allows replaying data in a topic. 

# Usage
See `saluki --help` for all options. 

## `listen` - Listen to a topic for updates
`saluki listen mybroker:9092/mytopic` - This will listen for updates for `mytopic` on `mybroker`. 

### Filter to specific schemas

`saluki listen mybroker:9092/mytopic -f f144 -f f142` - This will listen for updates but ignore messages with schema IDs of `f142` or `f144`

## `consume`- Consume from a topic
`saluki consume mybroker:9092/mytopic -p 1 -o 123456 -m 10` - This will print 9 messages before (and inclusively the offset specified) offset `123456` of `mytopic` on `mybroker`, in partition 1.

Use the `-g` flag to go the other way, ie. in the above example to consume the 9 messages _after_ offset 123456

### Consume X of a certain schema(s)

TODO

## `sniff` - List all topics and their high, low watermarks and number of messages
`saluki sniff mybroker:9092`

## `play` - Replay data from one topic to another

### Between offsets

TODO

### Between timestamps 

TODO

# Install 
`pip install saluki`

## Developer setup 
`pip install .[dev]`

