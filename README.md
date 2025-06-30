![](https://github.com/rerpha/saluki/blob/main/resources/logo.png)

Serialise/deserialise flatbuffers blobs from kafka. 
This currently deserialises https://github.com/ess-dmsc/python-streaming-data-types, but I am working to make it agnostic. Python bindings for the respective schema will need to be generated. 

# Usage
See `saluki --help` for all options. 

## Listen to a topic for updates
`saluki listen mybroker:9092/mytopic` - This will listen for updates for `mytopic` on `mybroker`. 

## Consume from a topic
`saluki consume mybroker:9092/mytopic -p 1 -o 123456 -m 10` - This will print 9 messages before (and inclusively the offset specified) offset `123456` of `mytopic` on `mybroker`, in partition 1.

Use the `-g` flag to go the other way, ie. in the above example to consume 9 messages FROM offset 123456

# Install 
`pip install saluki`

## Developer setup 
`pip install .[dev]`

