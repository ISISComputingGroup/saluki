use anyhow::{Context, Result, bail};
use std::str::FromStr;

pub(crate) fn parse_broker_spec(s: &str) -> Result<BrokerAndTopic> {
    let b = parse_broker_spec_optional_topic(s)?;

    if b.topic.is_none() {
        bail!("Topic cannot be empty");
    }

    Ok(BrokerAndTopic {
        host: b.host.to_string(),
        port: b.port,
        topic: b.topic.unwrap(),
    })
}

pub(crate) fn parse_broker_spec_optional_topic(s: &str) -> Result<BrokerAndOptionalTopic> {
    let (host_port, topic) = match s.split_once('/') {
        Some((l, r)) => (l.to_string(), Some(r.to_string())),
        None => (s.to_string(), None),
    };

    let (host, port_str) = host_port
        .rsplit_once(':')
        .context("Missing ':' separating host and port")?;

    let port = port_str
        .parse::<u16>()
        .with_context(|| "Invalid port number")?;

    Ok(BrokerAndOptionalTopic {
        host: host.to_string(),
        port,
        topic,
    })
}

#[derive(Clone, Debug)]
pub struct BrokerAndTopic {
    pub host: String,
    pub port: u16,
    pub topic: String,
}

impl BrokerAndTopic {
    pub(crate) fn broker(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Clone, Debug)]
pub struct BrokerAndOptionalTopic {
    pub host: String,
    pub port: u16,
    pub topic: Option<String>,
}

impl BrokerAndOptionalTopic {
    pub(crate) fn broker(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone)]
pub struct KafkaOption {
    pub key: String,
    pub value: String,
}

impl FromStr for KafkaOption {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (key, value) = s
            .split_once('=')
            .ok_or_else(|| format!("expected KEY=VALUE, got '{}'", s))?;

        Ok(KafkaOption {
            key: key.to_string(),
            value: value.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use rstest::*;

    #[rstest]
    #[case("localhost", 9092, "mytopic")]
    #[case("localhost.co.uk", 9093, "mytopic1")]
    #[case("192.168.0.1", 9094, "mytopic2")]
    fn test_parse_correct_topic(#[case] host: String, #[case] port: u16, #[case] topic: String) {
        let res = parse_broker_spec(&format!("{host}:{port}/{topic}")).unwrap();
        assert_eq!(res.host, host);
        assert_eq!(res.port, port);
        assert_eq!(res.topic, topic);
    }

    #[test]
    fn test_parse_empty_topic() {
        let res = parse_broker_spec_optional_topic("localhost:9092").unwrap();
        assert_eq!(res.host, "localhost");
        assert_eq!(res.port, 9092);
        assert_eq!(res.topic, None);
    }

    #[test]
    fn test_parse_missing_port_bails() {
        let res = parse_broker_spec_optional_topic("localhost").unwrap_err();
        assert_eq!(res.to_string(), "Missing ':' separating host and port");
    }

    #[test]
    fn test_parse_empty_string() {
        let res = parse_broker_spec_optional_topic("").unwrap_err();
        assert_eq!(res.to_string(), "Missing ':' separating host and port");
    }

    #[test]
    fn parse_broker_spec_missing_topic() {
        let res = parse_broker_spec("localhost:9092").unwrap_err();
        assert_eq!(res.to_string(), "Topic cannot be empty");
    }

    #[test]
    fn broker_and_topic_to_string() {
        let b = BrokerAndTopic {
            host: "localhost".to_string(),
            port: 9092,
            topic: "mytopic".to_string(),
        };
        assert_eq!(b.broker(), "localhost:9092");
    }

    #[test]
    fn broker_and_optional_topic_to_string() {
        let b = BrokerAndOptionalTopic {
            host: "localhost".to_string(),
            port: 9092,
            topic: None,
        };
        assert_eq!(b.broker(), "localhost:9092");
    }
    #[test]
    fn test_parse_valid_kafka_option() {
        let res = KafkaOption::from_str("key=value").unwrap();
        assert_eq!(res.key, "key");
        assert_eq!(res.value, "value");
    }

    #[test]
    fn test_parse_valid_kafka_option_without_equals() {
        let res = KafkaOption::from_str("key value");
        assert!(
            res.is_err(),
            "Expected error when parsing invalid Kafka option"
        );
        assert_eq!(
            res.unwrap_err().to_string(),
            "expected KEY=VALUE, got 'key value'"
        );
    }

    #[test]
    fn test_parse_valid_kafka_option_without_space() {
        let res = KafkaOption::from_str("keyvalue");
        assert!(
            res.is_err(),
            "Expected error when parsing invalid Kafka option"
        );
        assert_eq!(
            res.unwrap_err().to_string(),
            "expected KEY=VALUE, got 'keyvalue'"
        );
    }
}
