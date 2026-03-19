use anyhow::{Context, Result, bail};

pub(crate) fn parse_broker_spec(s: &str) -> Result<BrokerAndTopic> {
    let b = parse_broker_spec_optional_topic(&s)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case("localhost", 9092, "mytopic")]
    #[case("localhost.co.uk", 9093, "mytopic1")]
    #[case("192.168.0.1", 9094, "mytopic2")]
    fn test_parse_correct_topic(#[case] host: String, #[case] port: u16, #[case] topic: String) {
        let res = parse_broker_spec(&format!("{}:{}/{}", host, port, topic)).unwrap();
        assert_eq!(res.host, host);
        assert_eq!(res.port, port);
        assert_eq!(res.topic, topic);
    }
    // TODO get these working
    // #[test]
    // fn test_parse_empty_string() {
    //     let res = parse_broker_spec("").unwrap_err();
    //     assert_eq!(res, Err("Missing '/' separating topic"));
    // }
    //
    // #[test]
    // fn test_parse_missing_port() {
    //     parse_broker_spec("localhost/mytopic").expect_err("Missing ':' separating host and port");
    // }
}
