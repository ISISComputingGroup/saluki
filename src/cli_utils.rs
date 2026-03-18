pub(crate) fn parse_broker_spec(s: &str) -> Result<BrokerAndTopic, String> {
    let (host_port, topic) = s.split_once('/').ok_or("Missing '/' separating topic")?;

    let (host, port_str) = host_port
        .rsplit_once(':')
        .ok_or("Missing ':' separating host and port")?;

    let port = port_str.parse::<u16>().map_err(|_| "Invalid port number")?;

    if topic.is_empty() {
        return Err("Topic cannot be empty".into());
    }

    Ok(BrokerAndTopic {
        host: host.to_string(),
        port,
        topic: topic.to_string(),
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
