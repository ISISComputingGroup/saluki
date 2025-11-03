from confluent_kafka import Consumer, Producer


def play(
    src_broker: str,
    src_topic: str,
    dest_broker: str,
    dest_topic: str,
    offsets: list[int] | None,
    timestamps: list[int] | None,
    chunks: int,
) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": src_broker,
            "group.id": "saluki-play",
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": dest_broker,
        }
    )
    pass
