from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import pika


EXCHANGE = "rapido-dispatch"
CITIES = {
    "bangalore": ["koramangala", "indiranagar", "whitefield"],
    "chennai": ["tnagar", "whitefield"],
}
TYPES = ["ride", "food"]


def create_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))


def publish_message(channel: Any, sequence: int) -> None:
    city = random.choice(list(CITIES))
    zone = random.choice(CITIES[city])
    order_type = random.choice(TYPES)
    routing_key = f"{city}.{zone}.{order_type}"
    message = {
        "id": str(uuid.uuid4()),
        "city": city,
        "zone": zone,
        "type": order_type,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    is_bad = sequence % 10 == 0
    if is_bad:
        message.pop("zone")


    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(message).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json"),
    )
    print(
        f"published #{sequence} key={routing_key} "
        f"id={message['id']} bad={is_bad}",
        flush=True,
    )


def main() -> None:
    connection = create_connection()
    channel = connection.channel()
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=False,
    )

    sequence = 0
    try:
        while True:
            sequence += 1
            publish_message(channel, sequence)
            time.sleep(2)
    except KeyboardInterrupt:
        print("dispatcher stopping", flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
