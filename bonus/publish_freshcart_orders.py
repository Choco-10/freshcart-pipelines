from __future__ import annotations

import json
import sys
from pathlib import Path

import pika


ROOT = Path(__file__).resolve().parents[1]
ELT_DIR = ROOT / "elt" / "freshcart_elt"
sys.path.insert(0, str(ELT_DIR))

import mock_data


EXCHANGE = "freshcart-orders"
ROUTING_KEY = "freshcart.order"


def main() -> None:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost"),
    )
    channel = connection.channel()
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=False,
    )
    try:
        for order in mock_data.WEB_ORDERS:
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=ROUTING_KEY,
                body=json.dumps({"data": order}).encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="application/json",
                ),
            )
            print(f"published order_id={order['order_id']}", flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
