from __future__ import annotations

import json
import sys
from typing import Any

import pika


EXCHANGE = "rapido-dispatch"


def create_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: python stream/driver.py NAME PATTERN")

    name, pattern = sys.argv[1:]
    connection = create_connection()
    channel = connection.channel()
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=False,
    )
    queue = channel.queue_declare(queue="", exclusive=True).method.queue
    channel.queue_bind(exchange=EXCHANGE, queue=queue, routing_key=pattern)

    def callback(ch: Any, method: Any, properties: Any, body: bytes) -> None:
        try:
            message = json.loads(body.decode("utf-8"))
            if not isinstance(message, dict) or "zone" not in message:
                print(
                    f"{name} NACK discarded "
                    f"key={method.routing_key} body={message}",
                    flush=True,
                )
                ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=False,
                )
                return

            print(
                f"{name} ACK key={method.routing_key} body={message}",
                flush=True,
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
            print(f"{name} NACK malformed message: {exc}", flush=True)
            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False,
            )

    channel.basic_consume(queue=queue, on_message_callback=callback)
    print(
        f"{name} listening pattern={pattern} queue={queue}",
        flush=True,
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
