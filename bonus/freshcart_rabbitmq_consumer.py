from __future__ import annotations

import json
import sys
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pika
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
ELT_DIR = ROOT / "elt" / "freshcart_elt"
SQL_DIR = ELT_DIR / "sql"
sys.path.insert(0, str(ELT_DIR))

import extract
import snowflake_io as sf


EXCHANGE = "freshcart-orders"
ROUTING_KEY = "freshcart.order"
SOURCE = "rabbitmq"


def freshcart_record(message: object) -> dict:
    if not isinstance(message, dict):
        raise ValueError("message must be a JSON object")

    record = message.get("data", message)
    required = {
        "order_id",
        "cust_email",
        "cust_name",
        "order_date",
        "amount",
        "currency",
        "pincode",
        "updated_at",
    }
    if not isinstance(record, dict):
        raise ValueError("message data must be a JSON object")

    missing = sorted(required - set(record))
    if missing:
        raise ValueError(f"missing FreshCart fields: {missing}")
    return record


def collect_orders(seconds: int = 60) -> list[dict]:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost"),
    )
    channel = connection.channel()
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=False,
    )
    queue = channel.queue_declare(queue="", exclusive=True).method.queue
    channel.queue_bind(
        exchange=EXCHANGE,
        queue=queue,
        routing_key=ROUTING_KEY,
    )
    channel.basic_qos(prefetch_count=100)

    records: list[dict] = []

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode("utf-8"))
            record = freshcart_record(message)
            records.append(record)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"ACK order_id={record['order_id']}", flush=True)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            print(f"NACK discarded: {exc}", flush=True)
            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False,
            )

    channel.basic_consume(queue=queue, on_message_callback=callback)
    deadline = time.monotonic() + seconds
    print(
        f"listening exchange={EXCHANGE} routing_key={ROUTING_KEY} "
        f"queue={queue} seconds={seconds}",
        flush=True,
    )
    try:
        while time.monotonic() < deadline:
            connection.process_data_events(time_limit=1)
    finally:
        channel.cancel()
        connection.close()

    return records


def run_freshcart_elt(records: list[dict], run_id: str, run_date: date) -> None:
    landing_path = extract.write_landing(
        SOURCE,
        records,
        run_id,
        run_date,
    )
    print(f"landing_path={landing_path}", flush=True)
    if landing_path is None:
        print("No valid records collected; ELT load skipped", flush=True)
        return

    connection = sf.connect()
    variables = {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
    }
    stage = f"rabbitmq/{run_date.isoformat()}/{run_id}"
    try:
        loaded = sf.load_file(
            connection,
            landing_path,
            "bronze.orders_raw",
            stage,
        )
        print(f"bronze_rows_loaded={loaded}", flush=True)

        sf.run_sql_file(
            connection,
            SQL_DIR / "10_silver_orders_clean.sql",
            variables,
        )
        sf.run_sql_file(
            connection,
            SQL_DIR / "20_silver_orders_dedup.sql",
            variables,
        )

        http = extract.http_session()
        pins = [
            row[0]
            for row in sf.query_all(
                connection,
                """
                SELECT DISTINCT o.pincode
                FROM silver.orders_dedup o
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM ref.pincode_geo g
                    WHERE g.pincode = o.pincode
                )
                """,
            )
        ]
        if pins:
            pin_file = extract.write_landing(
                "pincode",
                extract.pincodes(http, pins, mock=False),
                run_id,
                run_date,
            )
            sf.load_file(
                connection,
                pin_file,
                "bronze.pincode_raw",
                f"pincode/{run_date.isoformat()}/{run_id}",
            )

        for filename in (
            "30_dim_customers_scd2.sql",
            "31_ref_fx_daily.sql",
            "32_ref_pincode_geo.sql",
            "40_gold_fct_orders.sql",
        ):
            sf.run_sql_file(connection, SQL_DIR / filename, variables)
            print(f"completed {filename}", flush=True)
    finally:
        connection.close()


def main() -> None:
    load_dotenv(ROOT / "elt" / ".env")
    run_date = date.today()
    run_id = (
        f"rabbitmq_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    records = collect_orders(seconds=60)
    print(f"valid_records={len(records)} run_id={run_id}", flush=True)
    run_freshcart_elt(records, run_id, run_date)


if __name__ == "__main__":
    main()
