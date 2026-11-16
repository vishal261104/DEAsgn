"""
stream/driver.py
================
RabbitMQ consumer that simulates a delivery driver accepting or rejecting
orders from the ``rapido-dispatch`` topic exchange.

Drivers subscribe to a topic-pattern binding (e.g. ``bangalore.#`` or
``bangalore.koramangala.ride``) and apply a simple acceptance rule:

* **Accept** — message contains a ``zone`` field (fully-routed order).
* **Reject** — message is missing ``zone`` (partial / malformed order).
  Rejected messages are nacked without requeue so they are discarded.

Usage
-----
.. code-block:: bash

    python -m stream.driver <driver_name> <routing_pattern>

    # Examples
    python -m stream.driver "Ravi"  "bangalore.#"
    python -m stream.driver "Priya" "chennai.velachery.food"

Environment variables
---------------------
AMQP_HOST : str, optional
    RabbitMQ host (defaults to ``localhost``).
"""

import json
import os
import sys

import pika

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AMQP_HOST: str = os.getenv("AMQP_HOST", "localhost")
EXCHANGE_NAME = "rapido-dispatch"
EXCHANGE_TYPE = "topic"


def _on_message(
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    _properties: pika.spec.BasicProperties,
    body: bytes,
    *,
    driver_name: str,
) -> None:
    """Callback invoked for each incoming message.

    Accepts messages that carry a ``zone`` field; rejects all others.

    Args:
        channel: The pika channel the message arrived on.
        method: Delivery metadata including the routing key and delivery tag.
        _properties: AMQP message properties (unused).
        body: Raw JSON-encoded message bytes.
        driver_name: Display name of the driver (used for logging).
    """
    try:
        message: dict = json.loads(body)
    except json.JSONDecodeError as exc:
        print(f"[{driver_name}] Malformed JSON — nacking: {exc}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    if "zone" not in message:
        print(f"[{driver_name}] REJECTED (no zone): {message}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        print(f"[{driver_name}] ACCEPTED {method.routing_key}: {message}")
        channel.basic_ack(delivery_tag=method.delivery_tag)


def run(driver_name: str, routing_pattern: str) -> None:
    """Start the driver consumer loop.

    Creates an exclusive, auto-deleted queue bound to *routing_pattern* on
    the topic exchange and blocks until interrupted.

    Args:
        driver_name: Human-readable name shown in log output.
        routing_pattern: AMQP topic pattern, e.g. ``bangalore.#`` or
            ``chennai.velachery.ride``.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(AMQP_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name: str = result.method.queue
    channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=queue_name,
        routing_key=routing_pattern,
    )

    # Fair dispatch — don't send more than one message to this consumer at once
    channel.basic_qos(prefetch_count=1)

    callback = lambda ch, method, props, body: _on_message(
        ch, method, props, body, driver_name=driver_name
    )
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

    print(f"[{driver_name}] Listening for pattern '{routing_pattern}' — press Ctrl+C to stop.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        print(f"\n[{driver_name}] Shutting down gracefully.")
    finally:
        connection.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:  # noqa: PLR2004
        print("Usage: python -m stream.driver <driver_name> <routing_pattern>")
        sys.exit(1)

    run(driver_name=sys.argv[1], routing_pattern=sys.argv[2])
