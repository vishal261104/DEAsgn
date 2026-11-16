"""
stream/dispatcher.py
====================
RabbitMQ event dispatcher for the FreshCart / Rapido dispatch simulation.

Publishes 60 randomised ride/food order events to a topic exchange
(``rapido-dispatch``) over a 2-second interval between messages.

Routing key convention
----------------------
Normal orders (90 % of traffic)::

    <city>.<zone>.<type>   e.g. bangalore.koramangala.ride

Partial / zone-less orders (every 10th message, used to test driver rejection)::

    <city>.<type>          e.g. chennai.food

Usage
-----
Requires a local RabbitMQ broker on the default port (5672).

.. code-block:: bash

    python -m stream.dispatcher

Environment variables
---------------------
AMQP_HOST : str, optional
    RabbitMQ host (defaults to ``localhost``).
DISPATCH_COUNT : int, optional
    Total number of messages to publish (defaults to 60).
DISPATCH_INTERVAL : float, optional
    Seconds to sleep between publishes (defaults to 2.0).
"""

import json
import os
import random
import time
from datetime import datetime, timezone

import pika

# ---------------------------------------------------------------------------
# Configuration — override via environment variables for different deployments
# ---------------------------------------------------------------------------
AMQP_HOST: str = os.getenv("AMQP_HOST", "localhost")
DISPATCH_COUNT: int = int(os.getenv("DISPATCH_COUNT", "60"))
DISPATCH_INTERVAL: float = float(os.getenv("DISPATCH_INTERVAL", "2.0"))

EXCHANGE_NAME = "rapido-dispatch"
EXCHANGE_TYPE = "topic"

# City → zones mapping
CITIES: dict[str, list[str]] = {
    "bangalore": ["koramangala", "indiranagar", "whitefield"],
    "chennai": ["tnagar", "adyar", "velachery"],
}

ORDER_TYPES: list[str] = ["ride", "food"]

# Every nth message is published without a zone (triggers driver rejection)
PARTIAL_MESSAGE_EVERY_N = 10


def _build_message(seq: int, city: str, zone: str | None, order_type: str) -> tuple[dict, str]:
    """Build a message payload and its routing key.

    Args:
        seq: 1-based sequence number for this publish round.
        city: City name (e.g. ``bangalore``).
        zone: Neighbourhood within the city, or ``None`` for partial messages.
        order_type: ``ride`` or ``food``.

    Returns:
        A (message_dict, routing_key) tuple.
    """
    ts = datetime.now(tz=timezone.utc).isoformat()
    if zone is None:
        return (
            {"id": seq, "city": city, "type": order_type, "ts": ts},
            f"{city}.{order_type}",
        )
    return (
        {"id": seq, "city": city, "zone": zone, "type": order_type, "ts": ts},
        f"{city}.{zone}.{order_type}",
    )


def run(count: int = DISPATCH_COUNT, interval: float = DISPATCH_INTERVAL) -> None:
    """Connect to RabbitMQ and publish ``count`` dispatch events.

    Args:
        count: Number of messages to publish.
        interval: Seconds to sleep between consecutive publishes.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(AMQP_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

    for i in range(count):
        city = random.choice(list(CITIES))
        zone: str | None = (
            None if (i + 1) % PARTIAL_MESSAGE_EVERY_N == 0
            else random.choice(CITIES[city])
        )
        order_type = random.choice(ORDER_TYPES)

        message, routing_key = _build_message(i + 1, city, zone, order_type)
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=routing_key,
            body=json.dumps(message),
        )
        print(f"[{i + 1:>3}/{count}] Sent {routing_key}: {message}")
        time.sleep(interval)

    connection.close()
    print("Dispatcher finished.")


if __name__ == "__main__":
    run()
