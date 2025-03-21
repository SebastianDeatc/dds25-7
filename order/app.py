import logging
import os
import atexit
import random
import uuid
import threading
import json
from collections import defaultdict
import time
import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO)

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

KAFKA_BROKER = os.environ['KAFKA_BROKER']

producer = Producer({'bootstrap.servers': KAFKA_BROKER})
logging.info("Kafka producer initialized successfully.")

admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
os_topic = NewTopic('order-stock-event', num_partitions=3, replication_factor=1)
op_topic = NewTopic('order-payment-event', num_partitions=3, replication_factor=1)
fs = admin_client.create_topics([os_topic, op_topic])

consumer = Consumer({
    "bootstrap.servers":KAFKA_BROKER,
    "group.id": "order-service-group",
    "auto.offset.reset": "earliest"
})

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry

def consume_kafka_events():
    logging.info("Kafka Consumer started...")
    consumer.subscribe(['stock-event', 'payment-event'])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.info(f"Kafka Consumer error: {msg.error()}")
            continue

        event = json.loads(msgpack.decode(msg.value()))
        logging.info(f'Received message:{event}')
        handle_event(event)

thread = threading.Thread(target=consume_kafka_events, daemon=True)
thread.start()
logging.info("Kafka Consumer started in a separate thread.")

def handle_event(event):
    event_type = event.get('event_type')
    order_id = event.get('order_id')
    user_id = event.get('user_id')

    if event_type == 'update_balance_success':
        items_quantities: dict[str, int] = defaultdict(int)
        order_entry = get_order_from_db(order_id)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity

        check_stock_event = {
            "event_type": "check_stock",
            "order_id": order_id,
            "user_id": order_entry.user_id,
            "items": items_quantities
        }
        producer.produce('order-stock-event', key=order_id, value=msgpack.encode(json.dumps(check_stock_event)))
        producer.flush()
    elif event_type == 'update_balance_fail':
        abort(400, f"Not enough balance! Order {order_id} did not go through")
    elif event_type == 'refund_balance_success':
        abort(400, f"Not enough balance! Refunded your order {order_id}.")
    elif event_type == 'check_stock_ack':
        success = event.get('success')
        if success:
            logging.info(f"Checkout complete: {order_id}")
            return Response(f"Checkout complete: {order_id}", status = 200)
        else:
            order_entry = get_order_from_db(order_id)
            # Rollback the reserved stock / unlock
            refund_event = {
                'event_type': "refund_balance",
                'order_id': order_id,
                'user_id': user_id,
                'amount': order_entry.total_cost
            }
            producer.produce('order-payment-event', key=order_id, value=msgpack.encode(json.dumps(refund_event)))
            abort(400, f"Failed to reserve stock for order {order_id}.")


def update_phase(item_id, user_id, amount_stock, amount_balance):
    update_stock_event = {
    "event_type": "update_stock",
    "item_id": item_id,
    "amount": amount_stock
    }
    update_stock_event_message = json.dumps(update_stock_event).encode('utf-8')

    update_balance_event = {
        "event_type": "update_balance",
        "user_id": user_id,
        "amount": amount_balance
    }
    update_balance_event_message = json.dumps(update_balance_event).encode('utf-8')

    producer.produce('order-stock-event', value=update_stock_event_message)
    producer.produce('order-payment-event', value=update_balance_event_message)
    producer.flush()

def update_stock(item_id, amount_stock):
    update_stock_event = {
    "event_type": "update_stock",
    "item_id": item_id,
    "amount": amount_stock
    }
    producer.produce('order-stock-event', value=json.dumps(update_stock_event))
    producer.flush()

def update_balance(user_id, amount_balance):
    update_balance_event = {
        "event_type": "update_balance",
        "user_id": user_id,
        "amount": amount_balance
    }
    producer.produce('order-payment-event', value=json.dumps(update_balance_event))
    producer.flush()


@app.post('/test/<item_id>/<user_id>/<amount_stock>/<amount_balance>')
def test(item_id: str, user_id:str, amount_stock: str, amount_balance: str):
    update_phase(item_id, user_id, amount_stock, amount_balance)
    return jsonify({"message": "ok"}), 200

@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")

# @app.post('/checkout/<order_id>')
# def checkout(order_id: str):
#     app.logger.debug(f"Checking out {order_id}")
#     order_entry: OrderValue = get_order_from_db(order_id)
#     items_quantities: dict[str, int] = defaultdict(int)
#     for item_id, quantity in order_entry.items:
#         items_quantities[item_id] += quantity
#
#     # Publish stock reservation events
#     for item_id, quantity in items_quantities.items():
#         producer.produce('stock-event', key=item_id, value=f"reserve:{item_id}:{quantity}".encode('utf-8'))
#     producer.flush()
#
#     # Publish funds reservation event
#     producer.produce('payment-event', key=order_entry.user_id, value=f"reserve:{order_entry.user_id}:{order_entry.total_cost}".encode('utf-8'))
#     producer.flush()
#
#     # Wait for responses from Stock and Payment Services
#     reserved_items = []
#     reserved_funds = False
#     timeout = 30  # Timeout in seconds
#     start_time = time.time()
#
#     consumer.subscribe(['order-event'])
#
#     while True:
#         msg = consumer.poll(1.0)  # Use a shorter poll timeout
#
#         if msg is None:
#             if time.time() - start_time > timeout:
#                 logging.error("Timeout waiting for reservation events")
#                 abort(400, "Timeout waiting for reservation events")
#             continue
#         if msg.error():
#             logging.info(f"Kafka Consumer error: {msg.error()}")
#             continue
#
#         event = msg.value().decode('utf-8')
#         event_type, entity_id, status = event.split(':')
#         logging.debug(f"Received event: {event}")
#
#         if event_type == 'stock' and status == 'reserved':
#             reserved_items.append(entity_id)
#             logging.debug(f"Stock reserved for item: {entity_id}")
#         elif event_type == 'stock' and status == 'failed':
#             logging.debug(f"Failed to reserve stock for item: {entity_id}, rolling back...")
#             # Rollback previously reserved items
#             for item_id in reserved_items:
#                 producer.produce('stock-event', key=item_id, value=f"unreserve:{item_id}".encode('utf-8'))
#             producer.flush()
#             # Unreserve funds
#             producer.produce('payment-event', key=order_entry.user_id, value=f"unreserve:{order_entry.user_id}:{order_entry.total_cost}".encode('utf-8'))
#             producer.flush()
#             abort(400, f"Failed to reserve stock for item: {entity_id}")
#
#         if event_type == 'payment' and status == 'reserved':
#             reserved_funds = True
#             logging.debug(f"Funds reserved for user: {entity_id}")
#         elif event_type == 'payment' and status == 'failed':
#             logging.debug(f"Failed to reserve funds for user: {entity_id}, rolling back...")
#             # Rollback previously reserved items
#             for item_id in reserved_items:
#                 producer.produce('stock-event', key=item_id, value=f"unreserve:{item_id}".encode('utf-8'))
#             producer.flush()
#             abort(400, f"Failed to reserve funds for user: {entity_id}")
#
#         if len(reserved_items) == len(items_quantities) and reserved_funds:
#             logging.debug("All items reserved and funds reserved.")
#             break
#
#     # Publish stock subtraction events
#     for item_id, quantity in items_quantities.items():
#         producer.produce('stock-event', key=item_id, value=f"subtract:{item_id}:{quantity}".encode('utf-8'))
#     producer.flush()
#
#     # Publish payment deduction event
#     producer.produce('payment-event', key=order_entry.user_id, value=f"pay:{order_entry.user_id}:{order_entry.total_cost}".encode('utf-8'))
#     producer.flush()
#
#     # Wait for subtraction and payment confirmations
#     subtracted_items = []
#     payment_confirmed = False
#     start_time = time.time()
#
#     while True:
#         msg = consumer.poll(1.0)  # Use a shorter poll timeout
#
#         if msg is None:
#             if time.time() - start_time > timeout:
#                 logging.error("Timeout waiting for subtraction and payment events")
#                 abort(400, "Timeout waiting for subtraction and payment events")
#             continue
#         if msg.error():
#             logging.info(f"Kafka Consumer error: {msg.error()}")
#             continue
#
#         event = msg.value().decode('utf-8')
#         event_type, entity_id, status = event.split(':')
#         logging.debug(f"Received event: {event}")
#
#         if event_type == 'stock' and status == 'subtracted':
#             subtracted_items.append(entity_id)
#             logging.debug(f"Stock subtracted for item: {entity_id}")
#         elif event_type == 'stock' and status == 'failed':
#             logging.debug(f"Failed to subtract stock for item: {entity_id}, rolling back...")
#             # Rollback previously subtracted items
#             for item_id in subtracted_items:
#                 producer.produce('stock-event', key=item_id, value=f"add:{item_id}:{items_quantities[item_id]}".encode('utf-8'))
#             producer.flush()
#             # Refund payment
#             producer.produce('payment-event', key=order_entry.user_id, value=f"refund:{order_entry.user_id}:{order_entry.total_cost}".encode('utf-8'))
#             producer.flush()
#             abort(400, f"Failed to subtract stock for item: {entity_id}")
#
#         if event_type == 'payment' and status == 'paid':
#             payment_confirmed = True
#             logging.debug(f"Payment confirmed for user: {entity_id}")
#         elif event_type == 'payment' and status == 'failed':
#             logging.debug(f"Failed to deduct payment for user: {entity_id}, rolling back...")
#             # Rollback previously subtracted items
#             for item_id in subtracted_items:
#                 producer.produce('stock-event', key=item_id, value=f"add:{item_id}:{items_quantities[item_id]}".encode('utf-8'))
#             producer.flush()
#             abort(400, f"Failed to deduct payment for user: {entity_id}")
#
#         if len(subtracted_items) == len(items_quantities) and payment_confirmed:
#             logging.debug("All items subtracted and payment confirmed.")
#             break
#
#     order_entry.paid = True
#     try:
#         db.set(order_id, msgpack.encode(order_entry))
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     app.logger.debug("Checkout successful")
#     return Response("Checkout successful", status=200)


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)

    payment_event = {
        'event_type': "payment",
        'order_id': order_id,
        'user_id': order_entry.user_id,
        'amount': order_entry.total_cost
    }
    producer.produce('order-payment-event', key=order_id, value=msgpack.encode(json.dumps(payment_event)))


    return 'OK'


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
