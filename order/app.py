import json
import logging
import os
import atexit
import random
import uuid
import threading
from collections import defaultdict

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

        logging.info('Received message:{}'.format(msg.value().decode('utf-8')))

thread = threading.Thread(target=consume_kafka_events, daemon=True)
thread.start()
logging.info("Kafka Consumer started in a separate thread.")

# def update_phase():
#     update_stock_event = {}
#     producer.produce('order-stock-event', value=)
#     producer.flush()

@app.post('/checkout_check/<order_id>')
def checkout_check_phase(order_id):

    order_entry: OrderValue = get_order_from_db(order_id)
    items_quantities = get_items_quantities(order_entry=order_entry)

    check_stock_event = {
        "order_id": order_id,
        "items": items_quantities,
    }
    check_balance_event = {
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost
    }
    try:
        producer.produce(
            topic=os_topic.name,
            key=msgpack.encode(order_id),
            value=msgpack.encode(json.dumps(check_stock_event)),
        )
        producer.flush()
        app.logger.info("CheckStockEvent produced for order %s", order_id)

        producer.produce(
            topic=op_topic.name,
            key=msgpack.encode(order_id),
            value=msgpack.encode(json.dumps(check_balance_event)),
        )
        producer.flush()
        app.logger.info("CheckPaymentEvent produced for order %s", order_id)

    except Exception as e:
            abort(500, f"Error producing Kafka events: {str(e)}")
    else:
        stock_confirmed = False
        money_confirmed = False
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                app.logger.error(f"Consumer error: {msg.error()}")
                continue
            msg_value = json.loads(msgpack.decode(msg.value()))
            
            match msg.topic():
                case os_topic.name:
                    if msg_value["status"] == "STOCK_AVAILABLE":
                        app.logger.info(f"Stock available for order: {order_id}")
                        stock_confirmed = True
                        #TODO: lock stock
                        if stock_confirmed and money_confirmed:
                            pass
                            #TODO: proceed with payment 
                            break
                    elif msg_value["status"] == "STOCK_UNAVAILABLE":
                        app.logger.info(f"Stock unavailable for order: {order_id}")
                        stock_confirmed = False
                        #TODO: rollback checkout or something
                        break
                case op_topic.name:
                    if msg_value["status"] == "MONEY_AVAILABLE":
                        app.logger.info(f"Money available for order: {order_id}")
                        money_confirmed = True
                        if stock_confirmed and money_confirmed:
                            pass
                            #TODO: proceed with payment
                            break
                    elif msg_value["status"] == "MONEY_UNAVAILABLE":
                        app.logger.info(f"Money unavailable for order: {order_id}")
                        money_confirmed = False
                        #TODO: rollback checkout or something
                        break
        #TODO: producer and consumer in stock and payment for checking + testing
        consumer.close()

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
            "total_cost": order_entry.total_cost
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

def get_items_quantities(order_entry: OrderValue) -> dict[str, int]:
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    return items_quantities

@app.post('/checkout/<order_id>') #NOTE this should probably decorate kafka checkout_phase()
def checkout(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    items_quantities = get_items_quantities(order_entry=order_entry)
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if stock_reply.status_code != 200:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            abort(400, f'Out of stock on item_id: {item_id}')
        removed_items.append((item_id, quantity))
    user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    if user_reply.status_code != 200:
        # If the user does not have enough credit we need to rollback all the item stock subtractions
        rollback_stock(removed_items)
        abort(400, "User out of credit")
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
