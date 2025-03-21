import logging
import os
import atexit
import uuid
import threading
import redis
import json

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from werkzeug.exceptions import HTTPException
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO)

DB_ERROR_STR = "DB error"

KAFKA_BROKER = os.environ['KAFKA_BROKER']

producer = Producer({'bootstrap.servers': KAFKA_BROKER})
logging.info("Kafka producer initialized successfully.")

admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
topic = NewTopic('stock-event', num_partitions=3, replication_factor=1)
fs = admin_client.create_topics([topic])

consumer = Consumer({
    "bootstrap.servers":KAFKA_BROKER,
    "group.id": "stock-service-group",
    "auto.offset.reset": "earliest"
})


app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    reserved: int  # Add this field to keep track of reserved stock
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry

def consume_kafka_events():
    logging.info("Kafka Consumer started...")
    consumer.subscribe(['order-stock-event'])

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
    if event_type == "check_stock":
        quantity = event.get('quantity')
        items = event.get('items')
        success = True
        for item_id, quantity in items.items():
            if success:
                stock = get_item_from_db(item_id).stock
                available = stock - quantity > 0
                if available:
                    logging.info(f"Locking item: {item_id}")
                    #TODO: lock
                else:
                    logging.info(f"Item {item_id} not available")
                    #TODO: release locks
                    success = False
                    break

        check_stock_ack = {
            "event_type": "check_stock_ack",
            "order_id": order_id,
            "user_id": user_id,
            "success": success
        }
        producer.produce('stock-event', key= order_id, value=msgpack.encode(json.dumps(check_stock_ack)))
        producer.flush()


    # if event_type == "update_stock":
    #     try:
    #         remove_stock(item_id, amount)
    #     except HTTPException:
    #         update_stock_fail_event = {
    #             "event_type": "update_stock_fail",
    #             "item_id": item_id,
    #             "amount": amount
    #         }
    #         update_stock_ack_message = json.dumps(update_stock_fail_event).encode('utf-8')
    #     else:
    #         update_stock_ack_event = {
    #             "event_type": "update_stock_ack",
    #             "item_id": item_id,
    #             "amount": amount
    #         }
    #         update_stock_ack_message = json.dumps(update_stock_ack_event).encode('utf-8')
    #     producer.produce('order-payment-event', value=update_stock_ack_message)
    #     producer.flush()
    # elif event_type == "rollback_stock":
    #     try:
    #         add_stock(item_id, amount)
    #     except HTTPException:
    #         rollback_stock_fail_event = {
    #             "event_type": "rollback_stock_fail",
    #             "item_id": item_id,
    #             "amount": amount
    #         }
    #         rollback_stock_ack_message = json.dumps(rollback_stock_fail_event).encode('utf-8')
    #     else:
    #         rollback_stock_ack_event = {
    #             "event_type": "rollback_stock_ack",
    #             "item_id": item_id,
    #             "amount": amount
    #         }
    #         rollback_stock_ack_message = json.dumps(rollback_stock_ack_event).encode('utf-8')
    #     producer.produce('order-payment-event', value=rollback_stock_ack_message)
    #     producer.flush()


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, reserved=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, reserved=0, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
