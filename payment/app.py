import logging
import os
import atexit
import uuid

import redis
import threading

import json

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO)

DB_ERROR_STR = "DB error"

KAFKA_BROKER = os.environ['KAFKA_BROKER']

producer = Producer({'bootstrap.servers': KAFKA_BROKER})
logging.info("Kafka producer initialized successfully.")

admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
topic = NewTopic('payment-event', num_partitions=3, replication_factor=1)
fs = admin_client.create_topics([topic])

consumer = Consumer({
    "bootstrap.servers":KAFKA_BROKER,
    "group.id": "payment-service-group",
    "auto.offset.reset": "earliest"
})

app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int
    reserved: int  # Add this field to keep track of reserved funds


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry

def consume_kafka_events():
    logging.info("Kafka Consumer started...")
    consumer.subscribe(['payment-event'])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.info(f"Kafka Consumer error: {msg.error()}")
            continue

        event = msg.value().decode('utf-8')
        logging.info(f"Received event: {event}")
        action, user_id, amount = event.split(':')
        amount = int(amount)

        if action == 'reserve':
            while True:
                user_entry = get_user_from_db(user_id)
                if user_entry.credit - user_entry.reserved < amount:
                    producer.produce('order-event', key=user_id, value=f"payment:{user_id}:failed".encode('utf-8'))
                    break
                user_entry.reserved += amount
                try:
                    with db.pipeline() as pipe:
                        pipe.watch(user_id)  # Watch the key
                        pipe.multi()  # Start the transaction
                        pipe.set(user_id, msgpack.encode(user_entry))  # Update the reserved credit
                        pipe.execute()  # Execute the transaction
                    producer.produce('order-event', key=user_id, value=f"payment:{user_id}:reserved".encode('utf-8'))
                    break
                except redis.exceptions.WatchError:
                    continue  # Retry if the watched key was modified by another transaction
        elif action == 'unreserve':
            user_entry = get_user_from_db(user_id)
            user_entry.reserved -= amount
            db.set(user_id, msgpack.encode(user_entry))
        elif action == 'pay':
            while True:
                user_entry = get_user_from_db(user_id)
                if user_entry.credit < amount:
                    producer.produce('order-event', key=user_id, value=f"payment:{user_id}:failed".encode('utf-8'))
                    break
                user_entry.credit -= amount
                user_entry.reserved -= amount
                try:
                    with db.pipeline() as pipe:
                        pipe.watch(user_id)  # Watch the key
                        pipe.multi()  # Start the transaction
                        pipe.set(user_id, msgpack.encode(user_entry))  # Update the credit
                        pipe.execute()  # Execute the transaction
                    producer.produce('order-event', key=user_id, value=f"payment:{user_id}:paid".encode('utf-8'))
                    break
                except redis.exceptions.WatchError:
                    continue  # Retry if the watched key was modified by another transaction
        elif action == 'refund':
            user_entry = get_user_from_db(user_id)
            user_entry.credit += amount
            db.set(user_id, msgpack.encode(user_entry))
            producer.produce('order-event', key=user_id, value=f"payment:{user_id}:refunded".encode('utf-8'))

        producer.flush()

thread = threading.Thread(target=consume_kafka_events, daemon=True)
thread.start()
logging.info("Kafka Consumer started in a separate thread.")

def handle_event(event):
    print(f"Received event: {event}")

@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0, reserved=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money, reserved=0))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit,
            "reserved": user_entry.reserved
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
