import asyncio
import functools
import logging
import os
import atexit
import uuid

from quart import Quart, jsonify, abort, Response

import redis
import threading

import json

from msgspec import msgpack, Struct
from werkzeug.exceptions import HTTPException
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from redis.sentinel import Sentinel

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

app = Quart("payment-service")


SENTINEL_HOST = os.getenv("REDIS_SENTINEL_HOST")
SENTINEL_PORT = int(os.getenv("REDIS_SENTINEL_PORT"))
MASTER_NAME = os.getenv("REDIS_MASTER_NAME")
REDIS_PASS = os.getenv("REDIS_PASSWORD")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

sentinel = Sentinel(
    [(SENTINEL_HOST, SENTINEL_PORT)],
    socket_timeout=1.0,
    password=REDIS_PASS
)
db = sentinel.master_for(MASTER_NAME, socket_timeout=1.0, db=REDIS_DB, password=REDIS_PASS)



def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


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

async def async_consumer_poll(loop, timeout):
    return await loop.run_in_executor(None, functools.partial(consumer.poll, timeout))

async def consume_kafka_events():
    # logging.info("Kafka Consumer started...")
    consumer.subscribe(['order-payment-event'])

    while True:
        msg = await async_consumer_poll(asyncio.get_event_loop(), 1.0)
        if msg is None:
            continue
        if msg.error():
            # logging.info(f"Kafka Consumer error: {msg.error()}")
            continue

        event = json.loads(msgpack.decode(msg.value()))
        # logging.info(f'Received message:{event}')
        # handle_event(event)
        await asyncio.get_running_loop().create_task(handle_event(event))


def start_consumer_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(consume_kafka_events())

threading.Thread(target=start_consumer_thread, daemon=True).start()

async def handle_event(event):
    event_type = event.get('event_type')
    order_id = event.get('order_id')
    user_id = event.get('user_id')
    amount = event.get('amount')
    if event_type == "payment":
        resp = await remove_credit(user_id, amount)
        if not resp.status_code == 200:
            payment_fail_event = {
                "event_type": "payment_fail",
                "order_id": order_id,
                "user_id": user_id,
                "amount": amount
            }
            payment_ack_message = msgpack.encode(json.dumps(payment_fail_event))
        else:
            payment_success_event = {
                "event_type": "payment_success",
                "order_id": order_id,
                "user_id": user_id,
                "amount": amount
            }
            payment_ack_message = msgpack.encode(json.dumps(payment_success_event))
        producer.produce('payment-event', key = order_id, value=payment_ack_message)
        producer.flush()
    elif event_type == "refund_payment":
        resp = await add_credit(user_id, amount)
        if not resp.status_code == 200:
            refund_payment_fail_event = {
                "event_type": "refund_payment_fail",
                "order_id": order_id,
                "user_id": user_id,
                "amount": amount
            }
            refund_payment_ack_message = msgpack.encode(json.dumps(refund_payment_fail_event))
        else:
            refund_payment_success_event = {
                "event_type": "refund_payment_success",
                "order_id": order_id,
                "user_id": user_id,
                "amount": amount
            }
            refund_payment_ack_message = msgpack.encode(json.dumps(refund_payment_success_event))
        producer.produce('payment-event', key = order_id, value=refund_payment_ack_message)
        producer.flush()

@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: int):
    lua_script = """
                local user_id = cjson.decode(ARGV[1])
                local amount = cjson.decode(ARGV[2])
                local user_obj = cmsgpack.unpack(redis.call('GET', user_id))
                if not user_obj then
                    return {false, "User ID not found: " .. user_id}
                end
                local user_credit = tonumber(user_obj.credit)
                user_obj.credit = user_credit + amount
                redis.call('SET', user_id, cmsgpack.pack(user_obj))
                return {true, "User credit updated successfully"}
            """

    result = db.eval(lua_script, 0, json.dumps(user_id), json.dumps(amount))
    return Response("Payment added.", status=200) if result[0] == 1 else Response("Payment failed to be added.", status=400)


@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    lua_script = """
                    local user_id = cjson.decode(ARGV[1])
                    local amount = cjson.decode(ARGV[2])
                    local user_obj = cmsgpack.unpack(redis.call('GET', user_id))
                    if not user_obj then
                        return {false, "User ID not found: " .. user_id}
                    end
                    local user_credit = tonumber(user_obj.credit)
                    if not user_credit or user_credit < amount then
                        return {false, "Not enough credit for user: " .. user_id}
                    end
                    user_obj.credit = user_credit - amount
                    redis.call('SET', user_id, cmsgpack.pack(user_obj))
                    return {true, "User credit updated successfully"}
                """

    result = db.eval(lua_script, 0, json.dumps(user_id), json.dumps(amount))
    return Response("Payment subtracted.", status=200) if result[0] == 1 else Response("Payment failed to be subtracted.", status=400)


@app.route("/health", methods=["GET"])
async def health():
    return "OK", 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)