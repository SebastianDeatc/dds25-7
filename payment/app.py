import asyncio
import functools
import logging
import os
import atexit
import sys
import uuid
import time
import requests

from quart import Quart, jsonify, abort, Response

import redis
import threading

import json

from msgspec import msgpack, Struct
from werkzeug.exceptions import HTTPException
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def save_log(new_entry):
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # log_file = os.path.join(current_dir, 'logs.json')
    log_file = '/logs/log.json'

    logging.info(f'log_file is: {log_file}')
    # logging.info(f'path to log file is: {current_dir}')
    try:
        # Try reading the current log data; if the file doesn't exist or is empty, start with an empty list.
        try:
            with open(log_file, 'r') as f:
                logs = json.load(f)
                logging.info(f'log is: {logs}')
                if not isinstance(logs, list):
                    logs = []
        except (FileNotFoundError, json.JSONDecodeError):
            logs = []
            logging.error('FILE NOT FOUND OR SOMETHING')
        # Append the new log entry
        logs.append(new_entry)
        
        # Write back the updated log list
        with open(log_file, 'w') as f:
            json.dump(logs, f, indent=4)
            logging.info(f'logs are: {logs}')

    except Exception as e:
        logging.error(f"Error saving log: {e}")

logging.basicConfig(level=logging.INFO)

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

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

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

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
        # Decode the previous value from Redis
        previous_value_bytes = db.get(user_id)
        previous_value = None
        if previous_value_bytes:
            try:
                previous_value = msgpack.decode(previous_value_bytes)  # Deserialize if it's msgpack-encoded
            except Exception:
                previous_value = previous_value_bytes.decode('utf-8')  # Fallback to UTF-8 decoding

        pre_payment_log = {
            "order_id": order_id,
            "timestamp": time.time(),
            "status": "PENDING",
            "event": event,
            "previous_value": previous_value,  # Ensure this is JSON-serializable
            "service": "PAYMENT"
        }
        save_log(pre_payment_log)
        # producer.produce('transaction-log', key=order_id, value=msgpack.encode(json.dumps(payment_commit_log)))
        # producer.flush()

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

        producer.produce('payment-event', key=order_id, value=payment_ack_message)
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

def on_startup():
    # pause_order_consumer()

    # go through the log file and find the latest (via timestamp) CHECKOUT_COMPLETED log

    # filter the remaining logs such that only logs with a higher (later) timestamp remain

    # IF there are 0 remaining logs -> no compensating actions needed, return
    # ELSE -> find the latest log that belongs to the PAYMENT service
    
    # IF this log is a PAYMENT_COMPLETED log -> refund the payment and return
    # ELSE (it has to be PAYMENT_PENDING log) -> using the previous value field in the log json
        # evaluate if the payment database needs to be corrected and act accordingly, return

    # resume_order_consumer()
    pass

def pause_order_consumer():
    try:
        response = requests.post(f"{GATEWAY_URL}/orders/consumer/pause")
        if response.status_code == 200:
            print("Consumer paused successfully")
        else:
            print(f"Failed to pause consumer: {response.text}")
    except Exception as e:
        print(f"An error occurred: {e}")


def resume_order_consumer():
    try:
        response = requests.post(f"{GATEWAY_URL}/orders/consumer/resume")
        if response.status_code == 200:
            print("Consumer paused successfully")
        else:
            print(f"Failed to pause consumer: {response.text}")
    except Exception as e:
        print(f"An error occurred: {e}")


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})

@app.post('/pause')
def pause():
    pause_order_consumer()
    return jsonify({"msg": "paused"})

@app.post('/resume')
def resume():
    resume_order_consumer()
    return jsonify({"msg": "resumed"})

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
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
