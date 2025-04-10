import asyncio
import functools
import logging
import os
import atexit
import random
import sys
import uuid
import threading
import json
from collections import defaultdict
import time
import redis
import requests

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# One way is to go one level up from the current file's directory.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))

# Insert the project root directory at the beginning of sys.path if it's not already there.
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from log import save_log


logging.basicConfig(level=logging.INFO)

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

KAFKA_BROKER = os.environ['KAFKA_BROKER']

producer = Producer({'bootstrap.servers': KAFKA_BROKER})
logging.info("Kafka producer initialized successfully.")

admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
os_topic = NewTopic('order-stock-event', num_partitions=3, replication_factor=1)
op_topic = NewTopic('order-payment-event', num_partitions=3, replication_factor=1)

# transaction_log_topic = NewTopic(
#     'transaction-log',
#     num_partitions=1,
#     replication_factor=1,
#     config={'cleanup.policy': 'compact'}  # Enables log compaction
# )

fs = admin_client.create_topics([os_topic, op_topic])

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

# log_consumer = Consumer({
#     "bootstrap.servers":KAFKA_BROKER,
#     "group.id": "logs-order-service-group",
#     "enable.auto.commit": False,
#     "auto.offset.reset": "earliest"
# })

# def handle_log(transaction_event):
#     order_id = transaction_event.get('order_id')
#     step = transaction_event.get('step')

#     # Checkout started but we did not arrive at payment step, so we
#     if step == "CHECKOUT_STARTED":


# log_consumer.subscribe(["transaction-log"])
# logging.info("Log Consumer started")

# dummy_log = {
#         "order_id": "DUMMY_ORDER_ID",
#         "status": "DUMMY_STATUS",
#         "step": "DUMMY_STEP"
#     }
# producer.produce('transaction-log', key="DUMMY_ORDER_ID", value=msgpack.encode(json.dumps(dummy_log)))
# producer.flush()

# while True:
#     msg = log_consumer.poll(timeout=1.0)
#     logging.info(f"log consumer processing {msg}")
#     if msg is None:
#         # Wait for all partitions to reach EOF
#         if log_consumer.assignment():
#             positions = log_consumer.position(log_consumer.assignment())
#             logging.info(f"Current positions: {positions}")
#             highwaters = log_consumer.get_watermark_offsets(log_consumer.assignment()[0])
#             logging.info(f"Current highwaters: {highwaters}")
#             if all(pos.offset >= highwaters[1] for pos in positions):
#                 break
#         continue

#     # Decode and handle message
#     transaction_event = json.loads(msgpack.decode(msg.value()))
#     status = transaction_event.get('status')

#     order_id = transaction_event.get('order_id')
#     step = transaction_event.get('step')
#     logging.info(f"order {order_id}, status: {status}, step: {step}")

#     # Only handle if not completed
#     # TODO: UNCOMMENT THIS
#     # if status == "PENDING":
#     #     handle_log(transaction_event)

#     # Manually commit the offset if needed
#     log_consumer.commit(msg)

# log_consumer.close()
# logging.info("Log Consumer closed")

consumer = Consumer({
    "bootstrap.servers":KAFKA_BROKER,
    "group.id": "order-service-group",
    "auto.offset.reset": "earliest"
})

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Quart("order-service")



def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

order_futures = {}
order_futures_lock = asyncio.Lock()

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

async def async_consumer_poll(loop, timeout):
    return await loop.run_in_executor(None, functools.partial(consumer.poll, timeout))

async def consume_kafka_events():
    logging.info("Kafka Consumer started...")
    consumer.subscribe(['stock-event', 'payment-event'])
    while True:
        msg = await async_consumer_poll(asyncio.get_event_loop(), 1.0)
        if msg is None:
            continue
        if msg.error():
            logging.info(f"Kafka Consumer error: {msg.error()}")
            continue

        event = json.loads(msgpack.decode(msg.value()))
        #logging.info(f'Received message:{event}')
        asyncio.get_running_loop().create_task(handle_event(event))

def start_consumer_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(consume_kafka_events())

threading.Thread(target=start_consumer_thread, daemon=True).start()

async def handle_event(event):
    event_type = event.get('event_type')
    order_id = event.get('order_id')
    user_id = event.get('user_id')

    async def update_order_result(status, message):
        async with order_futures_lock:
            future = order_futures.get(order_id)
            if future and not future.done():
                future.set_result((status, message))

    if event_type == 'payment_success':
        # payment was successfully completed so move on to checking stock
        items_quantities: dict[str, int] = defaultdict(int)
        order_entry = get_order_from_db(order_id)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity

        payment_commit_log = {
            "order_id": order_id,
            "timestamp": time.time(),
            "status": "COMPLETED",
            "event": event,
            "service": "PAYMENT"
        }
        save_log(payment_commit_log)

        check_stock_event = {
            "event_type": "check_stock",
            "order_id": order_id,
            "user_id": order_entry.user_id,
            "items": items_quantities
        }
        producer.produce('order-stock-event', key=order_id, value=msgpack.encode(json.dumps(check_stock_event)))
        producer.flush()
    elif event_type == 'payment_fail':
        await update_order_result(400, f"Not enough balance! Order {order_id} did not go through")
        # logging.error(f"Payment failed for order {order_id}")
    elif event_type == 'refund_payment_success':
        await update_order_result(400, f"Not enough balance! Refunded your order {order_id}.")
        # logging.error(f"Refund processed for order {order_id} indicates a failure")
    elif event_type == 'check_stock_ack':
        # logging.info('In check stock ack')
        success = event.get('success')
        stock_commit_log = {
            "order_id": order_id,
            "timestamp": time.time(),
            "status": "COMPLETED",
            "event": event,
            "service": "STOCK"
        }
        save_log(stock_commit_log)
        producer.produce('transaction-log', key=order_id, value=msgpack.encode(json.dumps(stock_commit_log)))
        producer.flush()
        if success:
            # if stock check suceeded checkout is successfully completed
            await update_order_result(200, f"Checkout complete: {order_id}")
            # logging.info(f"Checkout complete: {order_id}")
        else:
            # if the stock check fails we should refund the order
            # logging.info('In handle event, refunding')
            order_entry = get_order_from_db(order_id)
            refund_event = {
                'event_type': "refund_payment",
                'order_id': order_id,
                'user_id': user_id,
                'amount': order_entry.total_cost
            }
            producer.produce('order-payment-event', key=order_id, value=msgpack.encode(json.dumps(refund_event)))
            producer.flush()
            await update_order_result(400, f"Failed to reserve stock for order {order_id}.")
    else:
        logging.warning(f"Unhandled event type: {event_type} for order {order_id}")

@app.route('/create/<user_id>', methods=['POST'])
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    #logging.info(f"Order created successfully with ID: {key}")
    return jsonify({'order_id': key})


@app.route('/batch_init/<n>/<n_items>/<n_users>/<item_price>', methods=['POST'])
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

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


@app.route('/find/<order_id>', methods=['GET'])
async def find_order(order_id: str):
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
    

@app.route('/addItem/<order_id>/<item_id>/<quantity>', methods=['POST'])
async def add_item(order_id: str, item_id: str, quantity: int):
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


@app.route('/checkout/<order_id>', methods=['POST'])
async def checkout(order_id: str):
    # app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)

    async with order_futures_lock:
        future = asyncio.Future()
        order_futures[order_id] = future


    # create and send the payment event message to payment microservice
    payment_event = {
        'event_type': "payment",
        'order_id': order_id,
        'user_id': order_entry.user_id,
        'amount': order_entry.total_cost
    }
    producer.produce('order-payment-event', key=order_id, value=msgpack.encode(json.dumps(payment_event)))
    producer.flush()
    # log = {
    #     "order_id": order_id,
    #     "status": "PENDING",
    #     "step": "CHECKOUT_STARTED"
    # }
    # producer.produce('transaction-log', key=order_id, value=msgpack.encode(json.dumps(log)))
    # producer.flush()
    try:
        # await the future result with a timeout.
        # logging.info('awaiting for future in checkout')
        status_code, message = await asyncio.wait_for(future, timeout=10)
    except asyncio.TimeoutError:
        async with order_futures_lock:
            order_futures.pop(order_id, None)
        abort(408, f"Timeout waiting for event for order {order_id}")
    async with order_futures_lock:
        order_futures.pop(order_id, None)
    if message is None:
        abort(500, "Internal error: event signaled but no result found")

    checkout_commit_log = {
            "order_id": order_id,
            "timestamp": time.time(),
            "status": "CHECKOUT_COMPLETED",
            "service": "ORDER"
    }
    save_log(checkout_commit_log)
    return Response(message, status=status_code)

if __name__ == '__main__':
    print("Current directory:", current_dir)
    print("Project root:", project_root)
    save_log({'a': 1})
    # app.run(host="0.0.0.0", port=8000, debug=True)
# else:
#     gunicorn_logger = logging.getLogger('gunicorn.error')
#     app.logger.handlers = gunicorn_logger.handlers
#     app.logger.setLevel(gunicorn_logger.level)

