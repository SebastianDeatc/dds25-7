import asyncio
import functools
import logging
import os
import atexit
import uuid
import threading
import redis
import json
import time

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

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


app = Quart("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int  # Add this field to keep track of reserved stock
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

async def async_consumer_poll(loop, timeout):
    return await loop.run_in_executor(None, functools.partial(consumer.poll, timeout))

async def consume_kafka_events():
    # logging.info("Kafka Consumer started...")
    consumer.subscribe(['order-stock-event'])

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
    if event_type == "check_stock":
        items = event.get('items')
        # logging.info(f"items: {items}, type: {type(items)}")

        pre_stock_log = {
            "order_id": order_id,
            "timestamp": time.time(),
            "status": "PENDING",
            "event": event,
            "previous_value": db.get(user_id), 
            "service": "STOCK"
        }
        producer.produce('transaction-log', key=order_id, value=msgpack.encode(json.dumps(pre_stock_log)))
        producer.flush()

        lua_script = """
                        local items = cjson.decode(ARGV[1])
                        local items_new_amount = {}
                        for item_id, amount in pairs(items) do
                            local stock_obj = cmsgpack.unpack(redis.call('GET', item_id))
                            if not stock_obj then
                                return {false, "Item ID not found: " .. item_id}
                            end
                            local stock = tonumber(stock_obj.stock)
                            if not stock or stock < amount then
                                return {false, "Not enough stock for item: " .. item_id}
                            end
                            stock_obj.stock = stock - amount
                            items_new_amount[item_id] = stock_obj
                        end
                        for item_id, stock_obj in pairs(items_new_amount) do
                            redis.call('SET', item_id, cmsgpack.pack(stock_obj))
                        end
                        return {true, "Stock updated successfully"}
                        """

        result = db.eval(lua_script, 0, json.dumps(items))
        success = result[0] == 1
        # logging.info(f"result: {result}")
        check_stock_ack = {
            "event_type": "check_stock_ack",
            "order_id": order_id,
            "user_id": user_id,
            "success": success
        }
        producer.produce('stock-event', key= order_id, value=msgpack.encode(json.dumps(check_stock_ack)))
        producer.flush()


@app.post('/item/create/<price>')
async def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
async def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
async def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    # app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    # Start the consumer as an asyncio task
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
