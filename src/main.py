import time
import pika
import threading
import json
import sys
import numpy as np
from random import randrange
from queue import PriorityQueue

MSG_JSON_KEY = "msg"
VECTOR_CLOCK_JSON_KEY = "vct"
SENDER_ID_JSON_KEY = "sender"
LOG = False
EXCHANGE_NAME = "birman-schiper-stephenson"

client_id = 0
mutex = threading.RLock()
vector_clock = np.array(())
my_queue = PriorityQueue()
producer_started = False


def compare(msg_comparable_wrapper_1, msg_comparable_wrapper_2):
  vector1 = msg_comparable_wrapper_1.msg[VECTOR_CLOCK_JSON_KEY]
  vector2 = msg_comparable_wrapper_2.msg[VECTOR_CLOCK_JSON_KEY]
  if np.all(vector1 > vector2):
    return 1
  if np.all(vector1 < vector2):
    return -1
  # Don't think too much. It's just one of possible ways to provide well-order relation
  # on top of vector clocks which by default support partial order relation
  index = np.argmax(np.absolute(vector1 - vector2))
  return vector1[index] - vector2[index]


class MsgComparableWrapper:
  def __init__(self, msg):
    self.msg = msg

  def __lt__(self, obj):
    return compare(self, obj) < 0

  def __le__(self, obj):
    return compare(self, obj) <= 0

  def __eq__(self, obj):
    return compare(self, obj) == 0

  def __ne__(self, obj):
    return compare(self, obj) != 0

  def __gt__(self, obj):
    return compare(self, obj) > 0

  def __ge__(self, obj):
    return compare(self, obj) >= 0


def log(msg):
  if LOG:
    print(f"[id: {client_id}]", msg)


def encode_packet(msg_string, vector_clock):
  msg = {MSG_JSON_KEY: str(msg_string), SENDER_ID_JSON_KEY: client_id, VECTOR_CLOCK_JSON_KEY: vector_clock.tolist()}
  return json.dumps(msg), msg


def decode_packet(packet):
  return json.loads(packet.decode("utf-8"))


def producer(channel: pika.adapters.blocking_connection.BlockingChannel):
  i = 1
  while True:
    sleep_time_secs = randrange(1, 11)
    log(f"Producer sleeps for {sleep_time_secs} secs")
    time.sleep(sleep_time_secs)
    with mutex:
      tmp_vector_clock = vector_clock.copy()
      tmp_vector_clock[client_id] = tmp_vector_clock[client_id] + 1
      (packet, msg) = encode_packet(f"{i}-th msg from {client_id}", tmp_vector_clock)
      log(f"Sending packet: {packet}")
      channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="", body=packet)
      deliver_msg(msg)
    i = i + 1


def start_producer_async_if_not_started_yet():
  with mutex:
    global producer_started
    if not producer_started:
      producer_started = True
      log('Starting producer')
      threading.Thread(target=producer, args=(setup_produce_channel(),)).start()


def msg_delivered(msg):
  print("MSG delivered ------->", msg[MSG_JSON_KEY])


def normalize_clocks(clock1, clock2):
  max_len = max(len(clock1), len(clock2))
  clock1 = np.pad(clock1, (0, max_len - len(clock1)), 'constant', constant_values=(0, 0))
  clock2 = np.pad(clock2, (0, max_len - len(clock2)), 'constant', constant_values=(0, 0))
  return clock1, clock2


def deliver_msg(msg):
  clock = msg[VECTOR_CLOCK_JSON_KEY]
  global vector_clock
  clock, vector_clock = normalize_clocks(clock, vector_clock)
  vector_clock = np.maximum(clock, vector_clock)
  msg_delivered(msg)


def consume_packet(ch, method, properties, body):
  start_producer_async_if_not_started_yet()
  with mutex:
    msg = decode_packet(body)
    log(f"Consumed msg: {msg}")
    log(f"Approximate queue size: {my_queue.qsize()}")
    sender_id = msg[SENDER_ID_JSON_KEY]
    if sender_id == client_id:
      # skip messages send by me because them are delivered in producer
      log("Skipping own message. Already delivered")
      return
    my_queue.put(MsgComparableWrapper(msg))
    while not my_queue.empty():
      msg = my_queue.get().msg
      sender_id = msg[SENDER_ID_JSON_KEY]
      msg_vector_clock = msg[VECTOR_CLOCK_JSON_KEY]
      global vector_clock
      vector_clock, msg_vector_clock = normalize_clocks(vector_clock, msg_vector_clock)
      tmp_vector_clock = vector_clock.copy()
      tmp_vector_clock[sender_id] = tmp_vector_clock[sender_id] + 1
      if tmp_vector_clock[sender_id] == msg_vector_clock[sender_id] and np.all(tmp_vector_clock >= msg_vector_clock):
        deliver_msg(msg)
      else:
        break


def setup_channel(consumer: bool):
  connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
  consumer_id = 0
  queue_name = str(consumer_id) if consumer else ""
  while True:
    try:
      if consumer:
        log(f"Trying queue_name: {queue_name}")
      channel: pika.adapters.blocking_connection.BlockingChannel = connection.channel()
      channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout")
      channel.queue_declare(queue=queue_name, exclusive=True)
      channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)
      if consumer:
        log(f"My queue_name is {queue_name}")
      break
    except pika.exceptions.ChannelClosedByBroker:
      consumer_id = consumer_id + 1
      queue_name = str(consumer_id) if consumer else ""
  return channel, queue_name


def setup_produce_channel():
  (channel, queue_name) = setup_channel(consumer=False)
  return channel


def setup_consumer_channel():
  (channel, queue_name) = setup_channel(consumer=True)
  global client_id
  client_id = int(queue_name)
  global vector_clock
  vector_clock = np.repeat(0, client_id + 1)
  channel.basic_consume(on_message_callback=consume_packet, queue=queue_name, auto_ack=True)
  return channel


def consumer(consumer_channel):
  consumer_channel.start_consuming()


def print_help_and_exit():
  print('usage: python src/main.py add|start')
  sys.exit()


if __name__ == "__main__":
  if len(sys.argv) - 1 != 1:
    print_help_and_exit()
  command = sys.argv[1]
  if command != 'add' and command != 'start':
    print_help_and_exit()

  # Firstly setup consumer channel and only after that producer channel. Otherwise clocks wouldn't be initialized
  consumer_channel = setup_consumer_channel()
  threading.Thread(target=consumer, args=(consumer_channel,)).start()
  if command == 'start':
    start_producer_async_if_not_started_yet()
