import time
import pika
import threading
import json
import os
import numpy as np

MSG_JSON_KEY='msg'
VECTOR_CLOCK_JSON_KEY='vct'
SENDER_ID_JSON_KEY='sender'
MAX_NUMBER_OF_CLIENTS_FILE_NAME='max_number_of_clients.txt'
MAX_NUMBER_OF_CLIENTS = 1

CLIENT_ID = 0
mutex = threading.Lock()
vector_clock = []


def encode_packet(msg):
  packet = {MSG_JSON_KEY: str(msg), SENDER_ID_JSON_KEY: CLIENT_ID}
  return json.dumps(packet)


def decode_packet(packet):
  return json.loads(packet.decode('utf-8'))


def producer(channel: pika.adapters.blocking_connection.BlockingChannel):
  i = 1
  while True:
    time.sleep(1)
    with mutex:
      channel.basic_publish(exchange='multicast', routing_key=' ', body=encode_packet('What!? %d' % i))
    i = i + 1


def consume_message(ch, method, properties, body):
  with mutex:
    print(decode_packet(body), "consumer: %d" % CLIENT_ID)


def setup_channel(consumer: bool):
  connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
  consumer_id = 0
  queue_name = str(consumer_id) if consumer else ''
  while True:
    try:
      print("Trying: %s" % queue_name)
      channel: pika.adapters.blocking_connection.BlockingChannel = connection.channel()
      channel.exchange_declare(exchange='multicast', exchange_type='fanout')
      channel.queue_declare(queue=queue_name, exclusive=True)
      channel.queue_bind(exchange='multicast', queue=queue_name)
      break
    except pika.exceptions.ChannelClosedByBroker:
      consumer_id = consumer_id + 1
      queue_name = str(consumer_id) if consumer else ''
  return channel, queue_name


def setup_produce_channel():
  (channel, queue_name) = setup_channel(consumer=False)
  return channel


def setup_consumer_channel():
  (channel, queue_name) = setup_channel(consumer=True)
  global CLIENT_ID
  CLIENT_ID = int(queue_name)
  channel.basic_consume(on_message_callback=consume_message, queue=queue_name, auto_ack=True)
  return channel


if __name__ == '__main__':
  with open(os.path.join(os.path.dirname(__file__), MAX_NUMBER_OF_CLIENTS_FILE_NAME), 'r') as f:
    MAX_NUMBER_OF_CLIENTS = int(f.readline())
    vector_clock = np.repeat(0, MAX_NUMBER_OF_CLIENTS)
  threading.Thread(target=producer, args=(setup_produce_channel(),)).start()
  setup_consumer_channel().start_consuming()
