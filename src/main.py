import time
import pika
import threading
from random import randrange


def producer(channel: pika.adapters.blocking_connection.BlockingChannel):
  i = 1
  while True:
    time.sleep(1)
    channel.basic_publish(exchange='multicast', routing_key=' ', body='What!? %d' % i)
    i = i + 1


def consume_message(ch, method, properties, body):
  print(body.decode('utf-8'))


def setup_channel():
  connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
  channel: pika.adapters.blocking_connection.BlockingChannel = connection.channel()
  channel.exchange_declare(exchange='multicast', exchange_type='fanout')
  queue_name = ''
  print(queue_name)
  channel.queue_declare(queue=queue_name)
  channel.queue_bind(exchange='multicast', queue=queue_name)
  return channel, queue_name


def setup_produce_channel():
  (channel, queue_name) = setup_channel()
  return channel


def setup_consumer_channel():
  (channel, queue_name) = setup_channel()
  channel.basic_consume(on_message_callback=consume_message, queue=queue_name, auto_ack=True)
  return channel


if __name__ == '__main__':
  connection = None
  threading.Thread(target=producer, args=(setup_produce_channel(),)).start()
  setup_consumer_channel().start_consuming()
