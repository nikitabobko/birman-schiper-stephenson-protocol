import pika


def callback(ch, method, properties, body):
  print(body)
  pass


if __name__ == '__main__':
  connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
  channel: pika.adapters.blocking_connection.BlockingChannel = connection.channel()
  channel.exchange_declare(exchange='multicast', exchange_type='fanout')
  queue_name = 'my_queue'
  result = channel.queue_declare(queue=queue_name)
  channel.queue_bind(exchange='multicast', queue=queue_name)
  channel.basic_consume(on_message_callback=callback, queue=queue_name)
  channel.start_consuming()
