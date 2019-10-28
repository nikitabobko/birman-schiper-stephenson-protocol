import pika


# def callback():
#   pass


if __name__ == '__main__':
  connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
  channel: pika.adapters.blocking_connection.BlockingChannel = connection.channel()
  channel.exchange_declare(exchange='multicast', exchange_type='fanout')
  result = channel.queue_declare(queue='my_queue')
  channel.queue_bind(exchange='multicast', queue='my_queue')

  channel.basic_publish(exchange='multicast', body='foo', routing_key=' ')
