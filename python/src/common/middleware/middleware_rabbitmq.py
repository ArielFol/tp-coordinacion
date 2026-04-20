import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()
            self.queue_name = queue_name
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.consuming = False
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def start_consuming(self, on_message_callback):
        if self.consuming:
            return
        self.consuming = True

        try:
            def callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag = method.delivery_tag)

                def nack():
                    ch.basic_nack(delivery_tag = method.delivery_tag)

                on_message_callback(body, ack, nack)

            
            self.channel.basic_qos(prefetch_count = 1)
            self.channel.basic_consume(queue = self.queue_name, on_message_callback = callback, auto_ack = False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            self.consuming = False
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            self.consuming = False
            raise MessageMiddlewareMessageError(str(e))
    
    def stop_consuming(self):
        try:
            if self.consuming:
                self.channel.stop_consuming()
                self.consuming = False
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        
    def send(self, message):
        try:
            self.channel.basic_publish(exchange = '', routing_key = self.queue_name, body = message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))
        
    def close(self):
        try:
            self.stop_consuming()
            if self.channel and self.channel.is_open:
                self.channel.close()

            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()
            self.exchange_name = exchange_name
            self.routing_keys = routing_keys
            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

            result = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue

            for key in routing_keys:
                self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name, routing_key=key)

            self.consuming = False
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def start_consuming(self, on_message_callback):
        if self.consuming:
            return
        self.consuming = True

        try:
            def callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag = method.delivery_tag)

                def nack():
                    ch.basic_nack(delivery_tag = method.delivery_tag)

                on_message_callback(body, ack, nack)
                
            self.channel.basic_qos(prefetch_count = 1)
            self.channel.basic_consume(queue = self.queue_name, on_message_callback = callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            self.consuming = False
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            self.consuming = False
            raise MessageMiddlewareMessageError(str(e))
    
    def stop_consuming(self):
        try:
            if self.consuming:
                self.channel.stop_consuming()
                self.consuming = False
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        
    def send(self, message):
        try:
            for key in self.routing_keys:
                self.channel.basic_publish(exchange = self.exchange_name, routing_key = key, body = message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))
        
    def close(self):
        try:
            self.stop_consuming()
            if self.channel and self.channel.is_open:
                self.channel.close()

            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))