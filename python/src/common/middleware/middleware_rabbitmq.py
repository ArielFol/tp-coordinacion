import threading

import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self.consume_connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.consume_channel = self.consume_connection.channel()

            self.publish_connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.publish_channel = self.publish_connection.channel()

            self.queue_name = queue_name

            self.consume_channel.queue_declare(queue=queue_name, durable=True)

            self.publish_channel.queue_declare(queue=queue_name, durable=True)

            self.consuming = False
            
            self.publish_lock = threading.Lock()
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

            self.consume_channel.basic_consume(queue = self.queue_name, on_message_callback = callback, auto_ack = False)
            self.consume_channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            self.consuming = False
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            self.consuming = False
            raise MessageMiddlewareMessageError(str(e))
    
    def stop_consuming(self):
        try:
            if self.consuming:
                self.consume_connection.add_callback_threadsafe(
                self.consume_channel.stop_consuming
                )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        
    def send(self, message):
        try:
            with self.publish_lock:
                self.publish_channel.basic_publish(exchange = '', routing_key = self.queue_name, body = message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))
        
    def close(self):
        try:
            self.stop_consuming()
            if self.consume_channel and self.consume_channel.is_open:
                self.consume_channel.close()

            if self.publish_channel and self.publish_channel.is_open:
                self.publish_channel.close()

            if self.consume_connection and self.consume_connection.is_open:
                self.consume_connection.close()

            if self.publish_connection and self.publish_connection.is_open:
                self.publish_connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.consume_connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.consume_channel = self.consume_connection.channel()

            self.publish_connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.publish_channel = self.publish_connection.channel()

            self.publish_lock = threading.Lock()
            self.exchange_name = exchange_name
            self.routing_keys = routing_keys
            self.consume_channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

            self.publish_channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

            result = self.consume_channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue

            for key in routing_keys:
                self.consume_channel.queue_bind(exchange=exchange_name, queue=self.queue_name, routing_key=key)

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
                
            self.consume_channel.basic_consume(queue = self.queue_name, on_message_callback = callback, auto_ack=False)
            self.consume_channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            self.consuming = False
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            self.consuming = False
            raise MessageMiddlewareMessageError(str(e))
    
    def stop_consuming(self):
        try:
            if self.consuming:
                self.consume_connection.add_callback_threadsafe(
                self.consume_channel.stop_consuming
                )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        
    def send(self, message):
        try:
            with self.publish_lock:
                for key in self.routing_keys:
                    self.publish_channel.basic_publish(exchange = self.exchange_name, routing_key = key, body = message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))
        
    def close(self):
        try:
            self.stop_consuming()
            if self.consume_channel and self.consume_channel.is_open:
                self.consume_channel.close()
                
            if self.publish_channel and self.publish_channel.is_open:
                self.publish_channel.close()

            if self.consume_connection and self.consume_connection.is_open:
                self.consume_connection.close()

            if self.publish_connection and self.publish_connection.is_open:
                self.publish_connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))