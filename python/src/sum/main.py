import os
import logging
import signal
import sys
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{ID}"]
        )

        self.control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)]
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.control_thread = None

        self.amount_by_query = {}
        self.closed_queries = set()
        self.notified_queries = set()
        self.lock = threading.Lock()

    def _process_data(self, query_id, fruit, amount):
        logging.info(f"Process data")

        with self.lock:
            if query_id not in self.amount_by_query:
                self.amount_by_query[query_id] = {}
            
            query_fruits = self.amount_by_query[query_id]

            query_fruits[fruit] = query_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
        
        self._try_close_query(query_id)

    def _partition_key(self, fruit):
        value = 0
        for char in fruit:
            value = value * 31 + ord(char)
        return value % AGGREGATION_AMOUNT

    def _process_eof(self, query_id):
        logging.info(f"Broadcasting data messages")

        with self.lock:
            if query_id in self.notified_queries:
                return
            self.notified_queries.add(query_id)
            query_fruits = self.amount_by_query.pop(query_id, None)

        if query_fruits is not None:
            for final_fruit_item in query_fruits.values():
                aggregation_target = self._partition_key(final_fruit_item.fruit)
                target_exchange = self.data_output_exchanges[aggregation_target]
                target_exchange.send(
                    message_protocol.internal.serialize({
                        'query_id': query_id,
                        'data': [final_fruit_item.fruit, final_fruit_item.amount]
                    })
                )
        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize({
                'query_id': query_id,
                'sender_sum_id': ID,
                'data': []
            }))
        
        with self.lock:
            self.closed_queries.discard(query_id)
            self.notified_queries.discard(query_id)

    def _broadcast_eof_to_sums(self, query_id):
        logging.info(f"Broadcasting EOF to sums for query {query_id}")

        self.control_output.send(message_protocol.internal.serialize({
            'query_id': query_id,
            'data': []
        }))
    
    def _receive_eof(self, query_id):
        logging.info(f"Received EOF for query {query_id}")

        with self.lock:

            if query_id in self.closed_queries:
                return
        
            self.closed_queries.add(query_id)
        
        self._try_close_query(query_id)

    def _try_close_query(self, query_id):
        with self.lock:
            if query_id not in self.closed_queries:
                return
        self._process_eof(query_id)

    def process_control_message(self, message, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(message)
            self._receive_eof(msg["query_id"])
            ack()
        except Exception as e:
            logging.error(f"Error processing control message: {str(e)}")
            nack()

    def process_data_message(self, message, ack, nack):
        try:
            deserialized_message = message_protocol.internal.deserialize(message)

            query_id = deserialized_message["query_id"]
            data = deserialized_message["data"]
            if len(data) == 2:
                self._process_data(query_id,*data)
            else:
                self._broadcast_eof_to_sums(query_id)
                self._receive_eof(query_id)
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
            nack()

    def shutdown(self):
        logging.info("SIGTERM received, shutting down")
        
        try:
            self.input_queue.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping input queue consuming: {str(e)}")
        
        try:
            self.control_exchange.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping control exchange consuming: {str(e)}")

        if self.control_thread and self.control_thread.is_alive():
            self.control_thread.join(timeout=2)

        try:
            self.input_queue.close()
        except Exception as e:
            logging.error(f"Error closing input queue: {str(e)}")

        try:
            self.control_exchange.close()
        except Exception as e:
                logging.error(f"Error closing control exchange: {str(e)}")

        try:
            self.control_output.close()
        except Exception as e:
            logging.error(f"Error closing control output: {str(e)}")

        for exchange in self.data_output_exchanges:
            try:
                exchange.close()
            except Exception as e:
                logging.error(f"Error closing data output exchange: {str(e)}")


    def start(self):
        self.control_thread = threading.Thread(
            target = lambda: self.control_exchange.start_consuming(self.process_control_message),
            daemon = True
        )

        self.control_thread.start()
        self.input_queue.start_consuming(self.process_data_message)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: sum_filter.shutdown())
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
