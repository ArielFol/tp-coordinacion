import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.top_by_query = {}
        self.eofs_by_query = {}

    def _process_data(self, query_id, fruit, amount):
        logging.info("Processing data message")

        if query_id not in self.top_by_query:
            self.top_by_query[query_id] = []

        fruit_top = self.top_by_query[query_id]

        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated = fruit_top.pop(i) + fruit_item.FruitItem(fruit, amount)
                bisect.insort(fruit_top, updated)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, query_id, sender_sum_id):
        logging.info(f"Received EOF for query {query_id}")
        
        if query_id not in self.eofs_by_query:
            self.eofs_by_query[query_id] = set()
        
        self.eofs_by_query[query_id].add(sender_sum_id)
        eof_count = len(self.eofs_by_query[query_id])

        if eof_count < SUM_AMOUNT:
            return
        
        fruit_top = self.top_by_query.get(query_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        partial_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize({
            'query_id': query_id,
            'sender_aggregation_id': ID,
            'data': partial_top
        }))
        self.top_by_query.pop(query_id, None)
        self.eofs_by_query.pop(query_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            query_id = deserialized_message["query_id"]
            data = deserialized_message["data"]

            if len(data) == 2:
                self._process_data(query_id, *data)
            else:
                sender_sum_id = deserialized_message.get("sender_sum_id")
                self._process_eof(query_id, sender_sum_id)
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
            nack()
    
    def shutdown(self):
        logging.info("Shutting down aggregation filter")

        try:
            self.input_exchange.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping input exchange consuming: {str(e)}")

        try:
            self.input_exchange.close()
        except Exception as e:
            logging.error(f"Error closing input exchange: {str(e)}")

        try:
            self.output_queue.close()
        except Exception as e:
            logging.error(f"Error closing output queue: {str(e)}")

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: aggregation_filter.shutdown()
    )

    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
