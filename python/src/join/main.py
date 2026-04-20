import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.partial_top_by_query = {}

    def _process_partial_top(self, query_id, sender_aggregation_id, partial_top):
        logging.info(f"Processing partial top message for query {query_id} from aggregation {sender_aggregation_id}")

        if query_id not in self.partial_top_by_query:
            self.partial_top_by_query[query_id] = {}

        self.partial_top_by_query[query_id][sender_aggregation_id] = partial_top

        if len(self.partial_top_by_query[query_id]) < AGGREGATION_AMOUNT:
            return
        
        total = []
        for top in self.partial_top_by_query[query_id].values():
            total.extend(top)
        
        final_top = sorted(total, key=lambda x: x[1], reverse=True)[:TOP_SIZE]

        self.output_queue.send(message_protocol.internal.serialize({
            'query_id': query_id,
            'data': final_top
        }))

        del self.partial_top_by_query[query_id]

    def process_message(self, message, ack, nack):
        logging.info("Received top")

        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            query_id = deserialized_message['query_id']
            sender_aggregation_id = deserialized_message['sender_aggregation_id']
            partial_top = deserialized_message['data']

            self._process_partial_top(query_id, sender_aggregation_id, partial_top)
            ack()

        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
            nack()
    
    def shutdown(self):
        logging.info("Shutting down join filter")

        try:
            self.input_queue.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping input queue consuming: {str(e)}")

        try:
            self.input_queue.close()
        except Exception as e:
            logging.error(f"Error closing input queue: {str(e)}")

        try:
            self.output_queue.close()
        except Exception as e:
            logging.error(f"Error closing output queue: {str(e)}")

    def start(self):
        self.input_queue.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: join_filter.shutdown()
    )

    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
