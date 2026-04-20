import os
import logging
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
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_query = {}

    def _process_data(self, query_id, fruit, amount):
        logging.info(f"Process data")

        if query_id not in self.amount_by_query:
            self.amount_by_query[query_id] = {}
        
        query_fruits = self.amount_by_query[query_id]

        query_fruits[fruit] = query_fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, query_id):
        logging.info(f"Broadcasting data messages")

        query_fruits = self.amount_by_query.get(query_id, {})

        for final_fruit_item in query_fruits.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize({
                        'query_id': query_id,
                        'data': [final_fruit_item.fruit, final_fruit_item.amount]
                    })
                )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize({
                'query_id': query_id,
                'data': []
            }))
        del self.amount_by_query[query_id]


    def process_data_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)

        query_id = deserialized_message["query_id"]
        data = deserialized_message["data"]
        if len(data) == 2:
            self._process_data(query_id,*data)
        else:
            self._process_eof(query_id, *data)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
