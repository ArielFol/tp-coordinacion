import uuid
import logging

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.query_id  = uuid.uuid4().hex
        self.records_by_fruit = {}
    
    def serialize_data_message(self, message):
        logging.info(f"Serializing data message for query {self.query_id}, and fruit {message[0]} with amount {message[1]}")
        fruit, amount = message
        self.records_by_fruit[fruit] = self.records_by_fruit.get(fruit, 0) + 1
        return message_protocol.internal.serialize({
            'query_id': self.query_id,
            'data': [fruit, amount]
        })

    def serialize_eof_message(self, message):
        logging.info(f"Serializing EOF message for query {self.query_id}")
        return message_protocol.internal.serialize({
            'query_id': self.query_id,
            'records_by_fruit': self.records_by_fruit,
            'data': []
        })

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        if fields['query_id'] != self.query_id:
            return None
        
        return fields["data"]
