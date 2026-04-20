import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.query_id  = uuid.uuid4().hex
    
    def serialize_data_message(self, message):
        fruit, amount = message
        return message_protocol.internal.serialize({
            'query_id': self.query_id,
            'data': [fruit, amount]
        })

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize({
            'query_id': self.query_id,
            'data': []
        })

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        if fields['query_id'] != self.query_id:
            return None
        
        return fields["data"]
