import queue
import json

class MessageHandler:
    def __init__(self):
        self.messages = queue.Queue()

    def callback_func(self, ch, method, properties, body):
        self.messages.put(json.loads(body))

message_handler = MessageHandler()