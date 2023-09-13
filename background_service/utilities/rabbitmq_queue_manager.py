import pika
import json


class RabbitMQManager:
    def __init__(self, host : str, queue : str):
        self.host = host
        self.queue = queue
        self.connection = None
        self.channel = None

        # Establish a connection to RabbitMQ
        connection_parameters = pika.ConnectionParameters(self.host)
        self.connection = pika.BlockingConnection(connection_parameters)
        self.channel = self.connection.channel()

        # Declare a queue you want to send messages to (if does not exist)
        self.channel.queue_declare(queue=self.queue)

    def push_to_queue(self, message):
        try:
            print('Pushing message to the queue ...')    
            # Publish the message to the Queue
            json_message = json.dumps(message)
            # exchange parameter is empty because we are using the default exchange
            self.channel.basic_publish(exchange='', routing_key=self.queue, body=json_message)
            print('Message pushed to the queue successfully')

        except Exception as error:
            print(f'Error occured while pushing message to the queue, Error : {error}')

    def close_connection(self):
        try:
            print('Closing the connection ...')
            self.connection.close()
            print('Connection closed successfully')

        except Exception as error:
            print(f'Error occured while closing the connection, Error : {error}')    