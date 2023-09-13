import pika
import json


from utilities.message_handler import message_handler

class RabbitMQManager:
    def __init__(self, host : str, queue : str):
        self.host = host
        self.queue = queue
        self.cononection = None
        self.channel = None
        self.recieved_message = None
        

        # Establish a connection to RabbitMQ
        
        connection_parameters = pika.ConnectionParameters(self.host)
        self.connection = pika.BlockingConnection(connection_parameters)
        self.channel = self.connection.channel()

        # Declare the Queue you want to consume messages from
        self.channel.queue_declare(queue=self.queue)

        # Ensures that consumer only recieves one message at a time 
        # and processes it before recieving the next one    
        self.channel.basic_qos(prefetch_count=1)

    def consume_messages(self):
        try:
            print('Started consuming messages ...')
            self.channel.basic_consume(queue=self.queue, 
                      on_message_callback=message_handler.callback_func, auto_ack=True)
            
            self.channel.start_consuming()

        except Exception as error:
            print(f'An error occured while consuming messages, Error : {error}') 

    def close_connection(self):
        try:
            print('closing the connection to RabbitMQ ...')
            self.connection.close()
            print('Successfully closed the connection')

        except Exception as error:
            print(f'An error occured while closing the connection, Error : {error}')  


    def callback_function(self,ch, method, properties, body):
        try:
            print('Started processing messages ...')
            
            # print(f'Inside call back function : {body}')

            self.recieved_message = json.loads(body) 
            

            print(f'Class Variable : {self.recieved_message}')
            
            # Acknowledge the message to remove it from the Queue
            # ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as error:
            print(f'An error occured while processing the message. Function : on_message_recieved, Error : {error}')         


     
