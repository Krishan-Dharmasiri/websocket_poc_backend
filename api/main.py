from fastapi import FastAPI, WebSocket
import time
import json
import queue
from threading import Thread


from utilities.rabbitmq_manager import RabbitMQManager
from utilities.message_handler import message_handler

app = FastAPI()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    '''
    Endpoint to establish the web socket connection
    '''
    print('Accepting Connection ...')
    await websocket.accept()
    print('Accepted the connection succeessfully ...')
    
    while True:
        try:
            message_ui = await websocket.receive_text()
            print(f'Message from the client app : {message_ui}')
            print('Initial Handshake successfull ...')

            

            # Manage the RabbitMQ
            queue_manager = RabbitMQManager('localhost', 'websocket_queue')
            # queue_manager.consume_messages()

            thread_1 = Thread(target=queue_manager.consume_messages)
            thread_1.start()

            # Send data to the Client UI
            print('Sending data to the client ...')
            while True:
                try:
                    message = message_handler.messages.get(timeout=1)
                    await websocket.send_json(message)
                    # print("Received message:", message)
                except queue.Empty:
                    # No message received yet
                    pass
                      
            
            
            print('data sent to the client scucessfully  ...')
            thread_1.join()

        except Exception as error:
            print('Error occured...')
            print(f'The error is : {error}')
            queue_manager.close_connection()
            break


          