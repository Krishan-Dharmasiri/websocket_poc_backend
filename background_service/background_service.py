import time
import numpy as np
import random

from utilities.rabbitmq_queue_manager import RabbitMQManager

def main() -> None : 
    '''
    Process data and push them to the queue
    '''
    try:
        print('Background service started ...')
        # Create an instance of the Queue Manager
        queue_manager = RabbitMQManager('localhost', 'websocket_queue')

        # Simulate processing data as a for loop
        for i in range(25):

            # Generate Data
            start_range = 1
            end_range = 100

            data = {
                'model' : 'Regression',
                'result' : random.randint(start_range, end_range)
            }

            # Push message to the queue
            queue_manager.push_to_queue(data)

            time.sleep(1)

        # Close the connection to RabbitMQ
        queue_manager.close_connection()    

    except Exception as error:
        print(f'An error occured while processing data, Error : {error}')


if __name__ == '__main__':
    main()
