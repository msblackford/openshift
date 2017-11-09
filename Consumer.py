import threading, logging, time
import multiprocessing
import os

from kafka import KafkaConsumer



class Consumer(multiprocessing.Process):
    
    def run(self):
        topics = 'my-topic'
        group_id = 'prodcon_contest'
        servers = os.environ['SERVER'].split(',')

        print( "Consuming Kafka messages on server: " + servers + ", topic: " + topics )

        consumer = KafkaConsumer(
            topics,
            group_id=group_id,
            bootstrap_servers=servers,
            auto_offset_reset='earliest')

        counter = 1
        for message in consumer:
            print(str(counter) + " Received message " + str(message.value) + " on topic '" + str(message.topic) + "'")
            counter = counter + 1
            
        print("Closing Kafka consumer")
        consumer.close()
        
        
def main():
    con = Consumer()
    con.run()

        
        
if __name__ == "__main__":
    main()