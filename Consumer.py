import threading, logging, time
import os

from kafka import KafkaConsumer



class Consumer:
    
    def run(self):
        topics = 'ctm-transaction-topic'
        group_id = 'prodcon_contest'
        servers = os.environ['SERVER'].split(',')

        print( "Consuming Kafka messages on \ngroup id: " + str(group_id) + "\n topic: " + str(topics) + "\n servers: " + str(servers) )

        consumer = KafkaConsumer(
            topics,
            group_id=group_id,
            bootstrap_servers=servers,
            )

        consumer.seek_to_beginning()

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