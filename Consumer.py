import threading, logging, time
import os

from kafka import KafkaConsumer
from kafka import TopicPartition

class Consumer:
    
    def run(self):
        topic = 'ctm-transaction-topic'
        group_id = 'prodcon_contest_2'
        servers = os.environ['SERVER'].split(',')

        print( "Consuming Kafka messages on \n group id: " + str(group_id) + "\n topic: " + str(topic) + "\n servers: " + str(servers) )

        #partition = TopicPartition(topic, 0)
        #print("Partition: " + str(partition))

        consumer = KafkaConsumer(
            topic,
            group_id=group_id,
            bootstrap_servers=servers,
            auto_offset_reset='earliest'
            )

        #consumer.assign( partition )
        #consumer.seek_to_beginning(partition)
        consumer.subscribe(topic)

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