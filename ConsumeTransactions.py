import threading, logging, time
import os
from kafka import KafkaConsumer
from kafka import TopicPartition

class Consumer:
    
    def run(self):
        topic = 'my-topic' # 'ctm-transactions-topic'
        group_id = ''
        #servers = os.environ['SERVER'].split(',')
        servers = '192.168.88.129:9092'

        print "Consuming Kafka messages on \n group id: " + str(group_id) + "\n topic: " + str(topic) + "\n servers: " + str(servers) 

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=servers,
            auto_offset_reset='earliest' #this reads from start off queue
            )


        counter = 1
        for message in consumer:
            print str(counter) + " Received message " + str(message.value) + " on topic '" + str(message.topic) + "'"
            counter = counter + 1
            
        print "Closing Kafka consumer" 
        consumer.close()
        
        
def main():
    con = Consumer()
    con.run()

        
        
if __name__ == "__main__":
    main()