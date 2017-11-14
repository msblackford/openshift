import threading, logging, time
import os, datetime
import json

from kafka import KafkaConsumer
from kafka import TopicPartition


def print_kafka_record(record):
    return ("Received message: partition: {} | offset: {} | timestamp: {} | key: {} | value: {} ".format(
                record.partition, 
                record.offset, 
                datetime.datetime.fromtimestamp(record.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S.') + str(record.timestamp%1000),
                record.key, 
                record.value))

class Consumer:
    
    def run(self):
        # Run a continously consuming KafkaConsumer starting from last offset of last record
        # must define partition
        # 
        # consumer = Consumer()
        # producer.run_seek_by_offset()


        # setting up server variables
        topic = 'ctm-transactions-topic'
        partition = 0
        group_id = None       
        servers = os.environ['SERVER'].split(',') #Kafka broker server system environment variable $SERVER

        # creating prodcuer instance
        print('Starting Consumer on: \n Server: {}\n Group ID: {}\n Topic: {}'.format(str(servers), str(group_id), str(topic)))

        # if seeking, don't assign topic or auto_offset_reset here
        consumer = KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=servers
            )


        topic_par = TopicPartition(topic, partition)

        consumer.assign([topic_par])
        current_pos = consumer.position(topic_par)
        new_pos = current_pos - 5
        if (new_pos < 0):
            new_pos = 0
        
        consumer.seek(topic_par, new_pos ) # consumer starting at offset from end of queue

        for record in consumer:
            #continuously runs and waits for new record, code in here will run on each record received

            # print(print_kafka_record(record))
            print("Original Record:")
            print(record.key)
            print(record.value)

            key = record.key.decode('utf-8')
            json_data = json.loads(record.value.decode('utf-8'))

            print("JSON data and transformation")
            print(json.dumps(json_data, indent=4, sort_keys=True)) # print the json prettily  

            print("key: " + key)
            print("accountId: " + json_data['account']['accountId'])
            print("accountCloseDate: " + json_data['account']['accountCloseDate'])

            json_data['account']['accountId'] = "abc123"

            
            new_record_key = record.key
            new_record_value = json.dumps(json_data).encode('utf-8')
            print("Transformed JSON data")
            print(json.dumps(json_data, indent=4, sort_keys=True))

            print('\n')
            

            # process record
            
        print("Closing Kafka consumer")
        consumer.close()
            
        
def main():
    con = Consumer()
    con.run()

        
        
if __name__ == "__main__":
    main()