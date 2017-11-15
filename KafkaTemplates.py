import threading, logging, time
import os
import datetime

from kafka import KafkaProducer, KafkaConsumer, TopicPartition


def strf_consumer_record(record):
    return_string =  ("Received message: partition: {} | offset: {} | timestamp: {} | key: {} | value: {} ".format(
                record.partition, 
                record.offset, 
                datetime.datetime.fromtimestamp(record.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S.') + str(record.timestamp%1000),
                record.key, 
                record.value))
    return return_string


class Producer(object):
    # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html

    def run(self):
        # Run a continously producing KafkaProducer
        # 
        # producer = Producer()
        # producer.run()


        # setting up server variables
        topic = 'ctm-transactions-topic' #replace with topic name
        client_id = ''        
        servers = os.environ['SERVER'].split(',') #Kafka broker server from system environment variable $SERVER

        # creating prodcuer instance
        print('Starting Producer on: \n Server: {}\n Client ID: {}\n Topic: {}'.format(str(servers), str(client_id), str(topic)))
        
        producer = KafkaProducer(bootstrap_servers=servers,
                                client_id=client_id)

        counter = 1
        while True:
            # message key and value type must be a byte
            msg_key = b'helloworld'
            msg_value = b'Hello World!!'

            #produce message to topic
            producer.send(topic, key=msg_key, value=msg_value,)

            print('{} Produced message, key: {}, value: {}'.format(counter, msg_key, msg_value ))
            counter += 1
            # check for success?
            time.sleep(1)

        print("Closing Kafka producer")
        producer.close()


class Consumer(object):
    # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
    # note: consumer can't be multithreaded

    def run_continuous(self):
        # Run a continously consuming KafkaConsumer
        # can be run from start of topic or only consumer latest message
        # consumer = Consumer()
        # consumer.run_continuous()


        # setting up server variables
        topic = 'ctm-transactions-topic'
        group_id = None       
        servers = os.environ['SERVER'].split(',') #Kafka broker server from system environment variable $SERVER

        # creating prodcuer instance
        print('Starting Consumer on: \n Server: {}\n Group ID: {}\n Topic: {}'.format(str(servers), str(group_id), str(topic)))

        consumer = KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=servers,
            auto_offset_reset='earliest' #this reads from start off queue, set to 'latest' for only receiving new records
            )

        #topic can be assigned here or in object creation above
        consumer.subscribe(topic)

        for record in consumer:
            #continuously runs and waits for new record, code in here will run on each record received

            print(strf_consumer_record(record))

            # process record
            
        print("Closing Kafka consumer")
        consumer.close()


class Conversions(object):
    def bytestring_to_json(self, bytestring):
        data = json.loads(bytestring.value.decode('utf-8'))
        return data

    def json_to_bytestring(self, json_data):
        byte_string = json.dumps(json_data).encode('utf-8')
        return byte_string




def main():
    consumer = Consumer()
    consumer.run_continuous()

    #prod = Producer()
    #prod.run()


if __name__ == "__main__":
    main()





