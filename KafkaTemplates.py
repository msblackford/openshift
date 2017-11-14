import threading, logging, time
import os
import datetime

from kafka import KafkaProducer, KafkaConsumer, TopicPartition


def print_kafka_record(record):
    return ("Received message: partition: {} | offset: {} | timestamp: {} | key: {} | value: {} ".format(
                record.partition, 
                record.offset, 
                datetime.datetime.fromtimestamp(record.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S.') + str(record.timestamp%1000),
                record.key, 
                record.value))


class Producer(threading.Thread):
    # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html

    def run(self):
        # Run a continously producing KafkaProducer
        # 
        # producer = Producer()
        # producer.run()


        # setting up server variables
        topic = 'ctm-transactions-topic' #replace with topic name
        client_id = ''        
        servers = os.environ['SERVER'].split(',') #Kafka broker server system environment variable $SERVER

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


class Consumer:
    # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
    # note: consumer can't be multithreaded

    def run_continuous(self):
        # Run a continously consuming KafkaConsumer
        # can be run from start of topic or only consumer latest message
        # consumer = Consumer()
        # producer.run_continuous()


        # setting up server variables
        topic = 'ctm-transactions-topic'
        group_id = None       
        servers = os.environ['SERVER'].split(',') #Kafka broker server system environment variable $SERVER

        # creating prodcuer instance
        print('Starting Producer on: \n Server: {}\n Group ID: {}\n Topic: {}'.format(str(servers), str(group_id), str(topic)))

        consumer = KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=servers,
            auto_offset_reset='earliest' #this reads from start off queue, set to 'latest' for only receiving new records
            )

        #topic can be assigned here or in object creation above
        consumer.subscribe(topic)

        for record in consumer:
            #continuously runs and waits for new record, code in here will run on each record received

            print(print_kafka_record(record))

            # process record
            
        print("Closing Kafka consumer")
        consumer.close()

    def run_seek_by_offset(self):
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
        print('Starting Producer on: \n Server: {}\n Group ID: {}\n Topic: {}'.format(str(servers), str(group_id), str(topic)))

        # if seeking, don't assign topic or auto_offset_reset here
        consumer = KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=servers
            )


        topicPar = TopicPartition(topic, partition)

        consumer.assign([topicPar])
        current_pos = consumer.position(topicPar)
        new_pos = current_pos - 5
        consumer.seek(topicPar, new_pos ) # consumer starting at offset from end of queue

        for record in consumer:
            #continuously runs and waits for new record, code in here will run on each record received

            print(print_kafka_record(record))

            # process record
            
        print("Closing Kafka consumer")
        consumer.close()


    def run_seek_by_timestamp(self):
        # run a consumer that consumer records between timestamps
        # 
        # consumer = Consumer()
        # producer.run_seek_by_timestamp()

        starting_timestamp = 1510607303965              # timestamp in milliseconds since epoch
        ending_timestamp = starting_timestamp + 2000    # starting plus offset in milliseconds

        # setting up server variables
        topic = 'ctm-transactions-topic'
        partition = 0
        group_id = None       
        servers = os.environ['SERVER'].split(',') #Kafka broker server system environment variable $SERVER

        # creating prodcuer instance
        print('Starting Producer on: \n Server: {}\n Group ID: {}\n Topic: {}'.format(str(servers), str(group_id), str(topic)))

        # if seeking, don't assign topic or auto_offset_reset here
        consumer = KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=servers
            )


        topicPar = TopicPartition(topic, partition)
        consumer.assign([topicPar])

        pos_at_time = consumer.offsets_for_times( { topicPar : starting_timestamp } )
        print(pos_at_time)
        print("Offset: {}, Timestampe: {}".format(pos_at_time[topicPar].offset, pos_at_time[topicPar].timestamp))
        
        consumer.seek(topicPar, pos_at_time[topicPar].offset ) # consumer starting at offset from end of queue

        records = []
        for record in consumer:
            #consume records until final timestamp
            if (record.timestamp > ending_timestamp):
                break
            records.append(record)
        
        for r in records:
            print(print_kafka_record(r))

            
        print("Closing Kafka consumer")
        consumer.close()



def main():
    consumer = Consumer()
    consumer.run_continuous()

    #prod = Producer()
    #prod.run()


if __name__ == "__main__":
    main()





