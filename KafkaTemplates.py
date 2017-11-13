import threading, logging, time
import os

from kafka import KafkaProducer, KafkaConsumer


class Producer(threading.Thread):
    # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html

    def run(self):
        # Run a continously producing KafkaProducer
        #
        # producer = Producer()
        # producer.run()


        # setting up server variables
        topic = 'test'      # 'ctm-transactions-topic'
        client_id = ''        
        servers = os.environ['SERVER'].split(',') #Kafka broker server system environment variable $SERVER

        # creating prodcuer instance
        print('Starting Producer on: \n Server: {}\n Client ID: {}\n Topic: {}'.format(str(servers), str(client_id), str(topic)))
        producer = KafkaProducer(bootstrap_servers=servers,
            client_id=client_id)
        print('Connected')

        counter = 1
        while True:
            # message key and value type must be a byte
            msg_key = b'helloworld'
            msg_value = b'Hello World!!'

            #produce message to topic
            producer.send(topic, value=msg_value, key=msg_key)

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
        topic = 'test'      # 'ctm-transactions-topic'
        group_id = None       
        servers = os.environ['SERVER'].split(',') #Kafka broker server system environment variable $SERVER

        # creating prodcuer instance
        print('Starting Producer on: \n Server: {}\n Group ID: {}\n Topic: {}'.format(str(servers), str(group_id), str(topic)))

        consumer = KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=servers,
            auto_offset_reset='earliest' #this reads from start off queue, set to 'latest' for only receiving new messages
            )

        #topic can be assigned here or in object creation above
        consumer.subscribe(topic)

        counter = 1
        for message in consumer:
            #continuously runs and waits for new messages, code in here will run on each message received

            print("{} Received message, key: {} | value: {}".format(counter, message.key, message.value))

            # process message

            counter = counter + 1
            
        print("Closing Kafka consumer")
        consumer.close()


    def find_key(self):
        print("find by key")



def main():
    consumer = Consumer()
    consumer.run_continuous()


if __name__ == "__main__":
    main()





