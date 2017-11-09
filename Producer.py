import threading, logging, time

from kafka import KafkaProducer


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers=os.environ['SERVER'].split(','))

        while not self.stop_event.is_set():
            producer.send('my-topic', b"Hello World!")
            time.sleep(1)

        producer.close()


def main():
    tasks = [
        Producer()
    ]

    for t in tasks:
        t.start()

    time.sleep(20)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()