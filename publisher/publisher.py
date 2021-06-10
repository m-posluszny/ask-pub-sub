from confluent_kafka import Producer
from time import sleep
from os import environ

class PublisherService:

    def __init__(self, topic, hostname):
        self.topic = topic
        self.conf = {'bootstrap.servers': hostname}
        self.producer = Producer(self.conf)
    
    def delivery_stats(self, err, msg):
        if err is not None:
            print(f'Delivery failed:{err}')
        else:
            print(f'Message delivered to {msg.topic()}') 

    def publishing_loop(self, interval):
        try:
            i = 0
            while True:
                self.producer.poll(0)
                self.producer.produce(self.topic, str(i).encode('utf-8'), callback = self.delivery_stats)
                sleep(interval)
                i += 1
                if i > 1000000:
                    i = 0 
        except:
            pass
        self.producer.flush()

def main():
    service = PublisherService(environ.get(
        "TOPIC"),  f'{environ.get("BROKER_IP")}:{environ.get("BROKER_PORT")}')
    service.publishing_loop(2)

if __name__ == "__main__":
    main()
