from confluent_kafka import Consumer, TIMESTAMP_NOT_AVAILABLE
from sql_handler import MySqlDB
from os import environ
import datetime
from time import time

class SubscriberService:

    def __init__(self, topic, hostname, sql_ip, sql_port, sql_user, sql_pwd, db_timeout=300):
        self.wait_for_db(sql_ip, sql_port, sql_user, sql_pwd, db_timeout)
        self.topic = topic
        self.conf = {
            'bootstrap.servers': hostname,
            'group.id': 'statistic_group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.conf)
        self.running = True

        self.counter = 0
        self.sum = 0

    def wait_for_db(self, sql_ip, sql_port, sql_user, sql_pwd, db_timeout):
        dt = 0
        st = time()
        while (dt < db_timeout):
            try:
                self.db = MySqlDB(sql_ip, sql_port, sql_user, sql_pwd)
                self.db.exec_query("use clientstat")
                return
            except:
                dt = time() - st 
        raise TimeoutError
            
    def shutdown(self):
        self.running = False
    
    def receive_stats(self, err, msg):
        if err is not None:
            print(f'Delivery failed:{err}')
        else:
            print(f'Message delivered to {msg.topic()}')

    def write_to_db(self, timestamp, mean):
        ts = datetime.datetime.fromtimestamp(
            timestamp//1000).strftime('%Y-%m-%d %H:%M:%S')
        self.db.insert_row("mean_statistics", 
                           ["msg_timestamp", "mean"],
                           [ts, mean])

    def process_msg(self, msg):
        ts = msg.timestamp()
        if ts[0] == TIMESTAMP_NOT_AVAILABLE:
            return False
        value = int(msg.value().decode("utf-8"))
        timestamp = ts[1]
        print(value, msg.value())
        self.counter += 1 
        self.sum += value
        if self.counter == 10:
            mean = self.sum/self.counter
            self.write_to_db(timestamp, mean)
            self.counter = 0
            self.sum = 0

    def subscribing_loop(self, topics):
        try:
            self.consumer.subscribe(topics)

            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                print("hello")
                if msg is None:
                    continue
                print(msg)
                if msg.error():
                    self.receive_stats(msg.error(), msg)
                else:
                    self.process_msg(msg)
        finally:
            self.consumer.close()


def main():
    service = SubscriberService(environ.get("TOPIC"), f'{environ.get("BROKER_IP")}:{environ.get("BROKER_PORT")}',
                                environ.get("DB_IP"), environ.get("DB_PORT"),
                                environ.get("DB_LOGIN"), environ.get("DB_PWD"))
    service.subscribing_loop([environ.get("TOPIC")])


if __name__ == "__main__":
    main()
