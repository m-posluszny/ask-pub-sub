from confluent_kafka import Producer
from time import sleep
from os import environ

class PublisherService:

    def __init__(self, topic, hostname):
        """
        Konstruktor tworzy konfigurację Producer'a z adresem brokera,
        po czym inicjalizuje obiekt Producera
        """
        self.topic = topic
        self.conf = {'bootstrap.servers': hostname}
        self.producer = Producer(self.conf)
    
    def delivery_stats(self, err, msg):
        """
        Status wiadomości, wyświetla istniejące błędy
        bądź informację do jakiego tematu wiadomość została dostarczona
        """
        if err is not None:
            print(f'Delivery failed:{err}')
        else:
            print(f'Message delivered to {msg.topic()}') 

    def publishing_loop(self, interval):
        """
        Pętla publikacji wiadomości,
        początkowy licznik i ustawiony na 0, w 
        przypadku przekroczenia 1000000 licznik zaczyna od zera.
        wartość licznika jest parsowana do stringa po czym enkodowana i wysyłana
        na temat metodą produce Producera. Następnie program odczekuje
        ustawioną wartość czasu z argumentu interval
        Na koniec wykonywana jest operacja flush
        """
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
    """
    Inicjalizuje obiekt klasy używając za argumenty zmiennych globalnych
    """
    service = PublisherService(environ.get(
        "TOPIC"),  f'{environ.get("BROKER_IP")}:{environ.get("BROKER_PORT")}')
    #ustawiam 2 sekundowy interwał
    service.publishing_loop(2)


if __name__ == "__main__":
    main()
