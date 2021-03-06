from confluent_kafka import Consumer, TIMESTAMP_NOT_AVAILABLE
from sql_handler import MySqlDB
from os import environ
import datetime
from time import time

class SubscriberService:

    def __init__(self, topic, hostname, sql_ip, sql_port, sql_user, sql_pwd, db_timeout=300):
        """
        Konstruktor w pierwszym kroku oczekuje na połączenie z bazą danych, dzieje się tak, ponieważ
        serwer sql uruchamia się wolniej niż aplikacja subskrybującego
        Następnie przygotowuje konfigurację do połączenia z brokerem,
        zawarte w niej jest adres serwera, id grupy, oraz ustawienie co do automatycznego
        resetowania offsetu partycji przy utworzeniu grupy
        config używany jest w inicjalizacji obiektu Consumer'a, po czym
        definiuję stan pętli głównej na True, oraz inicjalizuje countery
        używane do zliczania statystyk
        """
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
        """
        Funkcja porównuje czas pod koniec iteracji z czasem początkowym,
        jeśli jest mniejszy niż okreslony timeout powtarzana jest operacja
        połączenia i wyboru bazy danych. W przypadku przekroczenia timeoutu
        wywoływany jest błąd timeoutu
        """
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

    def receive_stats(self, err):
        """
        funkcja wyświetlając zawartość błędu z wiadomości
        """
        print(f'Delivery failed:{err}')

    def write_to_db(self, timestamp, mean):
        """
        funkcja zapisu do bazy danych, przed zapisem odpowiednio formatuję
        timestamp, w tym celu dzielę timestamp z wiadomości przez 1000 aby
        dopsaować się do dokładności datetime'a po czym konwertuję go na stringa,
        następnie data jak i uzyskana średnia dodawane są do tablicy
        """
        ts = datetime.datetime.fromtimestamp(
            timestamp//1000).strftime('%Y-%m-%d %H:%M:%S')
        self.db.insert_row("mean_statistics", 
                           ["msg_timestamp", "mean"],
                           [ts, mean])

    def process_msg(self, msg):
        """
        Obsługa wiadomości, sprawdzamy czy timestamp jest dostępny
        poprzez porównanie kodu timestampu, jeśli istnieje
        to dekodujemy zawartość wiadomości do integera i pobieramy
        timestamp z drugiego elementu tablicy msg.timestamp()
        zwiększamy counter o 1, dodajemy wartość do sum i w przypadku
        gdy counter osiągnie 10, obliczana jest średnia, wywoływana jest metoda
        write_to_db a potem zeruje się counter i sum
        """
        ts = msg.timestamp()
        if ts[0] == TIMESTAMP_NOT_AVAILABLE:
            return False
        value = int(msg.value().decode("utf-8"))
        timestamp = ts[1]
        self.counter += 1 
        self.sum += value
        if self.counter == 10:
            mean = self.sum/self.counter
            self.write_to_db(timestamp, mean)
            self.counter = 0
            self.sum = 0

    def subscribing_loop(self, topics):
        """
        główna metoda klasy, po przekazaniu listy tematów,
        konsumer subskrybuje się do nich po czym
        uruchamiana jest pętla odbierania wiadomości.
        W momencie w któym wiadomość posiada błąd, wyświetlane są
        szczegóły błędu, w przypadku prawidłowiej wiadomości
        uruchamiane jest przetwarzanie wiadomości
        """
        try:
            self.consumer.subscribe(topics)

            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.receive_stats(msg.error())
                else:
                    self.process_msg(msg)
        finally:
            self.consumer.close()



if __name__ == "__main__":
    #obiekt jest inicjalizowany używając za argumenty odpowiednie zmienne środowiskowe
    service = SubscriberService(environ.get("TOPIC"), f'{environ.get("BROKER_IP")}:{environ.get("BROKER_PORT")}',
                                environ.get("DB_IP"), environ.get("DB_PORT"),
                                environ.get("DB_LOGIN"), environ.get("DB_PWD"))
    service.subscribing_loop([environ.get("TOPIC")])
