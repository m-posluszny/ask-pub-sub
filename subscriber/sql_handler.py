import mysql.connector

#Klasa obsługująca baze danych
class MySqlDB:

    def __init__(self, ip, port, user, password):
        self.conn = None
        self.cursor = self.connect(ip, port, user, password) #inicjalizacjia kursora bazy


    def connect(self, ip, port, user, password):
        #podaje dane do połączenia z bazą danych
        self.conn = mysql.connector.connect(
            host=ip,
            user=user,
            passwd=password,
            #wymagane było ustawienie tej metody autentykacji
            #aby uzyskać kompatybilność z bazą danych w kontenerze
            auth_plugin='mysql_native_password'
            
        )
        return self.conn.cursor()

    def __del__(self):
        """
        destruktor, w przypadku istniejącego połączeniam zakończ je
        """
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
    def exec_query(self, query, commit=False):
        """
        metoda wykonania kwerendy
        argument opcjonalny commit używany jest w przypadku zapisu do bazy
        wtedy po wykonaniu kwerendy stan bazy danych jest aktualizowany
        dane zwracane są w formie rzędów po wcześniejszym odczytaniu ich z obecnej zawartości kursora
       
        """
        if commit:
            self.cursor.execute(query)
            self.conn.call_timeout = 5
            self.conn.commit()
            return True
        else:
            self.cursor.execute(query)
            ret = []
            for row in self.cursor:
                ret.append([row])
            return ret

    def insert_row(self, table, columns, values):
        """
        generator kwerend dodający wartości z listy values do podanych kolumn w liście columns
        w tabeli podawanej w argumencie table

        """
        #lista przechowująca przeparsowane na format kwerendy wartości
        str_vals = []
        for val in values:
            #sprawdzam typ zmiennej, w przypadku stringa dodaje cudzysłowy aby sql traktował to jako stringa
            if isinstance(val, (str)):
                str_vals.append(f"'{val}'")
            #w przypadku liczb, dodaje czystą wartość
            elif isinstance(val, (int, float)):
                str_vals.append(f"{val}")
        #generuje kwerende 
        query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(str_vals)})"
        #wykonuje kwerende z ustawioną opcją commita
        self.exec_query(query, True)

