import mysql.connector


class MySqlDB:

    def __init__(self, ip, port, user, password):
        self.cursor = None
        self.cursor = self.connect(ip, port, user, password)

    def connect(self, ip, port, user, password):
        self.conn = mysql.connector.connect(
            host=ip,
            user=user,
            passwd=password,
            auth_plugin='mysql_native_password'
            
        )
        return self.conn.cursor()

    def __del__(self):
        if self.cursor:
            try:
                self.conn.close()
            except:
                pass

    def exec_query(self, query, commit=False):
        if commit:
            self.cursor.execute(query)
            self.conn.commit()
            return True
        else:
            self.cursor.execute(query)
            ret = []
            for row in self.cursor:
                ret.append([row])
            return ret

    def insert_row(self, table, columns, values):
        str_vals = []
        for val in values:
            if isinstance(val, (str)):
                str_vals.append(f"'{val}'")
            elif isinstance(val, (int, float)):
                str_vals.append(f"{val}")
        query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(str_vals)})"
        print(query)
        self.exec_query(query, True)
        self.conn.call_timeout = 5
        self.conn.commit()

