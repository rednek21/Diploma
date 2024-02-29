import psycopg2
import redis


class PostgreSQL:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None

    def create_table(self):
        cursor = self.connection.cursor()
        cursor.execute(f'CREATE TABLE {self.dbname} (key NUMERIC PRIMARY KEY, value NUMERIC);')
        self.connection.commit()
        cursor.close()

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print("\nConnected to PostgreSQL database!")
        except psycopg2.Error as e:
            print("Unable to connect to database:", e)

    def close(self):
        if self.connection:
            self.connection.close()
            print("\nConnection to PostgreSQL closed.")

    def execute_query(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()

    def set_data(self, key, value):
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {self.dbname} (key, value) VALUES ({key}, {value})")
        self.connection.commit()
        cursor.close()

    def get_data(self, key):
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT key, value FROM {self.dbname} WHERE key = {key};")
        data = cursor.fetchone()
        self.connection.commit()
        cursor.close()
        if data:
            key_value = tuple(str(value) for value in data)
            data = {
                    'key': key_value[0],
                    'value': key_value[1]
                    }
            return data
        else:
            return None


class Redis:
    def __init__(self, host, port, db):
        self.host = host
        self.port = port
        self.db = db
        self.connection = None

    def connect(self):
        try:
            self.connection = redis.Redis(
                host=self.host,
                port=self.port,
                decode_responses=True
            )
            print("Connected to Redis server!")
        except redis.ConnectionError as e:
            print("Unable to connect to Redis server:", e)

    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection to Redis closed.")

    def set_data(self, key, value):
        self.connection.set(key, value)

    def get_data(self, key):
        return self.connection.get(key)