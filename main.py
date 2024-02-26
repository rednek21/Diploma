import os

from DataStructures import PostgreSQL, Redis
from dotenv import load_dotenv

load_dotenv()

env = {
    'POSTGRES_USER': os.getenv('POSTGRES_USER'),
    'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD'),
    'POSTGRES_DB': os.getenv('POSTGRES_DB'),
    'POSTGRES_HOST': os.getenv('POSTGRES_HOST'),
    'POSTGRES_PORT': os.getenv('POSTGRES_PORT'),

    'REDIS_HOST': os.getenv('REDIS_HOST'),
    'REDIS_PORT': os.getenv('REDIS_PORT'),
}

postgres_conn = PostgreSQL(dbname=env.get('POSTGRES_DB'),
                               user=env.get('POSTGRES_USER'),
                               password=env.get('POSTGRES_PASSWORD'),
                               host=env.get('POSTGRES_HOST'),
                               port=env.get('POSTGRES_PORT')
                               )
redis_conn = Redis(host=env.get('REDIS_HOST'),
                   port=env.get('REDIS_PORT'),
                   db=1)

# Подключаемся к базам данных
postgres_conn.connect()
postgres_conn.create_table()

redis_conn.connect()

# Выполняем операции с данными
postgres_conn.set_data(7, 10)
redis_conn.set_data(7, 10)

# Получаем данные
data = postgres_conn.get_data(7)
print("Data from PostgreSQL:", data)

data = redis_conn.get_data(7)
print("Data from Redis: 7 :", data)

# Закрываем соединения
postgres_conn.close()
redis_conn.close()
