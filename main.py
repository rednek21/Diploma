import os

from DataStructures import PostgreSQL, Redis
from dotenv import load_dotenv

from common import get_random_data

load_dotenv()

# Чтение переменных окружения
env = {
    'POSTGRES_USER': os.getenv('POSTGRES_USER'),
    'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD'),
    'POSTGRES_DB': os.getenv('POSTGRES_DB'),
    'POSTGRES_HOST': os.getenv('POSTGRES_HOST'),
    'POSTGRES_PORT': os.getenv('POSTGRES_PORT'),

    'REDIS_HOST': os.getenv('REDIS_HOST'),
    'REDIS_PORT': os.getenv('REDIS_PORT'),
}

#
postgres_conn = PostgreSQL(dbname=env.get('POSTGRES_DB'),
                               user=env.get('POSTGRES_USER'),
                               password=env.get('POSTGRES_PASSWORD'),
                               host=env.get('POSTGRES_HOST'),
                               port=env.get('POSTGRES_PORT')
                               )
redis_conn = Redis(host=env.get('REDIS_HOST'),
                   port=env.get('REDIS_PORT'),
                   db=1)

hash_table = {}

# Соединение с БД
postgres_conn.connect()
postgres_conn.create_table()

redis_conn.connect()

# Проверка наличия данных в БД: если данные есть -> вывести, если данных НЕТ -> добавить + вывести
for i in range(10):
    random_data = get_random_data()

    # if not hash_table[f'{random_data["key"]}']:
    #     key = random_data["key"]
    #     value = random_data["value"]
    #     hash_table.update({key : value})
    #     print(hash_table)
    #     print(f'Successful set to hash-table {key} : {value}')

    if not postgres_conn.get_data(random_data["key"]):
        postgres_conn.set_data(random_data["key"], random_data["value"])
        print(f'Successful set to PostgreSQL {random_data["key"]} : {random_data["value"]}')
    else:
        data = postgres_conn.get_data(random_data["key"])
        print(f'Got from PostgreSQL {data["key"]} : {data["value"]}')

    if not redis_conn.get_data(random_data["key"]):
        redis_conn.set_data(random_data["key"], random_data["value"])
        print(f'Successful set to Redis {random_data["key"]} : {random_data["value"]}')
    else:
        data = redis_conn.get_data(random_data['key'])
        print(f'Got from Redis {random_data["key"]} : {data}')


# Закрытие соединения с БД
postgres_conn.close()
redis_conn.close()
