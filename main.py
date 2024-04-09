import os
import time
from sys import getsizeof

from DataStructures import PostgreSQL, Redis
from dotenv import load_dotenv

from common import get_random_data

from statistics import mean


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

# Создание объекта подключения в БД
postgres_conn = PostgreSQL(dbname=env.get('POSTGRES_DB'),
                               user=env.get('POSTGRES_USER'),
                               password=env.get('POSTGRES_PASSWORD'),
                               host=env.get('POSTGRES_HOST'),
                               port=env.get('POSTGRES_PORT')
                               )
# Создание объекта подключения к Redis
redis_conn = Redis(host=env.get('REDIS_HOST'),
                   port=env.get('REDIS_PORT'),
                   db=1)

# Инициализация хэщ-таблицы
hash_table = {}

# Подключение к БД и создание таблицы
postgres_conn.connect()
postgres_conn.create_table()

# Подлкючение к Redis
redis_conn.connect()

# Списки для хранения замеров времени получения данных в БД, Redis и хэш-таблице
db_time = []
redis_time = []
hash_table_time = []

# Заполнение структур данными
for i in range(2000):
    random_data = get_random_data()
    # print('\n' + str(random_data))

    key = random_data["key"]
    value = random_data["value"]

    if not key in hash_table:
        hash_table.update({key : value})
        # print(f'\nSuccessful set to hash-table {key} : {value}')
    # else:
    #     data = {
    #         'key': key,
    #         'value': hash_table.get(key)
    #     }
        # print(f'Such key-value pair already exists in hash-table')


    if not postgres_conn.get_data(key):
        postgres_conn.set_data(key, value)
        # print(f'Successful set to PostgreSQL {key} : {value}')
    # else:
        # data = postgres_conn.get_data(key)
        # print(f'Such key-value pair already exists in DB')

    # if not redis_conn.get_data(key):
    #     redis_conn.set_data(key, value)
    #     print(f'Successful set to Redis {key} : {value}')
    # else:
    #     data = redis_conn.get_data(key)
    #     print(f'Got from Redis {key} : {data}')


# print('\nEnd of setting data to structures\n')
# print('\nStart searching data in structures\n')

# Поиск данных в заполненных структурах
for i in range(2000):
    random_data = get_random_data()
    # print('\n' + str(random_data))

    key = random_data["key"]
    value = random_data["value"]

    # Поиск в хэш-таблице
    if key in hash_table:
        start = time.monotonic()
        value = hash_table.get(key)
        end = time.monotonic()
        hash_table_time.append(end-start)
        # print(f'Got from Hash-table {key} : {value} in {end - start} seconds')
    else:
        hash_table.update({key: value})

    # Поиск в Redis, если не найдено -> поиск в БД, если не найдено -> запись данных в БД
    if not redis_conn.get_data(key):
        # print(f"\n{key} not found in redis. Going to DB")
        if postgres_conn.get_data(key):

            start = time.monotonic()
            db_data = postgres_conn.get_data(key)
            end = time.monotonic()
            db_time.append(end - start)
            # print(f'\nFound in PostgreSQL {db_data["key"]} : {db_data["value"]} in {end - start} seconds')

            redis_conn.set_data(db_data['key'], db_data['value'])
            # print(f'\nSuccessful set to Redis {db_data["key"]} : {db_data["value"]}')
        else:
            # print(f'Key {key} not found in PostgreSQL')
            postgres_conn.set_data(key, value)
            redis_conn.set_data(key, value)
            # print(f'\nSuccessful set to PostgreSQL {key} : {value}')
    else:
        start = time.monotonic()
        data = redis_conn.get_data(key)
        end = time.monotonic()
        redis_time.append(end - start)
        # print(f'Got from Redis {key} : {data} in {end-start} seconds')


avg_db_search_time = mean(db_time)
print(f'\nПроизведено замеров для БД: {len(db_time)}')
print(f'Оценка Мат. ожидания времени поиска в БД: {round(avg_db_search_time, 10)}')
print(f'Объем данных в БД: {postgres_conn.get_database_size_bytes()} bytes')

avg_redis_search_time = mean(redis_time)
print(f'\nПроизведено замеров для Redis: {len(redis_time)}')
print(f'Оценка Мат. ожидания времени поиска в Redis: {round(avg_redis_search_time, 10)}')
print(f'Объем данных в Redis: {redis_conn.get_total_memory_usage()} bytes')


avg_ht_search_time = mean(hash_table_time)
print(f'\nПроизведено замеров для хэш-таблицы: {len(hash_table_time)}')
print(f'Оценка Мат. ожидания времени поиска в хэш-таблице: {avg_ht_search_time:.10f}')
print(f'Объем данных в хэш-таблице: {getsizeof(hash_table)} bytes')


# Закрытие соединения с БД
postgres_conn.close()
redis_conn.close()
