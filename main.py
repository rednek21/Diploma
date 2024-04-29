import os
import time

from sys import getsizeof
from dotenv import load_dotenv

from statistics import mean
from random import uniform

from common import get_random_data
from DataStructures import PostgreSQL, Redis

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

# Всего записй занести в сруктуры
# records_number = [1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000]
# records_number = [1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000]
records_number = [100, 200, 300]

# Вероятности внесения новой записи в определенную структуру
filling_probabilities = [0.5, 0.6, 0.7, 0.8, 0.9]

# Заполнение структур данных разными объемами
for record_number in records_number:
    for filling_probability in filling_probabilities:
        print(f'Заполнение структур. Количество элементов: {record_number}')
        for i in range(record_number):
            random_data = get_random_data(record_number)

            key = random_data["key"]
            value = random_data["value"]

            # Добавление записей в БД с вероятностью filling_probabilities
            # и в хэш-таблицу с вероятностью (1 - filling_probabilities)
            if round(uniform(0, 1), 2) < filling_probability:
                if not postgres_conn.get_data(key):
                    postgres_conn.set_data(key, value)
            else:
                if key not in hash_table:
                    hash_table.update({key: value})

            # if not redis_conn.get_data(key):
            #     redis_conn.set_data(key, value)
            #     print(f'Successful set to Redis {key} : {value}')
            # else:
            #     data = redis_conn.get_data(key)
            #     print(f'Got from Redis {key} : {data}')

        print('Данные внесены. Поиск данных...')
        # Поиск данных в заполненных структурах
        for i in range(record_number):
            random_data = get_random_data(record_number)

            key = random_data["key"]
            value = random_data["value"]

            # Поиск в хэш-таблице
            if key in hash_table:
                start = time.monotonic()
                value = hash_table.get(key)
                end = time.monotonic()
                hash_table_time.append(end - start)
            # else:
            #     hash_table.update({key: value})

            # Поиск в Redis, если не найдено -> поиск в БД, если не найдено -> запись данных в БД
            if not redis_conn.get_data(key):
                if postgres_conn.get_data(key):

                    start = time.monotonic()
                    db_data = postgres_conn.get_data(key)
                    end = time.monotonic()
                    db_time.append(end - start)

                    redis_conn.set_data(db_data['key'], db_data['value'])
                else:
                    postgres_conn.set_data(key, value)
                    redis_conn.set_data(key, value)
            else:
                start = time.monotonic()
                data = redis_conn.get_data(key)
                end = time.monotonic()
                redis_time.append(end - start)

        print(f'\nРезультаты при вероятности добавления новой записи в БД: {filling_probability}')

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

        print('\nОчистка данных...')
        hash_table.clear()
        postgres_conn.clear_data()
        redis_conn.clear_data()
        print('Все данные удалены\n')

# Закрытие соединения с БД
postgres_conn.close()
redis_conn.close()
