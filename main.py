import multiprocessing
import os
import time

import pandas as pd

from random import uniform
from sys import getsizeof
from dotenv import load_dotenv
from statistics import mean
import matplotlib.pyplot as plt
from DataStructures import PostgreSQL, Redis
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

# Создание объекта подключения к БД
postgres_conn = PostgreSQL(dbname=env.get('POSTGRES_DB'),
                           user=env.get('POSTGRES_USER'),
                           password=env.get('POSTGRES_PASSWORD'),
                           host=env.get('POSTGRES_HOST'),
                           port=env.get('POSTGRES_PORT'))
# Создание объекта подключения к Redis
redis_conn = Redis(host=env.get('REDIS_HOST'),
                   port=env.get('REDIS_PORT'),
                   db=1)

# Инициализация хэш-таблицы
hash_table = {}

# Подключение к БД и создание таблицы
postgres_conn.connect()
postgres_conn.create_table()

# Подключение к Redis
redis_conn.connect()

# Переменные для хранения замеров времени
avg_db_search_time = {}
avg_redis_search_time = {}
avg_ht_search_time = {}

# Переменные для хранения замеров памяти
db_memory_usage = {}
redis_memory_usage = {}
ht_memory_usage = {}

# Переменные для хранения количества промахов
db_misses = {}
redis_misses = {}
ht_misses = {}

# Всего записей занести в структуры1_000_000
records_number = [1_000, 2_000, 5_000, 10_000, 20_000, 50_000,
                  100_000, 200_000, 500_000]
filling_probabilities = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]

# records_number = [1_000, 5_000, 10_000]
# filling_probabilities = [0.5, 0.6, 0.7]

# Инициализация списков для хранения данных для графиков
probabilities = []

db_search_times = {filling_probability: [] for filling_probability in filling_probabilities}
redis_search_times = {filling_probability: [] for filling_probability in filling_probabilities}
ht_search_times = {filling_probability: [] for filling_probability in filling_probabilities}

db_misses_data = {filling_probability: [] for filling_probability in filling_probabilities}
redis_misses_data = {filling_probability: [] for filling_probability in filling_probabilities}
ht_misses_data = {filling_probability: [] for filling_probability in filling_probabilities}


def calculating():
    # Заполнение структур данных разными объемами и вероятностными распределениями
    for filling_probability in filling_probabilities:
        probabilities.append(filling_probability)
        for record_number in records_number:
            db_time = []
            redis_time = []
            hash_table_time = []

            db_memory = []
            redis_memory = []
            ht_memory = []

            db_misses_counter = 0
            redis_misses_counter = 0
            ht_misses_counter = 0

            print(
                f'Заполнение структур. Количество элементов: {record_number}, Вероятность заполнения БД: {filling_probability}')
            for i in range(record_number):
                random_data = get_random_data(record_number)
                key = random_data["key"]
                value = random_data["value"]

                if round(uniform(0, 1), 2) < filling_probability:
                    if not postgres_conn.get_data(key):
                        postgres_conn.set_data(key, value)
                        db_memory.append(getsizeof(value))
                        if round(uniform(0, 1), 2) > filling_probability:
                            if not redis_conn.get_data(key):
                                redis_conn.set_data(key, value)
                                redis_memory.append(getsizeof(value))
                else:
                    if key not in hash_table:
                        hash_table.update({key: value})
                        ht_memory.append(getsizeof(value))

            print('Данные внесены. Поиск данных...')
            for i in range(record_number):
                random_data = get_random_data(record_number)
                key = random_data["key"]
                value = random_data["value"]

                if key in hash_table:
                    start = time.monotonic()
                    value = hash_table.get(key)
                    end = time.monotonic()
                    hash_table_time.append(end - start)
                else:
                    ht_misses_counter += 1
                    hash_table.update({key: value})

                if not redis_conn.get_data(key):
                    redis_misses_counter += 1
                    start = time.monotonic()
                    db_data = postgres_conn.get_data(key)
                    if db_data:
                        end = time.monotonic()
                        db_time.append(end - start)
                        db_memory.append(getsizeof(db_data['value']))
                        redis_conn.set_data(db_data['key'], db_data['value'])
                    else:
                        db_misses_counter += 1
                        postgres_conn.set_data(key, value)
                        db_memory.append(getsizeof(value))
                        redis_conn.set_data(key, value)
                else:
                    start = time.monotonic()
                    data = redis_conn.get_data(key)
                    end = time.monotonic()
                    redis_time.append(end - start)
                    redis_memory.append(getsizeof(data))

            avg_db_search_time = mean(db_time) if db_time else 0
            avg_redis_search_time = mean(redis_time) if redis_time else 0
            avg_ht_search_time = mean(hash_table_time) if hash_table_time else 0

            db_search_times[filling_probability].append(avg_db_search_time)
            redis_search_times[filling_probability].append(avg_redis_search_time)
            ht_search_times[filling_probability].append(avg_ht_search_time)

            db_memory_usage[(record_number, filling_probability)] = sum(db_memory)
            redis_memory_usage[(record_number, filling_probability)] = sum(redis_memory)
            ht_memory_usage[(record_number, filling_probability)] = sum(ht_memory)

            db_misses_data[filling_probability].append(db_misses_counter)
            redis_misses_data[filling_probability].append(redis_misses_counter)
            ht_misses_data[filling_probability].append(ht_misses_counter)

            print('\nОчистка данных...')
            hash_table.clear()
            postgres_conn.clear_data()
            redis_conn.clear_data()
            print('Все данные удалены\n')

        # Графики для времени доступа
        plt.figure(figsize=(10, 6))
        x = records_number

        avg_db_search_time = mean(db_search_times[filling_probability])
        avg_redis_search_time = mean(redis_search_times[filling_probability])
        avg_ht_search_time = mean(ht_search_times[filling_probability])

        plt.plot(x, [t * 1e6 for t in db_search_times[filling_probability]],
                 label=f'БД (Среднее: {avg_db_search_time * 1e6:.2f} мс)')
        plt.plot(x, [t * 1e6 for t in redis_search_times[filling_probability]],
                 label=f'Redis (Среднее: {avg_redis_search_time * 1e6:.2f} мс)')
        plt.plot(x, [t * 1e6 for t in ht_search_times[filling_probability]],
                 label=f'Хэш-таблица (Среднее: {avg_ht_search_time * 1e6:.2f} мс)')

        plt.title(
            f'Среднее время доступа vs Количество записей (Вероятность заполнения БД: {f"{filling_probability:.2f}"}, Хэш-таблица: {f"{1 - filling_probability:.2f}"})')
        plt.xlabel('Количество записей')
        plt.ylabel('Среднее время доступа (наносекунды)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f'app_results/время_доступа_vs_количество_записей_заполнение_{f"{filling_probability:.2f}"}.png')
        plt.close()

        # Графики для количества промахов
        plt.figure(figsize=(10, 6))
        x = records_number

        avg_db_misses = mean(db_misses_data[filling_probability])
        avg_redis_misses = mean(redis_misses_data[filling_probability])
        avg_ht_misses = mean(ht_misses_data[filling_probability])

        plt.plot(x, db_misses_data[filling_probability], label=f'Промахи БД (Среднее: {avg_db_misses:.2f})')
        plt.plot(x, redis_misses_data[filling_probability], label=f'Промахи Redis (Среднее: {avg_redis_misses:.2f})')
        plt.plot(x, ht_misses_data[filling_probability], label=f'Промахи хэш-таблицы (Среднее: {avg_ht_misses:.2f})')

        plt.title(
            f'Количество промахов vs Количество записей (Вероятность заполнения БД: {f"{filling_probability:.2f}"}, Хэш-таблица: {f"{1 - filling_probability:.2f}"})')
        plt.xlabel('Количество записей')
        plt.ylabel('Количество промахов')
        plt.legend()
        plt.grid(True)
        plt.savefig(f'app_results/промахи_vs_количество_записей_заполнение_{f"{filling_probability:.2f}"}.png')
        plt.close()

        # Графики для занимаемой памяти
        plt.figure(figsize=(10, 6))
        x = records_number

        db_memory_usage_values = [db_memory_usage[(rn, filling_probability)] for rn in records_number]
        redis_memory_usage_values = [redis_memory_usage[(rn, filling_probability)] for rn in records_number]
        ht_memory_usage_values = [ht_memory_usage[(rn, filling_probability)] for rn in records_number]

        avg_db_memory_usage = mean(db_memory_usage_values)
        avg_redis_memory_usage = mean(redis_memory_usage_values)
        avg_ht_memory_usage = mean(ht_memory_usage_values)

        plt.plot(x, db_memory_usage_values, label=f'Память БД (Среднее: {avg_db_memory_usage / 1024:.2f} КБ)')
        plt.plot(x, redis_memory_usage_values, label=f'Память Redis (Среднее: {avg_redis_memory_usage / 1024:.2f} КБ)')
        plt.plot(x, ht_memory_usage_values, label=f'Память хэш-таблицы (Среднее: {avg_ht_memory_usage / 1024:.2f} КБ)')

        plt.title(
            f'Память vs Количество записей (Вероятность заполнения БД: {f"{filling_probability:.2f}"}, Хэш-таблица: {f"{1 - filling_probability:.2f}"})')
        plt.xlabel('Количество записей')
        plt.ylabel('Память (Килобайты)')
        plt.legend()
        plt.grid(True)
        plt.savefig(f'app_results/память_vs_количество_записей_заполнение_{f"{filling_probability:.2f}"}.png')
        plt.close()

    # Создание сводной таблицы для Excel
    columns = ['record_number', 'filling_probability', 'avg_db_search_time', 'avg_redis_search_time', 'avg_ht_search_time',
               'db_misses', 'redis_misses', 'ht_misses', 'db_memory_usage', 'redis_memory_usage', 'ht_memory_usage']
    data = []

    for filling_probability in filling_probabilities:
        for record_number in records_number:
            row = [
                record_number,
                filling_probability,
                mean(db_search_times[filling_probability]) * 1e3,
                mean(redis_search_times[filling_probability]) * 1e3,
                mean(ht_search_times[filling_probability]) * 1e3,
                mean(db_misses_data[filling_probability]),
                mean(redis_misses_data[filling_probability]),
                mean(ht_misses_data[filling_probability]),
                sum([db_memory_usage[(record_number, fp)] for fp in filling_probabilities if fp == filling_probability]) / 1024,
                sum([redis_memory_usage[(record_number, fp)] for fp in filling_probabilities if fp == filling_probability]) / 1024,
                sum([ht_memory_usage[(record_number, fp)] for fp in filling_probabilities if fp == filling_probability]) / 1024
            ]
            data.append(row)

    df = pd.DataFrame(data, columns=columns)
    df.to_excel('app_results/summary_table.xlsx', index=False)


if __name__ == "__main__":
    pool = multiprocessing.Pool()
    pool.apply(calculating)
    pool.close()
    pool.join()

    postgres_conn.close()
    redis_conn.close()
