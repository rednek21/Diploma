import os
import time
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

# Инициализация хэш-таблицы
hash_table = {}

# Подключение к БД и создание таблицы
postgres_conn.connect()
postgres_conn.create_table()

# Подключение к Redis
redis_conn.connect()

# Переменные для хранения средних замеров времени
avg_db_search_time = {}
avg_redis_search_time = {}
avg_ht_search_time = {}

# Переменные для хранения замеров памяти
db_memory_usage = {}
redis_memory_usage = {}
ht_memory_usage = {}

# Всего записей занести в структуры
records_number = [100, 200, 300]
filling_probabilities = [0.5, 0.6, 0.7, 0.8, 0.9]

# Lists to store data for plotting
probabilities = []
avg_db_search_times = []
avg_redis_search_times = []
avg_ht_search_times = []
db_memory_usages = []
redis_memory_usages = []
ht_memory_usages = []

# Заполнение структур данных разными объемами и вероятностными распределениями
for record_number in records_number:
    for filling_probability in filling_probabilities:
        # Инициализация временных списков
        db_time = []
        redis_time = []
        hash_table_time = []

        # Инициализация списков для памяти
        db_memory = []
        redis_memory = []
        ht_memory = []

        print(f'Заполнение структур. Количество элементов: {record_number}')
        for i in range(record_number):
            random_data = get_random_data(record_number)

            key = random_data["key"]
            value = random_data["value"]

            # Добавление записей в БД с вероятностью filling_probabilities
            # и в хэш-таблицу с вероятностью (1 - filling_probabilities)
            if round(uniform(0, 1), 2) < filling_probability:
                if not postgres_conn.get_data(key):
                    start = time.monotonic()
                    postgres_conn.set_data(key, value)
                    end = time.monotonic()
                    db_time.append(end - start)
                    db_memory.append(getsizeof(value))
            else:
                if key not in hash_table:
                    start = time.monotonic()
                    hash_table.update({key: value})
                    end = time.monotonic()
                    hash_table_time.append(end - start)
                    ht_memory.append(getsizeof(value))

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

            # Поиск в Redis, если не найдено -> поиск в БД, если не найдено -> запись данных в БД
            if not redis_conn.get_data(key):
                if postgres_conn.get_data(key):
                    start = time.monotonic()
                    db_data = postgres_conn.get_data(key)
                    end = time.monotonic()
                    db_time.append(end - start)
                    db_memory.append(getsizeof(db_data['value']))
                    redis_conn.set_data(db_data['key'], db_data['value'])
                else:
                    start = time.monotonic()
                    postgres_conn.set_data(key, value)
                    end = time.monotonic()
                    db_time.append(end - start)
                    db_memory.append(getsizeof(value))
                    redis_conn.set_data(key, value)
            else:
                start = time.monotonic()
                data = redis_conn.get_data(key)
                end = time.monotonic()
                redis_time.append(end - start)
                redis_memory.append(getsizeof(data))

        # Расчет средних значений времени
        avg_db_search_time[(record_number, filling_probability)] = mean(db_time)
        avg_redis_search_time[(record_number, filling_probability)] = mean(redis_time)
        avg_ht_search_time[(record_number, filling_probability)] = mean(hash_table_time)

        # Замеры памяти
        db_memory_usage[(record_number, filling_probability)] = sum(db_memory)
        redis_memory_usage[(record_number, filling_probability)] = sum(redis_memory)
        ht_memory_usage[(record_number, filling_probability)] = sum(ht_memory)

        # Append data for plotting
        probabilities.append(filling_probability)
        avg_db_search_times.append(avg_db_search_time[(record_number, filling_probability)])
        avg_redis_search_times.append(avg_redis_search_time[(record_number, filling_probability)])
        avg_ht_search_times.append(avg_ht_search_time[(record_number, filling_probability)])
        db_memory_usages.append(db_memory_usage[(record_number, filling_probability)])
        redis_memory_usages.append(redis_memory_usage[(record_number, filling_probability)])
        ht_memory_usages.append(ht_memory_usage[(record_number, filling_probability)])

        # Вывод результатов
        print(f'\nРезультаты при вероятности добавления новой записи в БД: {filling_probability}')
        print(f'Среднее время поиска в БД: {avg_db_search_time[(record_number, filling_probability)]:.10f} сек')
        print(f'Среднее время поиска в Redis: {avg_redis_search_time[(record_number, filling_probability)]:.10f} сек')
        print(f'Среднее время поиска в хэш-таблице: {avg_ht_search_time[(record_number, filling_probability)]:.10f} сек')

        print(f'Объем памяти БД: {db_memory_usage[(record_number, filling_probability)]} байт')
        print(f'Объем памяти Redis: {redis_memory_usage[(record_number, filling_probability)]} байт')
        print(f'Объем памяти хэш-таблицы: {ht_memory_usage[(record_number, filling_probability)]} байт\n')

        print('\nОчистка данных...')
        hash_table.clear()
        postgres_conn.clear_data()
        redis_conn.clear_data()
        print('Все данные удалены\n')

# Закрытие соединения с БД
postgres_conn.close()
redis_conn.close()

# Сохранение графиков в файлы
plt.figure(figsize=(10, 6))
plt.plot(probabilities, avg_db_search_times, label='Average DB Search Time')
plt.plot(probabilities, avg_redis_search_times, label='Average Redis Search Time')
plt.plot(probabilities, avg_ht_search_times, label='Average Hash Table Search Time')
plt.title('Average Search Time vs Filling Probability')
plt.xlabel('Filling Probability')
plt.ylabel('Average Search Time (seconds)')
plt.legend()
plt.grid(True)
plt.savefig('app_results/search_time_vs_filling_probability.png')

plt.figure(figsize=(10, 6))
plt.plot(probabilities, db_memory_usages, label='DB Memory Usage')
plt.plot(probabilities, redis_memory_usages, label='Redis Memory Usage')
plt.plot(probabilities, ht_memory_usages, label='Hash Table Memory Usage')
plt.title('Memory Usage vs Filling Probability')
plt.xlabel('Filling Probability')
plt.ylabel('Memory Usage (bytes)')
plt.legend()
plt.grid(True)
plt.savefig('app_results/memory_usage_vs_filling_probability.png')


