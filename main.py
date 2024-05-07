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

# Переменные для хранения количества промахов
db_misses = {}
redis_misses = {}
ht_misses = {}

# Всего записей занести в структуры
records_number = [100, 200, 300, 400, 500]
filling_probabilities = [0.5, 0.6, 0.7, 0.8, 0.9]

# Инициализация списков для хранения данных для графиков
probabilities = []
db_search_times = {filling_probability: [] for filling_probability in filling_probabilities}
redis_search_times = {filling_probability: [] for filling_probability in filling_probabilities}
ht_search_times = {filling_probability: [] for filling_probability in filling_probabilities}
db_misses_data = {filling_probability: [] for filling_probability in filling_probabilities}
redis_misses_data = {filling_probability: [] for filling_probability in filling_probabilities}
ht_misses_data = {filling_probability: [] for filling_probability in filling_probabilities}

probabilities = []

# Заполнение структур данных разными объемами и вероятностными распределениями
for filling_probability in filling_probabilities:
    # Append the filling probability to the probabilities list
    probabilities.append(filling_probability)
    for record_number in records_number:
        # Rest of the code remains unchanged
        # Инициализация временных списков
        db_time = []
        redis_time = []
        hash_table_time = []

        # Инициализация списков для памяти
        db_memory = []
        redis_memory = []
        ht_memory = []

        # Инициализация счетчиков промахов
        db_misses_counter = 0
        redis_misses_counter = 0
        ht_misses_counter = 0

        print(f'Заполнение структур. Количество элементов: {record_number}, Вероятность заполнения БД: {filling_probability}')
        for i in range(record_number):
            random_data = get_random_data(record_number * 100)

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
            random_data = get_random_data(record_number * 100)

            key = random_data["key"]
            value = random_data["value"]

            # Поиск в хэш-таблице
            if key in hash_table:
                start = time.monotonic()
                value = hash_table.get(key)
                end = time.monotonic()
                hash_table_time.append(end - start)
            else:
                ht_misses_counter += 1  # Увеличение счетчика промахов для хэш-таблицы

            # Поиск в Redis, если не найдено -> поиск в БД, если не найдено -> запись данных в БД
            if not redis_conn.get_data(key):
                redis_misses_counter += 1  # Увеличение счетчика промахов для Redis
                if postgres_conn.get_data(key):
                    start = time.monotonic()
                    db_data = postgres_conn.get_data(key)
                    end = time.monotonic()
                    db_time.append(end - start)
                    db_memory.append(getsizeof(db_data['value']))
                    redis_conn.set_data(db_data['key'], db_data['value'])
                else:
                    db_misses_counter += 1  # Увеличение счетчика промахов для БД
                    start = time.monotonic()
                    postgres_conn.set_data(key, value)
                    end = time.monotonic()
                    db_time.append(end - start)
                    db_memory.append(getsizeof(value))
                    redis_conn.set_data(key, value)
            else:  # Добавление времени доступа для Redis
                start = time.monotonic()
                data = redis_conn.get_data(key)
                end = time.monotonic()
                redis_time.append(end - start)
                redis_memory.append(getsizeof(data))

        # Расчет средних значений времени
        avg_db_search_time = mean(db_time) if db_time else 0
        avg_redis_search_time = mean(redis_time) if redis_time else 0
        avg_ht_search_time = mean(hash_table_time)

        # Добавление значений в соответствующие списки
        db_search_times[filling_probability].append(avg_db_search_time)
        redis_search_times[filling_probability].append(avg_redis_search_time)
        ht_search_times[filling_probability].append(avg_ht_search_time)

        # Замеры памяти
        db_memory_usage[(record_number, filling_probability)] = sum(db_memory)
        redis_memory_usage[(record_number, filling_probability)] = sum(redis_memory)
        ht_memory_usage[(record_number, filling_probability)] = sum(ht_memory)

        # Сохранение количества промахов
        db_misses_data[filling_probability].append(db_misses_counter)
        redis_misses_data[filling_probability].append(redis_misses_counter)
        ht_misses_data[filling_probability].append(ht_misses_counter)

        # Вывод результатов
        print(f'\nРезультаты при вероятности добавления новой записи в БД: {filling_probability}')
        print(f'Среднее время поиска в БД: {avg_db_search_time:.10f} сек')
        print(f'Среднее время поиска в Redis: {avg_redis_search_time:.10f} сек')
        print(f'Среднее время поиска в хэш-таблице: {avg_ht_search_time:.10f} сек')

        print(f'Объем памяти БД: {db_memory_usage[(record_number, filling_probability)]} байт')
        print(f'Объем памяти Redis: {redis_memory_usage[(record_number, filling_probability)]} байт')
        print(f'Объем памяти хэш-таблицы: {ht_memory_usage[(record_number, filling_probability)]} байт')
        print(f'Количество промахов в БД: {db_misses_counter}')
        print(f'Количество промахов в Redis: {redis_misses_counter}')
        print(f'Количество промахов в хэш-таблице: {ht_misses_counter}\n')

        print('\nОчистка данных...')
        hash_table.clear()
        postgres_conn.clear_data()
        redis_conn.clear_data()
        print('Все данные удалены\n')

# Закрытие соединения с БД
postgres_conn.close()
redis_conn.close()

# Сохранение графиков в файлы

# Графики для времени доступа
for filling_probability in filling_probabilities:
    plt.figure(figsize=(10, 6))
    x = records_number
    plt.plot(x, db_search_times[filling_probability], label=f'DB')
    plt.plot(x, redis_search_times[filling_probability], label=f'Redis')
    plt.plot(x, ht_search_times[filling_probability], label=f'Hash Table')
    plt.title(f'Average Search Time vs Number of Records (Fill Probability: {filling_probability})')
    plt.xlabel('Number of Records')
    plt.ylabel('Average Search Time (seconds)')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'app_results/search_time_vs_records_filling_{filling_probability}.png')
    plt.close()

# Графики для количества промахов
for filling_probability in filling_probabilities:
    plt.figure(figsize=(10, 6))
    plt.plot(probabilities, db_misses_data[filling_probability], label=f'DB')
    plt.plot(probabilities, redis_misses_data[filling_probability], label=f'Redis')
    plt.plot(probabilities, ht_misses_data[filling_probability], label=f'Hash Table')
    plt.title(f'Number of Misses vs Fill Probability (Fill Probability: {filling_probability})')
    plt.xlabel('Fill Probability')
    plt.ylabel('Number of Misses')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'app_results/misses_vs_fill_probability_filling_{filling_probability}.png')
    plt.close()
