import pandas as pd
import matplotlib.pyplot as plt

# Загрузка данных из сводной таблицы
df = pd.read_excel('app_results/summary_table.xlsx')

# Группировка данных по вероятностям заполнения и вычисление средних значений
grouped = df.groupby('filling_probability').mean().reset_index()

# Получение списков для осей
filling_probabilities = grouped['filling_probability']
avg_db_search_time = grouped['avg_db_search_time']
avg_redis_search_time = grouped['avg_redis_search_time']
avg_ht_search_time = grouped['avg_ht_search_time']
db_misses = grouped['db_misses']
redis_misses = grouped['redis_misses']
ht_misses = grouped['ht_misses']
db_memory_usage = grouped['db_memory_usage']
redis_memory_usage = grouped['redis_memory_usage']
ht_memory_usage = grouped['ht_memory_usage']

# График для среднего времени поиска
plt.figure(figsize=(10, 6))
plt.plot(filling_probabilities, avg_db_search_time, label='БД (в мс)', marker='o')
plt.plot(filling_probabilities, avg_redis_search_time, label='Redis (в мс)', marker='o')
plt.plot(filling_probabilities, avg_ht_search_time, label='Хэш-таблица (в мс)', marker='o')
plt.title('Среднее время поиска vs Вероятность заполнения')
plt.xlabel('Вероятность заполнения')
plt.ylabel('Среднее время поиска (мс)')
plt.legend()
plt.grid(True)
plt.savefig('results/время_поиска_vs_вероятность_заполнения.png')
plt.close()

# График для количества промахов
plt.figure(figsize=(10, 6))
plt.plot(filling_probabilities, db_misses, label='Промахи БД', marker='o')
plt.plot(filling_probabilities, redis_misses, label='Промахи Redis', marker='o')
plt.plot(filling_probabilities, ht_misses, label='Промахи хэш-таблицы', marker='o')
plt.title('Количество промахов vs Вероятность заполнения')
plt.xlabel('Вероятность заполнения')
plt.ylabel('Количество промахов')
plt.legend()
plt.grid(True)
plt.savefig('results/промахи_vs_вероятность_заполнения.png')
plt.close()

# График для использования памяти
plt.figure(figsize=(10, 6))
plt.plot(filling_probabilities, db_memory_usage, label='Память БД (в КБ)', marker='o')
plt.plot(filling_probabilities, redis_memory_usage, label='Память Redis (в КБ)', marker='o')
plt.plot(filling_probabilities, ht_memory_usage, label='Память хэш-таблицы (в КБ)', marker='o')
plt.title('Использование памяти vs Вероятность заполнения')
plt.xlabel('Вероятность заполнения')
plt.ylabel('Использование памяти (КБ)')
plt.legend()
plt.grid(True)
plt.savefig('results/память_vs_вероятность_заполнения.png')
plt.close()