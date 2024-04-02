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

# Заполнение структур данными
for i in range(100):
    random_data = get_random_data()
    # print('\n' + str(random_data))

    key = random_data["key"]
    value = random_data["value"]

    if not key in hash_table:
        hash_table.update({key : value})
        # print(f'\nSuccessful set to hash-table {key} : {value}')
    # else:
        # data = {
        #     'key': key,
        #     'value': hash_table.get(key)
        # }
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


print('\nEnd of setting data to structures\n')
print('\nStart searching data in structures\n')

# Поиск данных в заполненных структурах
for i in range(100):
    random_data = get_random_data()
    # print('\n' + str(random_data))

    key = random_data["key"]
    value = random_data["value"]

    if not redis_conn.get_data(key):
        print(f"\n{key} not found in redis. Going to DB")
        if postgres_conn.get_data(key):
            db_data = postgres_conn.get_data(key)
            print(f'\nFound in PostgreSQL {db_data["key"]} : {db_data["value"]}')
            redis_conn.set_data(db_data['key'], db_data['value'])
            print(f'\nSuccessful set to Redis {db_data["key"]} : {db_data["value"]}')
        else:
            print(f'Key {key} not found in PostgreSQL')
            postgres_conn.set_data(key, value)
            print(f'\nSuccessful set to PostgreSQL {key} : {value}')

    else:
        data = redis_conn.get_data(key)
        print(f'Got from Redis {key} : {data}')



# Закрытие соединения с БД
postgres_conn.close()
redis_conn.close()
