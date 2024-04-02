import random

def get_random_data():
    key = random.randint(0, 100000)
    value = random.randint(0, 1000000)

    return {'key': key,
            'value': value}