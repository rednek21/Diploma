import random

def get_random_data():
    key = random.randint(0, 9)
    value = random.randint(0, 1000)

    return {'key': key,
            'value': value}