import random


def get_random_data(limit):
    key = random.randint(0, limit)
    value = random.randint(0, limit * 100)

    return {'key': key,
            'value': value}
