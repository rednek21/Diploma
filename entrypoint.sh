#!/bin/sh

if [ "$DATABASE" = "postgres" ]
then
    echo "Waiting for postgres..."

    while ! nc -z $DATABASE_HOST $DATABASE_PORT; do
      sleep 0.1
    done

    echo "PostgreSQL started"
fi

python manage.py flush --no-input
python manage.py migrate
python manage.py loaddata categories.json
python manage.py loaddata projects.json

if [ "$DEBUG" = "False" ]
then
    python manage.py collectstatic --noinput
fi

exec "$@"