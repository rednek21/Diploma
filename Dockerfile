FROM python:3.11-alpine AS base

ENV PYTHONDONTWRITEBYTECODE 1

COPY requirements.txt .

RUN apk --no-cache add libpq-dev gcc && rm -f /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt


# Stage 2: Final image
FROM python:3.11-alpine

COPY --from=base /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=base /usr/local/bin/ /usr/local/bin/

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR /app

COPY . .

RUN chmod +x ./entrypoint.sh
RUN mkdir -p /app/results

ENTRYPOINT ["sh", "entrypoint.sh"]