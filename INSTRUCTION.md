### 1. Создайте файл окружения

Первостепенно создайте файл с переменными окружения (`.env`): `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `CLICKHOUSE_DB`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` 

**Примет `.env` файла:**

```
DB_NAME=db_name
DB_USER=user
DB_PASSWORD=password
CLICKHOUSE_DB=clickhouse_db_name
CLICKHOUSE_USER=clickhouse_user
CLICKHOUSE_PASSWORD=clickhouse_password
```

### 2. Запуск контейнеров

Перед выполнением команд убедись, что твоя Docker-инфраструктура запущена и работает. При необходимости подними окружение в терминале:

```bash
docker compose up -d --build
```

### 3. Заполнение базы данных Postgres (ETL)

Первый скрипт считывает сырые данные из таблицы `mock_data` в Postgres, обрабатывает их, убирает дубликаты и раскладывает по таблицам связей (звездной схеме) обратно в Postgres.

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark_jobs/etl-star.py
```

### 4. Расчет витрин и перенос в ClickHouse (Reports)

Второй скрипт забирает уже структурированную звездную схему из Postgres, рассчитывает 6 агрегированных аналитических отчетов (Data Marts) средствами Spark и заливает их в ClickHouse в режиме `overwrite`.

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark_jobs/clickhouse-reports.py
```