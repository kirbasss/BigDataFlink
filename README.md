# BigDataFlink

Лабораторная работа №3 по дисциплине «Анализ больших данных».

Выполнил студент Арусланов К.А. группы М8О-303Б-23

## Быстрый запуск

1. Поднять сервисы:

```powershell
docker compose up --build -d
```

2. Запустить Flink-джобу:

```powershell
docker compose exec jobmanager flink run -d /opt/flink/usrlib/lab3-flink-job.jar
```

3. Проверить, что джоба запущена:

```powershell
docker compose exec jobmanager flink list
```

4. После загрузки данных выполнить `postgres/add_foreign_keys.sql` в DBeaver.

5. Проверить запросы из `postgres/analytics_queries.sql`.

PostgreSQL: `localhost:5432`, база `lab3`, пользователь `flink`, пароль `flink`  
Flink UI: `http://localhost:8081`


6. Остановка

```powershell
docker compose down
```

Если нужно удалить и данные PostgreSQL:

```powershell
docker compose down -v
```

## О проекте

В репозитории реализован потоковый ETL-конвейер:

1. Python-приложение читает все `csv` из папки [исходные данные](./%D0%B8%D1%81%D1%85%D0%BE%D0%B4%D0%BD%D1%8B%D0%B5%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%B5), преобразует каждую строку в `json` и отправляет сообщения в Kafka.
2. Flink-приложение читает Kafka-топик в режиме streaming.
3. Поток преобразуется в модель данных «звезда».
4. Результат сохраняется в PostgreSQL.

## Структура проекта

- `docker-compose.yml` — поднимает PostgreSQL, Kafka, Flink и producer.
- `Dockerfile.flink` — сборка Flink-джобы.
- `Dockerfile.producer` — сборка приложения-генератора событий.
- `producer/producer.py` — отправка строк CSV в Kafka как JSON.
- `flink-job/` — код Flink-приложения на Java.
- `postgres/init/01_schema.sql` — схема БД со звездой.
- `postgres/add_foreign_keys.sql` — внешние ключи и индексы для схемы звезда после загрузки данных.
- `postgres/analytics_queries.sql` — примеры аналитических SQL-запросов.
- `исходные данные/` — 10 CSV-файлов по 1000 строк.

## Целевая схема

В PostgreSQL создаются таблицы:

- `dim_customer`
- `dim_pet`
- `dim_seller`
- `dim_product`
- `dim_store`
- `dim_supplier`
- `dim_date`
- `fact_sales`

В качестве ключей измерений используются детерминированные hash-ключи, чтобы Flink мог безопасно делать `upsert` в потоковом режиме.

`FOREIGN KEY` не задаются сразу, потому что Flink пишет измерения и факт разными потоковыми sink-ами, и порядок вставки в PostgreSQL не гарантирован. Из-за этого факт может прийти раньше родительской записи в измерении и вставка упадёт по ограничению ссылочной целостности. Поэтому сначала выполняется потоковая загрузка данных, а внешние ключи добавляются после её завершения отдельным SQL-скриптом.

## Что нужно для запуска

- Docker Desktop
- Docker Compose
- Любой SQL-клиент: DBeaver, DataGrip, pgAdmin

## Как работает преобразование

Каждое сообщение Kafka содержит:

- исходные поля строки CSV
- `source_file` — имя файла-источника
- `source_row_number` — номер строки внутри файла

Flink выполняет:

1. Чтение JSON из Kafka.
2. Парсинг и нормализацию типов.
3. Выделение измерений `customer`, `pet`, `seller`, `product`, `store`, `supplier`, `date`.
4. Формирование таблицы фактов `fact_sales`.
5. Потоковую запись в PostgreSQL через JDBC sink.
