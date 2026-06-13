# Парсер мануалів GenuineFactoryParts

Python-скрипт для збору даних з каталогу GenuineFactoryParts ARI PartStream. Скрипт збирає OEM-номери деталей та їх описи за шляхом:

`MTD Merged Data Staging > Troy-Bilt > 11-Push Walk-Behind Mowers > 2024/2025 Models > model > Assemblies > scheme`

Результат зберігається у CSV. Повторний запуск не створює дублікати, бо записи оновлюються за унікальним ключем.

## Встановлення

Вимоги:

- Python 3.11 або новіший
- доступ до інтернету з машини, на якій запускається скрипт

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
```

Для Linux/macOS активація віртуального середовища:

```bash
source venv/bin/activate
```

## Запуск

```bash
python scraper.py --years 2024 2025 --output data/parts.csv
```

Запуск для налагодження у видимому браузері:

```bash
python scraper.py --years 2025 --headless false --slow-mo-ms 150 --output data/parts.csv
```

Файли за замовчуванням:

- CSV-файл: `data/parts.csv`
- лог-файл: `logs/run.log`
- скріншоти помилок та HTML-знімки сторінок: `artifacts/errors/`

Колонки CSV:

- `unique_key`
- `full_scheme_path`
- `year`
- `model`
- `assembly`
- `scheme`
- `oem`
- `description`
- `scraped_at`

`unique_key` формується як SHA-256 від `full_scheme_path`, `oem` та `description`. Якщо запис з таким ключем уже є у CSV, скрипт оновлює його тільки тоді, коли змінилися основні поля.

## Імпорт CSV у Google Sheets

1. Відкрити Google Sheets.
2. Створити або відкрити потрібну таблицю.
3. Обрати **File > Import > Upload**.
4. Завантажити файл `data/parts.csv`.
5. Обрати заміну поточного аркуша або імпорт у новий аркуш.

## Запуск за розкладом у Windows Task Scheduler

1. Відкрити **Task Scheduler**.
2. Створити нову задачу.
3. Додати тригер з потрібним розкладом.
4. Додати дію:
   - Program/script: `C:\path\to\repo\venv\Scripts\python.exe`
   - Arguments: `scraper.py --years 2024 2025 --output data\parts.csv --log-file logs\run.log`
   - Start in: `C:\path\to\repo`
5. Зберегти задачу і запустити її вручну один раз, щоб перевірити оновлення `logs\run.log`.

## Тести

```bash
pytest
```

## Лінтинг

```bash
ruff check .
```

Тести перевіряють upsert-логіку CSV, стабільність унікального ключа, обробку CLI-параметрів та базовий парсинг рядків з деталями.

## GitHub Actions

На кожен push у гілку `main` запускається workflow `Scrape Parts Catalog`. Він встановлює залежності, запускає скрипт у headless-режимі та зберігає результати як artifacts:

- `parts-csv` — файл `data/parts.csv`
- `scraper-log` — файл `logs/run.log`
- `scraper-error-artifacts` — скріншоти та HTML-знімки помилок, якщо вони були створені

## Примітки

Каталог рендериться динамічно у браузері, тому скрипт використовує Playwright замість статичного HTML-парсингу. Якщо постачальник змінить інтерфейс PartStream, запустіть скрипт з параметрами `--headless false --slow-mo-ms 150` і перевірте артефакти помилок у `artifacts/errors/`.
