FROM python:3.11-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

COPY requirements.txt pyproject.toml ./
RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt \
    && python -m playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*

COPY scraper.py ./

RUN groupadd --system --gid 10001 scraper \
    && useradd --uid 10001 --gid scraper --create-home --shell /usr/sbin/nologin scraper \
    && mkdir -p /app/data /app/logs /app/artifacts/errors \
    && chown -R scraper:scraper /app /ms-playwright

USER scraper

ENTRYPOINT ["python", "scraper.py"]
CMD ["--years", "2024", "2025", "--output", "data/parts.csv", "--log-file", "logs/run.log", "--artifacts-dir", "artifacts/errors"]
