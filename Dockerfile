FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY my_cache.py crawler.py ./

CMD ["python", "crawler.py"]