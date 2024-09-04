FROM python:3-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY . /app


CMD ["python", "crawler.py"]