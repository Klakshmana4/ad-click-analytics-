FROM python:3.9-slim

WORKDIR /app

COPY generate_events.py .
COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["python", "generate_events.py"]
