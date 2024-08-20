FROM python:3.9-slim
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
#CMD ["python", "telemetry.py"]
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "telemetry:app"]