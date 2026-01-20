FROM python:3.11-slim

WORKDIR /app

# Copia e instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia módulos
COPY config.py .
COPY utils.py .
COPY process_audio.py .
COPY main.py .

CMD ["python", "process_audio.py"]
