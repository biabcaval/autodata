FROM python:3.11-slim

# Instala FFmpeg
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todos os módulos
COPY config.py .
COPY utils.py .
COPY validation_pipeline.py .
COPY organization_pipeline.py .
COPY process_audio.py .
COPY main.py .

# Comando padrão (pode ser sobrescrito)
CMD ["python", "process_audio.py"]
