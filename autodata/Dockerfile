FROM python:3.11-slim

WORKDIR /app

# Instalar dependências do sistema para gcloud e audio
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    apt-transport-https \
    ca-certificates \
    gnupg \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependências públicas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalar torchcodec (versão nightly)
RUN pip install --no-cache-dir torchcodec --pre

# Copiar e instalar pacotes privados localmente
COPY packages/*.whl /tmp/packages/
RUN pip install --no-cache-dir /tmp/packages/*.whl && rm -rf /tmp/packages

# Copy modules
COPY config.py .
COPY organize.py .
COPY run_audio_val.py .
COPY main.py .

CMD ["python", "main.py"]