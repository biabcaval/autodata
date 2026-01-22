FROM python:3.11-slim

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install keyrings.google-artifactregistry-auth
RUN gcloud auth print-access-token | pip install moises-ml-utils --index-url https://oauth2accesstoken:$(gcloud auth print-access-token)@us-central1-python.pkg.dev/moises-ds-data/python/simple/ --extra-index-url https://pypi.org/simple

# Copy modules
COPY config.py .
COPY organize.py .
COPY run_audio_val.py .
COPY main.py .

CMD ["python", "main.py"]