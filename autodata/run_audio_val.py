
import os
from datetime import datetime
from moises_ml_utils import AudioVerification
import pandas as pd
from google.cloud import storage

from config import BUCKET_ORIGEM, BUCKET_DESTINO, FORMATOS_ACEITOS


def listar_arquivos(storage_client: storage.Client, bucket_name: str) -> list[str]:
    """Lista todos os arquivos de áudio no bucket."""
    arquivos = []
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()
    
    for blob in blobs:
        nome = blob.name
        if any(nome.lower().endswith(ext) for ext in FORMATOS_ACEITOS):
            arquivos.append(nome)
    
    
    return arquivos[:1000]



def paths_metadata(listed_files):
    # Extrai identificador da música a partir do path
    def extract_song_identifier(path: str) -> str:
        parts = path.split('/')

        # Verifica se é um stem (pasta pai termina com -stem)
        for part in parts:
            if part.endswith('-stem'):
                return part[:-5]  # Remove sufixo '-stem'

        # Se não é stem, usa o nome do arquivo sem extensão
        filename = parts[-1]
        return os.path.splitext(filename)[0]
    
    # Agrupa arquivos por música e atribui song_id único por grupo
    song_identifiers = [extract_song_identifier(path) for path in listed_files]
    unique_songs = {song: idx for idx, song in enumerate(dict.fromkeys(song_identifiers), 1)}
    
    df = pd.DataFrame([
        {
            "file_id": f"file_{file_idx}",
            "song_id": f"song_{unique_songs[song_id]}",
            "path": f'gs://{BUCKET_ORIGEM}/{path}'
        }
        for file_idx, (path, song_id) in enumerate(zip(listed_files, song_identifiers), 1)
    ])
    
    df.to_csv("paths_metadata.csv", index=False)

    return df

def run_pipeline():
    """Executa a pipeline de processamento."""
    
    print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📥 Bucket origem: {BUCKET_ORIGEM}")
    print(f"📤 Bucket destino: {BUCKET_DESTINO}")
    
    # Cria cliente de storage
    storage_client = storage.Client('data-sourcing')
    
    # Lista arquivos
    print("\n📂 Listando arquivos...")
    listed_files = listar_arquivos(storage_client, BUCKET_ORIGEM)
    print(f"📁 Total de arquivos no bucket: {len(listed_files)}")
    
    df = paths_metadata(listed_files)
    audio_verifier = AudioVerification(storage_client='data-sourcing', bucket_name='beatriz-temp')
    audio_verifier.verify_table(df=df, new_songs=True)


if __name__ == "__main__":
    run_pipeline()


