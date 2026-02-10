
import os
import ast
import json
import io
from datetime import datetime
from moises_ml_utils import AudioVerification
from moises_ml_utils.audio_verification import VerificationError
import pandas as pd
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor

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
    
    
    return arquivos  



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
    
    csv_path = os.path.join(os.getcwd(), "paths_metadata.csv")
    df.to_csv(csv_path, index=False)
    print(f"💾 CSV saved to: {csv_path}")

    return df


def organizar_arquivos_por_validacao(storage_client: storage.Client, df_validacao: pd.DataFrame):
    """
    Organiza arquivos no bucket de destino baseado nos resultados da validação.
    Arquivos com erros vão para 'quarentena/', arquivos sem erros vão para 'aprovados/'.
    """
    bucket_origem = storage_client.bucket(BUCKET_ORIGEM)
    bucket_destino = storage_client.bucket(BUCKET_DESTINO)
    
    def mover_arquivo(row):
        try:
            # Extrai o nome do blob do path
            path_origem = row['path'].replace(f'gs://{BUCKET_ORIGEM}/', '')
            
            # Define pasta de destino baseado em erros
            tem_erro = row.get('errors', {}) != {} and row.get('errors', {}) is not None
            pasta_destino = 'quarentena' if tem_erro else 'aprovados'
            
            # Mantém a estrutura de pastas original dentro da pasta de destino
            path_destino = f'{pasta_destino}/{path_origem}'
            
            # Copia o arquivo
            blob_origem = bucket_origem.blob(path_origem)
            
            if blob_origem.exists():
                bucket_destino.copy_blob(blob_origem, bucket_destino, path_destino)
                status = '❌ QUARENTENA' if tem_erro else '✅ APROVADO'
                print(f'{status}: {path_origem}')
                return {'path': path_origem, 'status': 'sucesso', 'destino': pasta_destino}
            else:
                print(f'⚠️  Arquivo não encontrado: {path_origem}')
                return {'path': path_origem, 'status': 'não encontrado', 'destino': None}
                
        except Exception as e:
            print(f'❌ Erro ao mover {path_origem}: {str(e)}')
            return {'path': path_origem, 'status': 'erro', 'destino': None, 'erro': str(e)}
    
    print("\n" + "="*60)
    print("📦 ORGANIZANDO ARQUIVOS NO BUCKET DE DESTINO")
    print("="*60)
    
    # Processa em paralelo com limite de workers
    with ThreadPoolExecutor(max_workers=10) as executor:
        resultados = list(executor.map(mover_arquivo, [row for _, row in df_validacao.iterrows()]))
    
    # Resumo
    aprovados = sum(1 for r in resultados if r['destino'] == 'aprovados')
    quarentena = sum(1 for r in resultados if r['destino'] == 'quarentena')
    erros = sum(1 for r in resultados if r['status'] != 'sucesso')
    
    print("\n" + "="*60)
    print("📊 RESUMO DA ORGANIZAÇÃO")
    print("="*60)
    print(f"✅ Arquivos aprovados: {aprovados}")
    print(f"❌ Arquivos em quarentena: {quarentena}")
    if erros > 0:
        print(f"⚠️  Erros ao processar: {erros}")
    print("="*60 + "\n")
    
    return resultados

def parse_errors(x):
    """
    Faz parse da coluna errors do CSV, lidando com diferentes formatos.
    Suporta dict Python (com aspas simples) e JSON válido.
    """
    if pd.isna(x) or x == '{}' or x == {}:
        return {}
    if isinstance(x, dict):
        return x
    try:
        return json.loads(x)
    except (json.JSONDecodeError, TypeError):
        try:
            return ast.literal_eval(x)
        except (ValueError, SyntaxError):
            return {}


def buscar_ultimo_csv_erros(storage_client: storage.Client) -> tuple[str, str]:
    """
    Busca o CSV de erros mais recente no bucket data_load_errors.
    Retorna o nome do blob e o bucket.
    """
    bucket = storage_client.bucket('data_load_errors')
    blobs = list(bucket.list_blobs(prefix='load-audio_checks/'))
    
    if not blobs:
        return None, None
    
    # Ordena por tempo de criação e pega o mais recente
    blobs_sorted = sorted(blobs, key=lambda x: x.time_created, reverse=True)
    return blobs_sorted[0].name, 'data_load_errors'


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
    
    df_original = paths_metadata(listed_files)
    
    # Cria verificador de áudio
    audio_verifier = AudioVerification(
        storage_client='data-sourcing', 
        bucket_name='beatriz-temp',
        n_workers_parallel=3  # Reduzido de 20 para 3 para evitar OOM
    )
    
    # Executa validação e captura possíveis erros
    validation_error_occurred = False
    try:
        audio_verifier.verify_table(df=df_original, new_songs=True)
        print("\n✅ Validação concluída sem erros!")
    except VerificationError as e:
        print(f"\n⚠️  Erros detectados na validação: {str(e)}")
        validation_error_occurred = True
    
    # Prepara dataframe final com informações de erros
    df_final = df_original.copy()
    df_final['errors'] = [{} for _ in range(len(df_final))]  # Inicializa coluna errors vazia
    
    if validation_error_occurred:
        # Busca e lê o CSV de erros mais recente do bucket data_load_errors
        csv_erros_path, bucket_erros_name = buscar_ultimo_csv_erros(storage_client)
        if csv_erros_path:
            print(f"📄 Lendo CSV de erros: gs://{bucket_erros_name}/{csv_erros_path}")
            
            # Lê o CSV do bucket de erros
            bucket_erros = storage_client.bucket(bucket_erros_name)
            blob_origem = bucket_erros.blob(csv_erros_path)
            
            csv_content = blob_origem.download_as_text()
            df_erros = pd.read_csv(io.StringIO(csv_content))
            
            # Copia o CSV para o bucket de destino para manter histórico
            bucket_destino = storage_client.bucket(BUCKET_DESTINO)
            path_destino_csv = csv_erros_path  # Mantém o mesmo path load-audio_checks/...
            bucket_destino.copy_blob(blob_origem, bucket_destino, path_destino_csv)
            print(f"💾 CSV copiado para: gs://{BUCKET_DESTINO}/{path_destino_csv}")
            
            # Converte coluna errors de string para dict (suporta dict Python e JSON)
            if 'errors' in df_erros.columns:
                df_erros['errors'] = df_erros['errors'].apply(parse_errors)
            
            # Mescla informações de erros no dataframe original
            df_final = df_original.merge(
                df_erros[['file_id', 'errors']], 
                on='file_id', 
                how='left',
                suffixes=('', '_erros')
            )
            
            # Usa a coluna de erros do CSV se existir, senão mantém vazia
            if 'errors_erros' in df_final.columns:
                df_final['errors'] = df_final['errors_erros'].fillna({}).apply(
                    lambda x: x if isinstance(x, dict) else {}
                )
                df_final = df_final.drop(columns=['errors_erros'])
    
    # Organiza arquivos no bucket de destino
    organizar_arquivos_por_validacao(storage_client, df_final)


if __name__ == "__main__":
    run_pipeline()


