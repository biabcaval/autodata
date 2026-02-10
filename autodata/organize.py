"""
AutoData - Pipeline Async de Organização de Arquivos de Áudio.

Responsável por:
- Listar arquivos de áudio no bucket de origem
- Normalizar nomes para snake_case
- Transferir para bucket de destino com estrutura organizada
- Gerar manifest com os arquivos processados

Usa asyncio + gcloud-aio-storage para processamento paralelo eficiente.
"""

import os
import re
import json
import hashlib
import asyncio
from datetime import datetime

import pandas as pd
import aiohttp
from gcloud.aio.storage import Storage

from config import BUCKET_ORIGEM, BUCKET_DESTINO, FORMATOS_ACEITOS


# ============================================================================
# FUNÇÕES DE VALIDAÇÃO E NORMALIZAÇÃO (síncronas, CPU-bound)
# ============================================================================

"""Valida se o nome do arquivo está em snake_case sem caracteres especiais."""

def validar_nome_arquivo(nome_arquivo):
    nome_base = os.path.splitext(os.path.basename(nome_arquivo))[0]
    
    pattern = r'^[a-z0-9]+(_[a-z0-9]+)*$'
    erros = []
    
    if re.search(r'[áàãâéèêíìîóòôõúùûç]', nome_base.lower()):
        erros.append("contém acentos")
    
    if ' ' in nome_base:
        erros.append("contém espaços")
    
    if re.search(r'[!@#$%^&*()\[\]{};:\'",<>?/\\|`~=+]', nome_base):
        erros.append("contém caracteres especiais")
    
    if nome_base != nome_base.lower():
        erros.append("não está em minúsculas")
    
    if not re.match(pattern, nome_base.lower().replace('-', '_')):
        if not erros:
            erros.append("não segue padrão snake_case")
    
    return len(erros) == 0, erros

"""Normaliza nome do arquivo para snake_case."""
def normalizar_nome(nome_arquivo):
    nome_base = os.path.splitext(os.path.basename(nome_arquivo))[0]
    extensao = os.path.splitext(nome_arquivo)[1].lower()
    
    acentos = {
        'á': 'a', 'à': 'a', 'ã': 'a', 'â': 'a',
        'é': 'e', 'è': 'e', 'ê': 'e',
        'í': 'i', 'ì': 'i', 'î': 'i',
        'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u',
        'ç': 'c'
    }
    for acento, sem_acento in acentos.items():
        nome_base = nome_base.replace(acento, sem_acento)
        nome_base = nome_base.replace(acento.upper(), sem_acento)
    
    nome_base = nome_base.lower()
    nome_base = nome_base.replace(' ', '_').replace('-', '_')
    nome_base = re.sub(r'[^a-z0-9_]', '', nome_base)
    nome_base = re.sub(r'_+', '_', nome_base)
    nome_base = nome_base.strip('_')
    
    return f"{nome_base}{extensao}"

"""Calcula o hash SHA256 do conteúdo em memória."""
def calcular_hash(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()

"""Extrai o blob name de um path GCS (remove prefixo gs://bucket/)."""
def extrair_blob_name(path: str, bucket_name: str) -> str:
    prefixo = f'gs://{bucket_name}/'
    if path.startswith(prefixo):
        return path[len(prefixo):]
    return path

"""Extrai o identificador da track/música a partir do path."""
def extrair_track_id(path: str) -> str:
    parts = path.split('/')
    
    # Verifica se é um stem (pasta pai termina com -stem)
    for part in parts:
        if part.endswith('-stem'):
            song_name = part[:-5]  # Remove sufixo '-stem'
            return normalizar_nome(song_name).replace('.', '_')
    
    # Se não é stem, usa o nome do arquivo sem extensão
    filename = os.path.splitext(os.path.basename(path))[0]
    return normalizar_nome(filename).replace('.', '_')

"""Detecta o tipo de dataset baseado no path."""
def detectar_dataset_type(path: str) -> str:
    path_lower = path.lower()
    if '-stem' in path_lower or 'multistem' in path_lower:
        return 'multistem'
    elif 'vocal' in path_lower:
        return 'vocalset'
    elif 'mir' in path_lower or 'chord' in path_lower:
        return 'mir'
    return 'audio'

"""Calcula duração do áudio em segundos a partir do conteúdo."""
def calcular_duracao(conteudo: bytes, extensao: str) -> float:
    try:
        import io
        from mutagen import File as MutagenFile
        from mutagen.wave import WAVE
        from mutagen.mp3 import MP3
        from mutagen.flac import FLAC
        from mutagen.mp4 import MP4
        
        audio_file = io.BytesIO(conteudo)
        
        ext = extensao.lower()
        if ext == '.wav':
            audio = WAVE(audio_file)
        elif ext == '.mp3':
            audio = MP3(audio_file)
        elif ext == '.flac':
            audio = FLAC(audio_file)
        elif ext in ['.m4a', '.mp4']:
            audio = MP4(audio_file)
        else:
            audio = MutagenFile(audio_file)
        
        if audio and audio.info:
            return round(audio.info.length, 2)
    except Exception as e:
        print(f"⚠️  Não foi possível calcular duração: {e}")
    return 0.0


# ============================================================================
# FUNÇÕES ASYNC (I/O-bound)
# ============================================================================

"""Lista todos os arquivos de áudio no bucket de forma async."""
async def listar_arquivos_async(storage: Storage, bucket_name: str) -> list[str]:
    csv_path = os.path.join(os.getcwd(), "paths_metadata.csv")
    print(f"📖 Reading CSV from: {csv_path}")
    
    df = pd.read_csv(csv_path)
    arquivos = [i for i in df['path']]
    
    return arquivos

"""Aplica as checagens e/ou processamentos necessarios a um arquivo"""
async def processar_arquivo_async(
    storage: Storage,
    nome_arquivo: str,
    hashes_processados: set,
    semaphore: asyncio.Semaphore
) -> dict:
    """
    Processa um único arquivo de forma async.
    
    - Baixa o conteúdo para memória
    - Valida/normaliza nome
    - Verifica duplicatas via hash
    - Faz upload para bucket destino
    """
    async with semaphore:  # Limita concorrência
        resultado = {
            "arquivo_original": nome_arquivo,
            "checagens": {},
            "status": "pendente",
            "erros": []
        }
        
        try:
            # Extrai blob name (remove gs://bucket/ se presente)
            blob_name = extrair_blob_name(nome_arquivo, BUCKET_ORIGEM)
            print(f"📥 Baixando: {blob_name}")
            
            # Download async para memória (não usa /tmp)
            conteudo = await storage.download(BUCKET_ORIGEM, blob_name)
            
            # Extrai informações do arquivo
            extensao = os.path.splitext(nome_arquivo)[1].lower()
            track_id = extrair_track_id(blob_name)
            dataset_type = detectar_dataset_type(blob_name)
            
            # Checagem 1: Nomenclatura
            nome_valido, erros_nome = validar_nome_arquivo(nome_arquivo)
            resultado["checagens"]["nomenclatura"] = {
                "valido": nome_valido,
                "detalhes": "snake_case válido" if nome_valido else f"Problemas: {', '.join(erros_nome)}"
            }
            
            # Checagem 2: Hash / Duplicação
            hash_arquivo = calcular_hash(conteudo)
            is_duplicado = hash_arquivo in hashes_processados
            
            resultado["checagens"]["hash"] = {
                "valido": not is_duplicado,
                "sha256": hash_arquivo,
                "detalhes": "duplicado encontrado" if is_duplicado else "arquivo único"
            }
            
            if is_duplicado:
                print(f"⏭️  Ignorado (duplicado): {nome_arquivo}")
                resultado["erros"].append("Arquivo duplicado")
                resultado["status"] = "ignorado_duplicado"
                return resultado
            
            # Adiciona hash ao set (thread-safe no asyncio single-thread)
            hashes_processados.add(hash_arquivo)
            
            # Calcula duração do áudio
            duracao_seconds = calcular_duracao(conteudo, extensao)
            
            # Normaliza nome
            nome_normalizado = normalizar_nome(nome_arquivo)
            resultado["nome_normalizado"] = nome_normalizado
            
            # Upload async para bucket destino
            nome_saida = f"audio/{nome_normalizado}"
            await storage.upload(BUCKET_DESTINO, nome_saida, conteudo)
            
            print(f"📤 Upload: {nome_saida}")
            
            # Monta resultado final
            resultado["arquivo_saida"] = nome_saida
            resultado["hash_sha256"] = hash_arquivo
            resultado["data_processamento"] = datetime.now().isoformat()
            resultado["status"] = "sucesso"
            
            # Informações para o manifest (formato padrão)
            resultado["track_id"] = track_id
            resultado["dataset_type"] = dataset_type
            resultado["duration_seconds"] = duracao_seconds
            resultado["folder_path"] = nome_saida
            
            # Upload metadados na pasta da track: metadata/{track_id}/data.json
            metadata_path = f"metadata/{track_id}/data.json"
            meta_json = json.dumps(resultado, indent=2, ensure_ascii=False).encode('utf-8')
            await storage.upload(BUCKET_DESTINO, metadata_path, meta_json)
            print(f"📝 Metadata: {metadata_path}")
            
            return resultado
            
        except Exception as e:
            print(f"❌ Erro em {nome_arquivo}: {e}")
            resultado["status"] = "falha"
            resultado["erros"].append(str(e))
            return resultado

"""Gera o arquivo manifest.csv com todos os arquivos processados (formato padrão)."""
async def gerar_manifest_async(storage: Storage, resultados: list[dict]):
    # Colunas obrigatórias conforme standard.md
    linhas = ["folder_path,dataset_type,id,duration_seconds"]
    
    for r in resultados:
        if r["status"] == "sucesso":
            folder_path = r.get('folder_path', '')
            dataset_type = r.get('dataset_type', 'audio')
            track_id = r.get('track_id', '')
            duration_seconds = r.get('duration_seconds', 0.0)
            
            linha = f"{folder_path},{dataset_type},{track_id},{duration_seconds}"
            linhas.append(linha)
    
    conteudo = "\n".join(linhas)
    
    await storage.upload(
        BUCKET_DESTINO, 
        "manifest.csv", 
        conteudo.encode('utf-8'),
        content_type="text/csv"
    )
    
    print(f"\n📋 Manifest gerado: manifest.csv ({len(linhas)-1} entradas)")


# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

async def organize():
    """Executa a pipeline async de processamento."""
    
    # Info do Cloud Run Job
    task_index = int(os.environ.get('CLOUD_RUN_TASK_INDEX', 
                     os.environ.get('BATCH_TASK_INDEX', 0)))
    task_count = int(os.environ.get('CLOUD_RUN_TASK_COUNT', 
                     os.environ.get('BATCH_TASK_COUNT', 1)))
    
    print(f"🚀 Task {task_index + 1} de {task_count}")
    print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📥 Bucket origem: {BUCKET_ORIGEM}")
    print(f"📤 Bucket destino: {BUCKET_DESTINO}")
    
    # Cria sessão HTTP async (reutilizada por todas as operações)
    async with aiohttp.ClientSession() as session:
        storage = Storage(session=session)
        
        # Lista arquivos
        print("\n📂 Listando arquivos...")
        todos_arquivos = await listar_arquivos_async(storage, BUCKET_ORIGEM)
        print(f"📁 Total de arquivos no bucket: {len(todos_arquivos)}")
        
        # Divide arquivos entre tasks do Cloud Run Job
        meus_arquivos = [
            arquivo for i, arquivo in enumerate(todos_arquivos)
            if i % task_count == task_index
        ]
        print(f"📋 Esta task vai processar: {len(meus_arquivos)} arquivos")
        
        if not meus_arquivos:
            print("⚠️  Nenhum arquivo para processar nesta task.")
            return
        
        # Configura limite de concorrência (evita sobrecarregar memória/rede)
        max_concurrent = int(os.environ.get('MAX_CONCURRENT', 10))
        semaphore = asyncio.Semaphore(max_concurrent)
        print(f"⚡ Concorrência máxima: {max_concurrent}")
        
        # Set para rastrear hashes (detectar duplicatas)
        hashes_processados = set()
        
        # Cria tasks async para todos os arquivos
        print("\n" + "─"*60)
        print("🔄 Processando arquivos...")
        print("─"*60)
        
        tasks = [
            processar_arquivo_async(storage, arq, hashes_processados, semaphore)
            for arq in meus_arquivos
        ]
        
        # Executa todas as tasks em paralelo
        resultados = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Trata exceções retornadas pelo gather
        resultados_limpos = []
        for r in resultados:
            if isinstance(r, Exception):
                resultados_limpos.append({"status": "falha", "erros": [str(r)]})
            else:
                resultados_limpos.append(r)
        
        # Gera manifest (apenas task 0)
        if task_index == 0:
            await gerar_manifest_async(storage, resultados_limpos)
        
        # Contabiliza resultados
        sucessos = sum(1 for r in resultados_limpos if r["status"] == "sucesso")
        ignorados = sum(1 for r in resultados_limpos if r["status"] == "ignorado_duplicado")
        falhas = sum(1 for r in resultados_limpos if r["status"] == "falha")
        
        # Resumo final
        print("\n" + "═"*60)
        print(f"📊 RESUMO FINAL - Task {task_index + 1}")
        print("═"*60)
        print(f"   ✅ Sucessos: {sucessos}")
        print(f"   ⏭️  Ignorados (duplicados): {ignorados}")
        print(f"   ❌ Falhas: {falhas}")
        print(f"   📁 Total processado: {len(meus_arquivos)}")
        print("═"*60 + "\n")


