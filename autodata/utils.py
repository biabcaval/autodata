"""
Utilitários compartilhados para manipulação de arquivos.
"""

import os
import re
import json
import hashlib
from pathlib import Path
from datetime import datetime

from config import FORMATOS_ACEITOS


def calcular_hash(conteudo: bytes) -> str:
    """Calcula o hash SHA256 do conteúdo."""
    return hashlib.sha256(conteudo).hexdigest()


def calcular_hash_arquivo(filepath: str) -> str:
    """Calcula o hash SHA256 de um arquivo no disco."""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def normalizar_nome(nome_arquivo: str) -> str:
    """Normaliza nome do arquivo para snake_case."""
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


def validar_nome_snake_case(nome_arquivo: str) -> tuple[bool, list[str]]:
    """Valida se o nome está em snake_case."""
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


def save_json(data: dict, filepath: str | Path, indent: int = 2):
    """Salva dados em arquivo JSON."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False, default=str)


def load_json(filepath: str | Path) -> dict:
    """Carrega dados de arquivo JSON."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def ensure_dir(path: str | Path) -> Path:
    """Garante que diretório existe, criando se necessário."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_timestamp() -> str:
    """Retorna timestamp no formato ISO."""
    return datetime.now().isoformat()


def format_bytes(size: int) -> str:
    """Formata tamanho em bytes para formato legível."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"
