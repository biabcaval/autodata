"""
Utilitários compartilhados para processamento de áudio e manipulação de arquivos.
"""

import os
import re
import json
import hashlib
import subprocess
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, asdict
from datetime import datetime

from config import SUPPORTED_AUDIO_EXTENSIONS, SUPPORTED_METADATA_EXTENSIONS


@dataclass
class AudioInfo:
    """Informações extraídas de um arquivo de áudio."""
    filepath: str
    duration_sec: float
    sample_rate: int
    channels: int
    bit_depth: Optional[int]
    format: str
    codec: str
    file_size_bytes: int
    is_valid: bool
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


def get_audio_info(filepath: str) -> AudioInfo:
    """
    Extrai informações detalhadas de um arquivo de áudio usando FFprobe.
    """
    try:
        result = subprocess.run([
            'ffprobe', '-v', 'error',
            '-select_streams', 'a:0',
            '-show_entries', 'stream=sample_rate,channels,bits_per_sample,codec_name,duration',
            '-show_entries', 'format=duration,size,format_name',
            '-of', 'json',
            filepath
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            return AudioInfo(
                filepath=filepath,
                duration_sec=0,
                sample_rate=0,
                channels=0,
                bit_depth=None,
                format="",
                codec="",
                file_size_bytes=0,
                is_valid=False,
                error_message=result.stderr
            )
        
        data = json.loads(result.stdout)
        stream = data.get('streams', [{}])[0]
        fmt = data.get('format', {})
        
        duration = float(stream.get('duration', fmt.get('duration', 0)))
        
        return AudioInfo(
            filepath=filepath,
            duration_sec=duration,
            sample_rate=int(stream.get('sample_rate', 0)),
            channels=int(stream.get('channels', 0)),
            bit_depth=int(stream.get('bits_per_sample', 0)) or None,
            format=fmt.get('format_name', ''),
            codec=stream.get('codec_name', ''),
            file_size_bytes=int(fmt.get('size', 0)),
            is_valid=True
        )
        
    except subprocess.TimeoutExpired:
        return AudioInfo(
            filepath=filepath, duration_sec=0, sample_rate=0, channels=0,
            bit_depth=None, format="", codec="", file_size_bytes=0,
            is_valid=False, error_message="Timeout ao processar arquivo"
        )
    except Exception as e:
        return AudioInfo(
            filepath=filepath, duration_sec=0, sample_rate=0, channels=0,
            bit_depth=None, format="", codec="", file_size_bytes=0,
            is_valid=False, error_message=str(e)
        )


def verify_audio_integrity(filepath: str) -> tuple[bool, Optional[str]]:
    """
    Verifica se o arquivo de áudio pode ser decodificado completamente.
    Retorna (is_valid, error_message).
    """
    try:
        result = subprocess.run([
            'ffmpeg', '-v', 'error',
            '-i', filepath,
            '-f', 'null', '-'
        ], capture_output=True, text=True, timeout=120)
        
        if result.returncode != 0 or result.stderr.strip():
            return False, result.stderr.strip() or "Erro desconhecido na decodificação"
        return True, None
        
    except subprocess.TimeoutExpired:
        return False, "Timeout ao verificar integridade"
    except Exception as e:
        return False, str(e)


def compute_file_hash(filepath: str, algorithm: str = "md5") -> str:
    """Calcula hash do arquivo para detecção de duplicatas."""
    hash_func = hashlib.new(algorithm)
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def compute_audio_hash(filepath: str) -> Optional[str]:
    """
    Calcula hash do conteúdo de áudio (ignora metadados do container).
    Útil para detectar duplicatas mesmo com formatos/metadados diferentes.
    """
    try:
        result = subprocess.run([
            'ffmpeg', '-v', 'error',
            '-i', filepath,
            '-f', 'md5', '-'
        ], capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            match = re.search(r'MD5=([a-f0-9]+)', result.stdout)
            if match:
                return match.group(1)
        return None
    except Exception:
        return None


def normalize_filename(
    filename: str,
    lowercase: bool = True,
    replace_spaces: str = "_",
    remove_special: bool = True
) -> str:
    """Normaliza nome de arquivo seguindo convenção definida."""
    name = Path(filename).stem
    ext = Path(filename).suffix.lower()
    
    if lowercase:
        name = name.lower()
    
    if replace_spaces:
        name = name.replace(" ", replace_spaces)
    
    if remove_special:
        name = re.sub(r'[^\w\-]', '', name)
    
    name = re.sub(r'_+', '_', name).strip('_')
    
    return f"{name}{ext}"


def generate_track_id(index: int, padding: int = 5, prefix: str = "track") -> str:
    """Gera ID padronizado para track."""
    return f"{prefix}_{str(index).zfill(padding)}"


def find_files_by_extension(
    root_path: str | Path,
    extensions: set[str],
    recursive: bool = True
) -> list[Path]:
    """Encontra todos os arquivos com extensões especificadas."""
    root = Path(root_path)
    files = []
    
    pattern = "**/*" if recursive else "*"
    for path in root.glob(pattern):
        if path.is_file() and path.suffix.lower() in extensions:
            files.append(path)
    
    return sorted(files)


def find_audio_files(root_path: str | Path, recursive: bool = True) -> list[Path]:
    """Encontra todos os arquivos de áudio."""
    return find_files_by_extension(root_path, SUPPORTED_AUDIO_EXTENSIONS, recursive)


def find_metadata_files(root_path: str | Path, recursive: bool = True) -> list[Path]:
    """Encontra todos os arquivos de metadados."""
    return find_files_by_extension(root_path, SUPPORTED_METADATA_EXTENSIONS, recursive)


def load_metadata(filepath: str | Path) -> dict:
    """Carrega metadados de arquivo JSON, CSV ou TXT."""
    path = Path(filepath)
    
    if path.suffix.lower() == '.json':
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    elif path.suffix.lower() == '.csv':
        import csv
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return {"rows": list(reader)}
    
    elif path.suffix.lower() in {'.yaml', '.yml'}:
        try:
            import yaml
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except ImportError:
            raise ImportError("PyYAML não instalado. Instale com: pip install pyyaml")
    
    else:
        with open(path, 'r', encoding='utf-8') as f:
            return {"content": f.read()}


def save_json(data: dict, filepath: str | Path, indent: int = 2):
    """Salva dados em arquivo JSON."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False, default=str)


def ensure_dir(path: str | Path) -> Path:
    """Garante que diretório existe, criando se necessário."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_timestamp() -> str:
    """Retorna timestamp no formato ISO."""
    return datetime.now().isoformat()


def format_duration(seconds: float) -> str:
    """Formata duração em formato legível (HH:MM:SS.ms)."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:05.2f}"
    return f"{minutes:02d}:{secs:05.2f}"


def format_bytes(size: int) -> str:
    """Formata tamanho em bytes para formato legível."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"
