"""
Configuração central do sistema de validação e organização de datasets MIR.
Define o schema final esperado e regras de validação.
"""

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class AudioFormat(Enum):
    WAV = "wav"
    MP3 = "mp3"
    FLAC = "flac"
    OGG = "ogg"
    M4A = "m4a"


class MetadataType(Enum):
    JSON = "json"
    CSV = "csv"
    TXT = "txt"


@dataclass
class AudioSpec:
    """Especificação técnica do áudio final."""
    format: AudioFormat = AudioFormat.WAV
    sample_rate: int = 44100
    channels: int = 2  # 1=mono, 2=stereo
    bit_depth: int = 16
    min_duration_sec: float = 1.0
    max_duration_sec: Optional[float] = None  # None = sem limite


@dataclass
class DirectoryStructure:
    """
    Estrutura de diretórios do formato final.
    
    Exemplo de estrutura final:
    dataset_name/
    ├── audio/
    │   ├── train/
    │   ├── val/
    │   └── test/
    ├── metadata/
    │   ├── tracks.json
    │   └── splits.json
    └── annotations/
        ├── train/
        ├── val/
        └── test/
    """
    audio_dir: str = "audio"
    metadata_dir: str = "metadata"
    annotations_dir: str = "annotations"
    splits: list = field(default_factory=lambda: ["train", "val", "test"])
    


@dataclass
class NamingConvention:
    """Convenção de nomenclatura para arquivos."""
    # Padrão: {prefix}_{track_id}.{ext}
    # Ex: track_00001.wav
    prefix: str = "track"
    id_padding: int = 5  # Quantidade de zeros à esquerda (00001)
    separator: str = "_"
    lowercase: bool = True
    replace_spaces_with: str = "_"
    remove_special_chars: bool = True


@dataclass
class MetadataSchema:
    """Schema dos metadados esperados para cada track."""
    required_fields: list = field(default_factory=lambda: [
        "track_id",
        "original_filename",
        "duration_sec",
    ])
    optional_fields: list = field(default_factory=lambda: [
        "artist",
        "title",
        "album",
        "genre",
        "bpm",
        "key",
        "source_provider",
        "original_format",
        "processed_at",
    ])


@dataclass
class ValidationRules:
    """Regras de validação aplicadas na pipeline."""
    check_audio_integrity: bool = True
    check_duration_range: bool = True
    check_sample_rate: bool = True
    check_channels: bool = True
    check_bit_depth: bool = True
    check_metadata_completeness: bool = True
    allow_duplicates: bool = False
    duplicate_detection: str = "hash"  # "hash", "filename", "metadata"


@dataclass
class DatasetConfig:
    """Configuração completa do dataset."""
    name: str = "mir_dataset"
    version: str = "1.0.0"
    audio: AudioSpec = field(default_factory=AudioSpec)
    structure: DirectoryStructure = field(default_factory=DirectoryStructure)
    naming: NamingConvention = field(default_factory=NamingConvention)
    metadata: MetadataSchema = field(default_factory=MetadataSchema)
    validation: ValidationRules = field(default_factory=ValidationRules)


# Configuração padrão
DEFAULT_CONFIG = DatasetConfig()


# Extensões de áudio suportadas para detecção automática
SUPPORTED_AUDIO_EXTENSIONS = {'.wav', '.mp3', '.flac', '.ogg', '.m4a', '.aiff', '.aif'}

# Extensões de metadados suportadas
SUPPORTED_METADATA_EXTENSIONS = {'.json', '.csv', '.txt', '.yaml', '.yml'}

# Extensões de anotações comuns em MIR
SUPPORTED_ANNOTATION_EXTENSIONS = {
    '.json', '.csv', '.txt', '.lab', '.jams', 
    '.mid', '.midi', '.xml', '.musicxml'
}
