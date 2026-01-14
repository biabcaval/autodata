"""
Pipeline de Organização de Datasets MIR.

Responsável por:
- Transformar datasets de qualquer formato para o padrão final
- Converter áudios para especificação técnica definida
- Renomear arquivos seguindo convenção de nomenclatura
- Organizar estrutura de diretórios
- Gerar metadados padronizados
- Criar splits train/val/test se necessário
"""

import os
import shutil
import subprocess
import random
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    DatasetConfig, DEFAULT_CONFIG, AudioFormat,
    SUPPORTED_AUDIO_EXTENSIONS
)
from utils import (
    get_audio_info, find_audio_files, find_metadata_files,
    load_metadata, save_json, ensure_dir, get_timestamp,
    generate_track_id, normalize_filename, compute_file_hash,
    format_duration, format_bytes, AudioInfo
)
from validation_pipeline import ValidationPipeline, ValidationReport


@dataclass
class TrackMapping:
    """Mapeamento de um track do formato original para o final."""
    original_path: str
    original_filename: str
    new_track_id: str
    new_filename: str
    new_path: str
    audio_info: Optional[AudioInfo] = None
    needs_conversion: bool = False
    conversion_done: bool = False
    error: Optional[str] = None


@dataclass
class OrganizationResult:
    """Resultado da organização do dataset."""
    source_path: str
    output_path: str
    organized_at: str
    config_used: dict = field(default_factory=dict)
    
    total_tracks: int = 0
    successfully_processed: int = 0
    failed: int = 0
    skipped_invalid: int = 0
    
    tracks_needing_conversion: int = 0
    total_output_size_bytes: int = 0
    total_duration_sec: float = 0
    
    split_distribution: dict = field(default_factory=dict)
    track_mappings: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    
    def to_dict(self) -> dict:
        d = asdict(self)
        d['track_mappings'] = [asdict(m) for m in self.track_mappings]
        return d


class OrganizationPipeline:
    """Pipeline para organização de datasets MIR."""
    
    def __init__(self, config: DatasetConfig = None):
        self.config = config or DEFAULT_CONFIG
        self.validator = ValidationPipeline(self.config)
    
    def convert_audio(
        self,
        input_path: str,
        output_path: str,
        target_format: AudioFormat = None,
        target_sample_rate: int = None,
        target_channels: int = None,
        target_bit_depth: int = None
    ) -> tuple[bool, Optional[str]]:
        """
        Converte arquivo de áudio para especificações definidas.
        
        Returns:
            (success, error_message)
        """
        config = self.config.audio
        target_format = target_format or config.format
        target_sample_rate = target_sample_rate or config.sample_rate
        target_channels = target_channels or config.channels
        target_bit_depth = target_bit_depth or config.bit_depth
        
        # Mapeia formato para codec FFmpeg
        codec_map = {
            AudioFormat.WAV: ('pcm_s16le' if target_bit_depth == 16 else 'pcm_s24le', 'wav'),
            AudioFormat.MP3: ('libmp3lame', 'mp3'),
            AudioFormat.FLAC: ('flac', 'flac'),
            AudioFormat.OGG: ('libvorbis', 'ogg'),
            AudioFormat.M4A: ('aac', 'ipod'),
        }
        
        codec, container = codec_map.get(target_format, ('pcm_s16le', 'wav'))
        
        cmd = [
            'ffmpeg', '-y', '-v', 'error',
            '-i', input_path,
            '-ar', str(target_sample_rate),
            '-ac', str(target_channels),
            '-c:a', codec,
            '-f', container,
            output_path
        ]
        
        # Adiciona bit depth para WAV
        if target_format == AudioFormat.WAV:
            sample_fmt = 's16' if target_bit_depth == 16 else 's32'
            cmd.insert(-2, '-sample_fmt')
            cmd.insert(-2, sample_fmt)
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                return False, result.stderr
            return True, None
        except subprocess.TimeoutExpired:
            return False, "Timeout na conversão"
        except Exception as e:
            return False, str(e)
    
    def needs_conversion(self, audio_info: AudioInfo) -> bool:
        """Verifica se o arquivo precisa ser convertido."""
        config = self.config.audio
        
        # Verifica formato
        target_ext = f".{config.format.value}"
        if not audio_info.filepath.lower().endswith(target_ext):
            return True
        
        # Verifica sample rate
        if audio_info.sample_rate != config.sample_rate:
            return True
        
        # Verifica canais
        if audio_info.channels != config.channels:
            return True
        
        # Verifica bit depth (se disponível)
        if audio_info.bit_depth and audio_info.bit_depth != config.bit_depth:
            return True
        
        return False
    
    def create_directory_structure(self, output_path: Path) -> dict[str, Path]:
        """Cria estrutura de diretórios do formato final."""
        structure = self.config.structure
        dirs = {}
        
        # Diretório base
        ensure_dir(output_path)
        
        # Diretório de áudio
        audio_base = output_path / structure.audio_dir
        dirs['audio'] = ensure_dir(audio_base)
        
        # Diretório de metadados
        dirs['metadata'] = ensure_dir(output_path / structure.metadata_dir)
        
        # Diretório de anotações
        dirs['annotations'] = ensure_dir(output_path / structure.annotations_dir)
        
        # Subdiretórios de splits
        if structure.use_splits_subdirs:
            for split in structure.splits:
                dirs[f'audio_{split}'] = ensure_dir(audio_base / split)
                dirs[f'annotations_{split}'] = ensure_dir(
                    output_path / structure.annotations_dir / split
                )
        
        return dirs
    
    def assign_splits(
        self,
        num_tracks: int,
        train_ratio: float = 0.8,
        val_ratio: float = 0.1,
        test_ratio: float = 0.1,
        seed: int = 42
    ) -> list[str]:
        """
        Atribui splits aos tracks de forma aleatória mas reproduzível.
        
        Returns:
            Lista com o split de cada track ['train', 'val', 'test', ...]
        """
        random.seed(seed)
        
        indices = list(range(num_tracks))
        random.shuffle(indices)
        
        n_train = int(num_tracks * train_ratio)
        n_val = int(num_tracks * val_ratio)
        
        splits = [''] * num_tracks
        for i, idx in enumerate(indices):
            if i < n_train:
                splits[idx] = 'train'
            elif i < n_train + n_val:
                splits[idx] = 'val'
            else:
                splits[idx] = 'test'
        
        return splits
    
    def process_track(
        self,
        mapping: TrackMapping,
        dirs: dict[str, Path],
        split: str = None
    ) -> TrackMapping:
        """Processa um único track (copia ou converte)."""
        try:
            # Determina diretório de destino
            if split and f'audio_{split}' in dirs:
                dest_dir = dirs[f'audio_{split}']
            else:
                dest_dir = dirs['audio']
            
            output_path = dest_dir / mapping.new_filename
            mapping.new_path = str(output_path)
            
            if mapping.needs_conversion:
                # Converte
                success, error = self.convert_audio(
                    mapping.original_path,
                    str(output_path)
                )
                if not success:
                    mapping.error = error
                    return mapping
                mapping.conversion_done = True
            else:
                # Apenas copia
                shutil.copy2(mapping.original_path, output_path)
            
            return mapping
            
        except Exception as e:
            mapping.error = str(e)
            return mapping
    
    def organize_dataset(
        self,
        source_path: str | Path,
        output_path: str | Path,
        validate_first: bool = True,
        skip_invalid: bool = True,
        create_splits: bool = True,
        max_workers: int = 4,
        progress_callback: callable = None
    ) -> OrganizationResult:
        """
        Organiza dataset para o formato final padronizado.
        
        Args:
            source_path: Caminho do dataset de origem
            output_path: Caminho para o dataset organizado
            validate_first: Se deve validar antes de organizar
            skip_invalid: Se deve pular arquivos inválidos
            create_splits: Se deve criar splits train/val/test
            max_workers: Workers para processamento paralelo
            progress_callback: Callback de progresso (current, total)
        
        Returns:
            OrganizationResult com resultados da organização
        """
        source = Path(source_path)
        output = Path(output_path)
        
        result = OrganizationResult(
            source_path=str(source),
            output_path=str(output),
            organized_at=get_timestamp(),
            config_used=asdict(self.config)
        )
        
        print(f"🚀 Iniciando organização de: {source}")
        print(f"📂 Destino: {output}")
        
        # 1. Validação opcional
        validation_report = None
        valid_files = set()
        
        if validate_first:
            print("\n📋 Executando validação prévia...")
            validation_report = self.validator.validate_dataset(
                source,
                check_integrity=True,
                max_workers=max_workers
            )
            
            for file_result in validation_report.file_results:
                if file_result.is_valid:
                    valid_files.add(file_result.filepath)
                else:
                    result.skipped_invalid += 1
        
        # 2. Encontra arquivos de áudio
        audio_files = find_audio_files(source)
        
        if validate_first and skip_invalid:
            audio_files = [f for f in audio_files if str(f) in valid_files]
        
        result.total_tracks = len(audio_files)
        print(f"\n🎵 {result.total_tracks} tracks para processar")
        
        if result.total_tracks == 0:
            print("⚠️ Nenhum track para processar!")
            return result
        
        # 3. Cria estrutura de diretórios
        print("📁 Criando estrutura de diretórios...")
        dirs = self.create_directory_structure(output)
        
        # 4. Atribui splits
        splits = None
        if create_splits and self.config.structure.use_splits_subdirs:
            splits = self.assign_splits(len(audio_files))
            split_counts = {s: splits.count(s) for s in set(splits)}
            result.split_distribution = split_counts
            print(f"📊 Distribuição de splits: {split_counts}")
        
        # 5. Cria mapeamentos
        print("🔗 Criando mapeamentos de tracks...")
        mappings = []
        naming = self.config.naming
        target_ext = f".{self.config.audio.format.value}"
        
        for i, filepath in enumerate(audio_files):
            track_id = generate_track_id(
                i + 1, 
                padding=naming.id_padding,
                prefix=naming.prefix
            )
            new_filename = f"{track_id}{target_ext}"
            
            audio_info = get_audio_info(str(filepath))
            needs_conv = self.needs_conversion(audio_info) if audio_info.is_valid else True
            
            mapping = TrackMapping(
                original_path=str(filepath),
                original_filename=filepath.name,
                new_track_id=track_id,
                new_filename=new_filename,
                new_path="",  # Será preenchido durante processamento
                audio_info=audio_info,
                needs_conversion=needs_conv
            )
            mappings.append(mapping)
            
            if needs_conv:
                result.tracks_needing_conversion += 1
        
        print(f"🔄 {result.tracks_needing_conversion} tracks precisam de conversão")
        
        # 6. Processa tracks em paralelo
        print(f"\n⚙️ Processando tracks (workers={max_workers})...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for i, mapping in enumerate(mappings):
                split = splits[i] if splits else None
                future = executor.submit(self.process_track, mapping, dirs, split)
                futures[future] = i
            
            for i, future in enumerate(as_completed(futures)):
                idx = futures[future]
                processed_mapping = future.result()
                mappings[idx] = processed_mapping
                
                if processed_mapping.error:
                    result.failed += 1
                    result.errors.append({
                        'track': processed_mapping.original_filename,
                        'error': processed_mapping.error
                    })
                else:
                    result.successfully_processed += 1
                    if processed_mapping.audio_info:
                        result.total_duration_sec += processed_mapping.audio_info.duration_sec
                
                if progress_callback:
                    progress_callback(i + 1, len(mappings))
                
                if (i + 1) % 50 == 0 or (i + 1) == len(mappings):
                    print(f"   Progresso: {i + 1}/{len(mappings)}")
        
        result.track_mappings = mappings
        
        # 7. Gera metadados
        print("\n📝 Gerando metadados...")
        self._generate_metadata(output, dirs, mappings, splits, result)
        
        # 8. Calcula tamanho final
        result.total_output_size_bytes = sum(
            os.path.getsize(m.new_path) 
            for m in mappings 
            if m.new_path and os.path.exists(m.new_path)
        )
        
        # 9. Imprime resumo
        self._print_summary(result)
        
        return result
    
    def _generate_metadata(
        self,
        output_path: Path,
        dirs: dict,
        mappings: list[TrackMapping],
        splits: list[str],
        result: OrganizationResult
    ):
        """Gera arquivos de metadados do dataset organizado."""
        metadata_dir = dirs['metadata']
        
        # tracks.json - informações de cada track
        tracks_data = []
        for i, mapping in enumerate(mappings):
            if mapping.error:
                continue
            
            track_info = {
                'track_id': mapping.new_track_id,
                'filename': mapping.new_filename,
                'original_filename': mapping.original_filename,
                'split': splits[i] if splits else None,
            }
            
            if mapping.audio_info:
                track_info.update({
                    'duration_sec': round(mapping.audio_info.duration_sec, 3),
                    'sample_rate': mapping.audio_info.sample_rate,
                    'channels': mapping.audio_info.channels,
                    'original_format': mapping.audio_info.format,
                    'was_converted': mapping.conversion_done
                })
            
            tracks_data.append(track_info)
        
        save_json({'tracks': tracks_data}, metadata_dir / 'tracks.json')
        
        # splits.json - distribuição de splits
        if splits:
            splits_data = {
                'train': [m.new_track_id for i, m in enumerate(mappings) 
                         if splits[i] == 'train' and not m.error],
                'val': [m.new_track_id for i, m in enumerate(mappings) 
                       if splits[i] == 'val' and not m.error],
                'test': [m.new_track_id for i, m in enumerate(mappings) 
                        if splits[i] == 'test' and not m.error],
            }
            save_json(splits_data, metadata_dir / 'splits.json')
        
        # dataset_info.json - informações gerais
        dataset_info = {
            'name': self.config.name,
            'version': self.config.version,
            'organized_at': result.organized_at,
            'source_path': result.source_path,
            'total_tracks': result.successfully_processed,
            'total_duration_sec': round(result.total_duration_sec, 2),
            'total_size_bytes': result.total_output_size_bytes,
            'audio_spec': {
                'format': self.config.audio.format.value,
                'sample_rate': self.config.audio.sample_rate,
                'channels': self.config.audio.channels,
                'bit_depth': self.config.audio.bit_depth
            },
            'split_distribution': result.split_distribution
        }
        save_json(dataset_info, metadata_dir / 'dataset_info.json')
        
        print(f"   ✓ tracks.json ({len(tracks_data)} tracks)")
        print(f"   ✓ splits.json")
        print(f"   ✓ dataset_info.json")
    
    def _print_summary(self, result: OrganizationResult):
        """Imprime resumo da organização."""
        print(f"\n{'='*60}")
        print("📋 RESULTADO DA ORGANIZAÇÃO")
        print(f"{'='*60}")
        
        print(f"\n📊 Estatísticas:")
        print(f"   Total de tracks: {result.total_tracks}")
        print(f"   Processados com sucesso: {result.successfully_processed} ✓")
        print(f"   Falhas: {result.failed} ✗")
        print(f"   Ignorados (inválidos): {result.skipped_invalid}")
        
        print(f"\n🔄 Conversões:")
        print(f"   Tracks convertidos: {result.tracks_needing_conversion}")
        
        print(f"\n⏱️ Duração Total: {format_duration(result.total_duration_sec)}")
        print(f"💾 Tamanho Final: {format_bytes(result.total_output_size_bytes)}")
        
        if result.split_distribution:
            print(f"\n📊 Distribuição de Splits:")
            for split, count in result.split_distribution.items():
                print(f"   {split}: {count}")
        
        if result.errors:
            print(f"\n⚠️ Erros ({len(result.errors)}):")
            for err in result.errors[:5]:
                print(f"   - {err['track']}: {err['error'][:50]}...")
            if len(result.errors) > 5:
                print(f"   ... e mais {len(result.errors) - 5} erros")
        
        success_rate = (result.successfully_processed / result.total_tracks * 100 
                       if result.total_tracks > 0 else 0)
        print(f"\n{'='*60}")
        print(f"✅ Taxa de Sucesso: {success_rate:.1f}%")
        print(f"📂 Dataset organizado em: {result.output_path}")
        print(f"{'='*60}\n")
    
    def save_result(self, result: OrganizationResult, output_path: str | Path):
        """Salva resultado da organização em JSON."""
        save_json(result.to_dict(), output_path)
        print(f"💾 Resultado salvo em: {output_path}")


def main():
    """Função principal para execução via CLI."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Pipeline de Organização de Datasets MIR"
    )
    parser.add_argument(
        "source_path",
        help="Caminho para o dataset de origem"
    )
    parser.add_argument(
        "output_path",
        help="Caminho para o dataset organizado"
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Pular validação prévia"
    )
    parser.add_argument(
        "--include-invalid",
        action="store_true",
        help="Incluir arquivos inválidos (tentar processar mesmo assim)"
    )
    parser.add_argument(
        "--no-splits",
        action="store_true",
        help="Não criar splits train/val/test"
    )
    parser.add_argument(
        "-w", "--workers",
        type=int,
        default=4,
        help="Número de workers paralelos (padrão: 4)"
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Nome do dataset"
    )
    
    args = parser.parse_args()
    
    config = DEFAULT_CONFIG
    if args.name:
        config.name = args.name
    
    pipeline = OrganizationPipeline(config)
    result = pipeline.organize_dataset(
        args.source_path,
        args.output_path,
        validate_first=not args.no_validate,
        skip_invalid=not args.include_invalid,
        create_splits=not args.no_splits,
        max_workers=args.workers
    )
    
    # Salva resultado no diretório de saída
    result_path = Path(args.output_path) / "organization_result.json"
    pipeline.save_result(result, result_path)


if __name__ == "__main__":
    main()
