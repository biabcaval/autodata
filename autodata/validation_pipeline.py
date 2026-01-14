"""
Pipeline de Validação de Datasets MIR.

Responsável por:
- Detectar automaticamente a estrutura/padrão de datasets de entrada
- Validar integridade dos arquivos de áudio
- Validar metadados e anotações
- Gerar relatório completo de validação
- Identificar problemas e incompatibilidades
"""

import os
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    DatasetConfig, DEFAULT_CONFIG,
    SUPPORTED_AUDIO_EXTENSIONS, 
    SUPPORTED_METADATA_EXTENSIONS,
    SUPPORTED_ANNOTATION_EXTENSIONS
)
from utils import (
    get_audio_info, verify_audio_integrity, compute_file_hash,
    compute_audio_hash, find_audio_files, find_metadata_files,
    load_metadata, save_json, get_timestamp, format_duration, format_bytes,
    AudioInfo
)


@dataclass
class DetectedStructure:
    """Estrutura detectada do dataset de entrada."""
    root_path: str
    total_dirs: int = 0
    depth_levels: int = 0
    dir_tree: dict = field(default_factory=dict)
    has_split_dirs: bool = False
    detected_splits: list = field(default_factory=list)
    has_audio_subdir: bool = False
    has_metadata_subdir: bool = False
    has_annotations_subdir: bool = False
    naming_patterns: list = field(default_factory=list)
    common_prefixes: list = field(default_factory=list)


@dataclass 
class AudioValidationResult:
    """Resultado da validação de um arquivo de áudio."""
    filepath: str
    filename: str
    is_valid: bool
    audio_info: Optional[AudioInfo] = None
    integrity_ok: bool = True
    duration_ok: bool = True
    sample_rate_ok: bool = True
    channels_ok: bool = True
    file_hash: Optional[str] = None
    audio_hash: Optional[str] = None
    issues: list = field(default_factory=list)


@dataclass
class ValidationReport:
    """Relatório completo de validação do dataset."""
    dataset_path: str
    validated_at: str
    detected_structure: DetectedStructure = None
    
    # Estatísticas de arquivos
    total_audio_files: int = 0
    valid_audio_files: int = 0
    invalid_audio_files: int = 0
    total_metadata_files: int = 0
    total_annotation_files: int = 0
    
    # Estatísticas de áudio
    total_duration_sec: float = 0
    avg_duration_sec: float = 0
    min_duration_sec: float = 0
    max_duration_sec: float = 0
    total_size_bytes: int = 0
    
    # Distribuições
    format_distribution: dict = field(default_factory=dict)
    sample_rate_distribution: dict = field(default_factory=dict)
    channels_distribution: dict = field(default_factory=dict)
    
    # Problemas encontrados
    issues_summary: dict = field(default_factory=dict)
    duplicates_found: list = field(default_factory=list)
    
    # Resultados detalhados
    file_results: list = field(default_factory=list)
    
    def to_dict(self) -> dict:
        d = asdict(self)
        if self.detected_structure:
            d['detected_structure'] = asdict(self.detected_structure)
        d['file_results'] = [asdict(r) for r in self.file_results]
        return d


class ValidationPipeline:
    """Pipeline para validação de datasets MIR."""
    
    def __init__(self, config: DatasetConfig = None):
        self.config = config or DEFAULT_CONFIG
    
    def detect_structure(self, dataset_path: str | Path) -> DetectedStructure:
        """
        Detecta automaticamente a estrutura do dataset de entrada.
        Analisa diretórios, padrões de nomenclatura e organização.
        """
        root = Path(dataset_path)
        structure = DetectedStructure(root_path=str(root))
        
        # Analisa árvore de diretórios
        all_dirs = []
        max_depth = 0
        dir_tree = {}
        
        for dirpath, dirnames, filenames in os.walk(root):
            rel_path = Path(dirpath).relative_to(root)
            depth = len(rel_path.parts)
            max_depth = max(max_depth, depth)
            all_dirs.append(str(rel_path))
            
            # Constrói árvore
            current = dir_tree
            for part in rel_path.parts:
                if part not in current:
                    current[part] = {}
                current = current[part]
        
        structure.total_dirs = len(all_dirs)
        structure.depth_levels = max_depth
        structure.dir_tree = dir_tree
        
        # Detecta padrões comuns
        dir_names_lower = [d.lower() for d in all_dirs]
        
        # Splits (train/val/test)
        split_keywords = {'train', 'training', 'val', 'valid', 'validation', 
                         'test', 'testing', 'dev', 'eval'}
        detected_splits = []
        for d in all_dirs:
            d_lower = d.lower()
            for keyword in split_keywords:
                if keyword in d_lower:
                    detected_splits.append(d)
                    break
        
        structure.has_split_dirs = len(detected_splits) > 0
        structure.detected_splits = detected_splits
        
        # Subdiretórios de áudio/metadados/anotações
        audio_keywords = {'audio', 'wav', 'wavs', 'mp3', 'music', 'songs', 'tracks'}
        metadata_keywords = {'metadata', 'meta', 'info', 'labels'}
        annotation_keywords = {'annotations', 'annotation', 'labels', 'transcriptions'}
        
        structure.has_audio_subdir = any(
            any(kw in d.lower() for kw in audio_keywords) 
            for d in all_dirs
        )
        structure.has_metadata_subdir = any(
            any(kw in d.lower() for kw in metadata_keywords) 
            for d in all_dirs
        )
        structure.has_annotations_subdir = any(
            any(kw in d.lower() for kw in annotation_keywords) 
            for d in all_dirs
        )
        
        # Detecta padrões de nomenclatura dos arquivos de áudio
        audio_files = find_audio_files(root)
        if audio_files:
            patterns = self._detect_naming_patterns(audio_files)
            structure.naming_patterns = patterns['patterns']
            structure.common_prefixes = patterns['prefixes']
        
        return structure
    
    def _detect_naming_patterns(self, files: list[Path]) -> dict:
        """Analisa padrões de nomenclatura dos arquivos."""
        import re
        
        patterns = []
        prefixes = Counter()
        
        for f in files[:100]:  # Amostra de 100 arquivos
            name = f.stem
            
            # Extrai prefixo (antes do primeiro número ou separador)
            prefix_match = re.match(r'^([a-zA-Z]+)', name)
            if prefix_match:
                prefixes[prefix_match.group(1).lower()] += 1
            
            # Detecta padrões comuns
            if re.match(r'^[a-zA-Z]+_\d+$', name):
                patterns.append('prefix_number')
            elif re.match(r'^\d+$', name):
                patterns.append('number_only')
            elif re.match(r'^[a-zA-Z]+_[a-zA-Z]+_\d+$', name):
                patterns.append('prefix_prefix_number')
            elif re.match(r'^.+_-_.+$', name):
                patterns.append('artist_title')
            else:
                patterns.append('custom')
        
        pattern_counts = Counter(patterns)
        common_prefixes = [p for p, c in prefixes.most_common(5)]
        
        return {
            'patterns': [p for p, _ in pattern_counts.most_common(3)],
            'prefixes': common_prefixes
        }
    
    def validate_audio_file(
        self, 
        filepath: Path,
        check_integrity: bool = True
    ) -> AudioValidationResult:
        """Valida um único arquivo de áudio."""
        result = AudioValidationResult(
            filepath=str(filepath),
            filename=filepath.name,
            is_valid=True,
            issues=[]
        )
        
        # Obtém informações do áudio
        audio_info = get_audio_info(str(filepath))
        result.audio_info = audio_info
        
        if not audio_info.is_valid:
            result.is_valid = False
            result.issues.append(f"Erro ao ler arquivo: {audio_info.error_message}")
            return result
        
        config = self.config
        
        # Valida duração
        if config.validation.check_duration_range:
            if audio_info.duration_sec < config.audio.min_duration_sec:
                result.duration_ok = False
                result.issues.append(
                    f"Duração muito curta: {audio_info.duration_sec:.2f}s "
                    f"(mínimo: {config.audio.min_duration_sec}s)"
                )
            if config.audio.max_duration_sec and audio_info.duration_sec > config.audio.max_duration_sec:
                result.duration_ok = False
                result.issues.append(
                    f"Duração muito longa: {audio_info.duration_sec:.2f}s "
                    f"(máximo: {config.audio.max_duration_sec}s)"
                )
        
        # Valida sample rate
        if config.validation.check_sample_rate:
            if audio_info.sample_rate != config.audio.sample_rate:
                result.sample_rate_ok = False
                result.issues.append(
                    f"Sample rate incorreto: {audio_info.sample_rate}Hz "
                    f"(esperado: {config.audio.sample_rate}Hz)"
                )
        
        # Valida canais
        if config.validation.check_channels:
            if audio_info.channels != config.audio.channels:
                result.channels_ok = False
                result.issues.append(
                    f"Número de canais incorreto: {audio_info.channels} "
                    f"(esperado: {config.audio.channels})"
                )
        
        # Verifica integridade
        if check_integrity and config.validation.check_audio_integrity:
            is_ok, error = verify_audio_integrity(str(filepath))
            result.integrity_ok = is_ok
            if not is_ok:
                result.issues.append(f"Falha de integridade: {error}")
        
        # Calcula hashes para detecção de duplicatas
        if not config.validation.allow_duplicates:
            result.file_hash = compute_file_hash(str(filepath))
            if config.validation.duplicate_detection == "hash":
                result.audio_hash = compute_audio_hash(str(filepath))
        
        # Determina validade geral
        result.is_valid = (
            result.integrity_ok and 
            result.duration_ok and 
            result.sample_rate_ok and 
            result.channels_ok and
            len(result.issues) == 0
        )
        
        return result
    
    def validate_dataset(
        self,
        dataset_path: str | Path,
        check_integrity: bool = True,
        max_workers: int = 4,
        progress_callback: callable = None
    ) -> ValidationReport:
        """
        Executa validação completa do dataset.
        
        Args:
            dataset_path: Caminho para o dataset
            check_integrity: Se deve verificar integridade dos áudios
            max_workers: Número de workers para processamento paralelo
            progress_callback: Função chamada com (current, total) para progresso
        
        Returns:
            ValidationReport com resultados completos
        """
        root = Path(dataset_path)
        report = ValidationReport(
            dataset_path=str(root),
            validated_at=get_timestamp()
        )
        
        print(f"🔍 Iniciando validação de: {root}")
        
        # 1. Detecta estrutura
        print("📁 Detectando estrutura do dataset...")
        report.detected_structure = self.detect_structure(root)
        self._print_structure_summary(report.detected_structure)
        
        # 2. Encontra todos os arquivos
        audio_files = find_audio_files(root)
        metadata_files = find_metadata_files(root)
        annotation_files = list(root.glob("**/*"))
        annotation_files = [
            f for f in annotation_files 
            if f.suffix.lower() in SUPPORTED_ANNOTATION_EXTENSIONS 
            and f not in metadata_files
        ]
        
        report.total_audio_files = len(audio_files)
        report.total_metadata_files = len(metadata_files)
        report.total_annotation_files = len(annotation_files)
        
        print(f"📊 Encontrados: {report.total_audio_files} áudios, "
              f"{report.total_metadata_files} metadados, "
              f"{report.total_annotation_files} anotações")
        
        if report.total_audio_files == 0:
            print("⚠️ Nenhum arquivo de áudio encontrado!")
            return report
        
        # 3. Valida arquivos de áudio em paralelo
        print(f"🎵 Validando {report.total_audio_files} arquivos de áudio...")
        
        file_results = []
        format_counts = Counter()
        sample_rate_counts = Counter()
        channel_counts = Counter()
        durations = []
        sizes = []
        issues_counter = defaultdict(int)
        hash_map = defaultdict(list)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.validate_audio_file, f, check_integrity): f 
                for f in audio_files
            }
            
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                file_results.append(result)
                
                if result.audio_info and result.audio_info.is_valid:
                    info = result.audio_info
                    format_counts[info.format] += 1
                    sample_rate_counts[info.sample_rate] += 1
                    channel_counts[info.channels] += 1
                    durations.append(info.duration_sec)
                    sizes.append(info.file_size_bytes)
                    
                    if result.file_hash:
                        hash_map[result.file_hash].append(result.filepath)
                
                for issue in result.issues:
                    issue_type = issue.split(':')[0]
                    issues_counter[issue_type] += 1
                
                if result.is_valid:
                    report.valid_audio_files += 1
                else:
                    report.invalid_audio_files += 1
                
                if progress_callback:
                    progress_callback(i + 1, len(audio_files))
                
                if (i + 1) % 100 == 0 or (i + 1) == len(audio_files):
                    print(f"   Progresso: {i + 1}/{len(audio_files)}")
        
        # 4. Detecta duplicatas
        duplicates = [
            {"hash": h, "files": files} 
            for h, files in hash_map.items() 
            if len(files) > 1
        ]
        report.duplicates_found = duplicates
        if duplicates:
            issues_counter["Duplicatas encontradas"] = sum(
                len(d["files"]) - 1 for d in duplicates
            )
        
        # 5. Compila estatísticas
        if durations:
            report.total_duration_sec = sum(durations)
            report.avg_duration_sec = sum(durations) / len(durations)
            report.min_duration_sec = min(durations)
            report.max_duration_sec = max(durations)
        
        report.total_size_bytes = sum(sizes)
        report.format_distribution = dict(format_counts)
        report.sample_rate_distribution = {str(k): v for k, v in sample_rate_counts.items()}
        report.channels_distribution = {str(k): v for k, v in channel_counts.items()}
        report.issues_summary = dict(issues_counter)
        report.file_results = file_results
        
        # 6. Imprime resumo
        self._print_validation_summary(report)
        
        return report
    
    def _print_structure_summary(self, structure: DetectedStructure):
        """Imprime resumo da estrutura detectada."""
        print(f"\n📂 Estrutura Detectada:")
        print(f"   Diretórios: {structure.total_dirs}")
        print(f"   Níveis de profundidade: {structure.depth_levels}")
        print(f"   Splits detectados: {structure.detected_splits or 'Nenhum'}")
        print(f"   Subdir de áudio: {'✓' if structure.has_audio_subdir else '✗'}")
        print(f"   Subdir de metadados: {'✓' if structure.has_metadata_subdir else '✗'}")
        print(f"   Subdir de anotações: {'✓' if structure.has_annotations_subdir else '✗'}")
        if structure.naming_patterns:
            print(f"   Padrões de nomenclatura: {structure.naming_patterns}")
        if structure.common_prefixes:
            print(f"   Prefixos comuns: {structure.common_prefixes}")
    
    def _print_validation_summary(self, report: ValidationReport):
        """Imprime resumo da validação."""
        print(f"\n{'='*60}")
        print("📋 RELATÓRIO DE VALIDAÇÃO")
        print(f"{'='*60}")
        
        print(f"\n📊 Estatísticas Gerais:")
        print(f"   Total de arquivos: {report.total_audio_files}")
        print(f"   Válidos: {report.valid_audio_files} ✓")
        print(f"   Inválidos: {report.invalid_audio_files} ✗")
        
        print(f"\n⏱️ Duração:")
        print(f"   Total: {format_duration(report.total_duration_sec)}")
        print(f"   Média: {format_duration(report.avg_duration_sec)}")
        print(f"   Min/Max: {format_duration(report.min_duration_sec)} / "
              f"{format_duration(report.max_duration_sec)}")
        
        print(f"\n💾 Tamanho Total: {format_bytes(report.total_size_bytes)}")
        
        print(f"\n🎵 Distribuição de Formatos:")
        for fmt, count in report.format_distribution.items():
            pct = count / report.total_audio_files * 100
            print(f"   {fmt}: {count} ({pct:.1f}%)")
        
        print(f"\n🔊 Sample Rates:")
        for sr, count in report.sample_rate_distribution.items():
            pct = count / report.total_audio_files * 100
            print(f"   {sr}Hz: {count} ({pct:.1f}%)")
        
        if report.issues_summary:
            print(f"\n⚠️ Problemas Encontrados:")
            for issue, count in report.issues_summary.items():
                print(f"   {issue}: {count}")
        
        if report.duplicates_found:
            print(f"\n🔄 Duplicatas: {len(report.duplicates_found)} grupos")
        
        validation_rate = (report.valid_audio_files / report.total_audio_files * 100 
                          if report.total_audio_files > 0 else 0)
        print(f"\n{'='*60}")
        print(f"✅ Taxa de Validação: {validation_rate:.1f}%")
        print(f"{'='*60}\n")
    
    def save_report(self, report: ValidationReport, output_path: str | Path):
        """Salva relatório de validação em JSON."""
        save_json(report.to_dict(), output_path)
        print(f"💾 Relatório salvo em: {output_path}")


def main():
    """Função principal para execução via CLI."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Pipeline de Validação de Datasets MIR"
    )
    parser.add_argument(
        "dataset_path",
        help="Caminho para o dataset a ser validado"
    )
    parser.add_argument(
        "-o", "--output",
        help="Caminho para salvar o relatório JSON",
        default=None
    )
    parser.add_argument(
        "--no-integrity",
        action="store_true",
        help="Pular verificação de integridade (mais rápido)"
    )
    parser.add_argument(
        "-w", "--workers",
        type=int,
        default=4,
        help="Número de workers paralelos (padrão: 4)"
    )
    
    args = parser.parse_args()
    
    pipeline = ValidationPipeline()
    report = pipeline.validate_dataset(
        args.dataset_path,
        check_integrity=not args.no_integrity,
        max_workers=args.workers
    )
    
    if args.output:
        pipeline.save_report(report, args.output)
    else:
        # Salva no mesmo diretório com timestamp
        output_path = Path(args.dataset_path) / f"validation_report_{get_timestamp()[:10]}.json"
        pipeline.save_report(report, output_path)


if __name__ == "__main__":
    main()
