"""
MIR Dataset AutoData - Ponto de entrada principal.

Sistema para automatizar validação e organização de datasets de 
Music Information Retrieval.

Uso:
    python main.py validate <dataset_path> [opções]
    python main.py organize <source_path> <output_path> [opções]
    python main.py full <source_path> <output_path> [opções]
"""

import argparse
import sys
from pathlib import Path

from config import DatasetConfig, DEFAULT_CONFIG, AudioFormat
from validation_pipeline import ValidationPipeline
from organization_pipeline import OrganizationPipeline


def cmd_validate(args):
    """Executa pipeline de validação."""
    config = DEFAULT_CONFIG
    
    pipeline = ValidationPipeline(config)
    report = pipeline.validate_dataset(
        args.dataset_path,
        check_integrity=not args.no_integrity,
        max_workers=args.workers
    )
    
    if args.output:
        pipeline.save_report(report, args.output)
    
    return 0 if report.invalid_audio_files == 0 else 1


def cmd_organize(args):
    """Executa pipeline de organização."""
    config = DatasetConfig(
        name=args.name or DEFAULT_CONFIG.name,
        version=DEFAULT_CONFIG.version,
        audio=DEFAULT_CONFIG.audio,
        structure=DEFAULT_CONFIG.structure,
        naming=DEFAULT_CONFIG.naming,
        metadata=DEFAULT_CONFIG.metadata,
        validation=DEFAULT_CONFIG.validation
    )
    
    # Sobrescreve formato se especificado
    if args.format:
        config.audio.format = AudioFormat(args.format)
    
    pipeline = OrganizationPipeline(config)
    result = pipeline.organize_dataset(
        args.source_path,
        args.output_path,
        validate_first=not args.no_validate,
        skip_invalid=not args.include_invalid,
        create_splits=not args.no_splits,
        max_workers=args.workers
    )
    
    # Salva resultado
    result_path = Path(args.output_path) / "organization_result.json"
    pipeline.save_result(result, result_path)
    
    return 0 if result.failed == 0 else 1


def cmd_full(args):
    """Executa pipeline completa (validação + organização)."""
    print("=" * 60)
    print("🎵 MIR Dataset AutoData - Pipeline Completa")
    print("=" * 60)
    
    # Primeiro valida
    print("\n📋 FASE 1: Validação")
    print("-" * 60)
    
    config = DatasetConfig(
        name=args.name or DEFAULT_CONFIG.name,
        version=DEFAULT_CONFIG.version
    )
    
    if args.format:
        config.audio.format = AudioFormat(args.format)
    
    validator = ValidationPipeline(config)
    report = validator.validate_dataset(
        args.source_path,
        check_integrity=True,
        max_workers=args.workers
    )
    
    # Salva relatório de validação
    validation_output = Path(args.output_path) / "validation_report.json"
    Path(args.output_path).mkdir(parents=True, exist_ok=True)
    validator.save_report(report, validation_output)
    
    # Depois organiza
    print("\n📁 FASE 2: Organização")
    print("-" * 60)
    
    organizer = OrganizationPipeline(config)
    result = organizer.organize_dataset(
        args.source_path,
        args.output_path,
        validate_first=False,  # Já validamos
        skip_invalid=not args.include_invalid,
        create_splits=not args.no_splits,
        max_workers=args.workers
    )
    
    # Salva resultado
    result_path = Path(args.output_path) / "organization_result.json"
    organizer.save_result(result, result_path)
    
    print("\n" + "=" * 60)
    print("✅ Pipeline completa finalizada!")
    print(f"📂 Output: {args.output_path}")
    print("=" * 60)
    
    return 0 if result.failed == 0 else 1


def main():
    parser = argparse.ArgumentParser(
        description="MIR Dataset AutoData - Validação e Organização de Datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Validar um dataset
  python main.py validate ./meu_dataset -o relatorio.json

  # Organizar um dataset
  python main.py organize ./entrada ./saida --name "MeuDataset"

  # Pipeline completa (validação + organização)
  python main.py full ./entrada ./saida --name "MeuDataset" --format wav
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Comando a executar')
    
    # Subcomando: validate
    p_validate = subparsers.add_parser('validate', help='Validar dataset')
    p_validate.add_argument('dataset_path', help='Caminho do dataset')
    p_validate.add_argument('-o', '--output', help='Caminho para salvar relatório JSON')
    p_validate.add_argument('--no-integrity', action='store_true',
                           help='Pular verificação de integridade')
    p_validate.add_argument('-w', '--workers', type=int, default=4,
                           help='Workers paralelos (padrão: 4)')
    
    # Subcomando: organize
    p_organize = subparsers.add_parser('organize', help='Organizar dataset')
    p_organize.add_argument('source_path', help='Caminho do dataset de origem')
    p_organize.add_argument('output_path', help='Caminho para dataset organizado')
    p_organize.add_argument('--name', help='Nome do dataset')
    p_organize.add_argument('--format', choices=['wav', 'mp3', 'flac', 'ogg'],
                           help='Formato de áudio de saída')
    p_organize.add_argument('--no-validate', action='store_true',
                           help='Pular validação prévia')
    p_organize.add_argument('--include-invalid', action='store_true',
                           help='Incluir arquivos inválidos')
    p_organize.add_argument('--no-splits', action='store_true',
                           help='Não criar splits train/val/test')
    p_organize.add_argument('-w', '--workers', type=int, default=4,
                           help='Workers paralelos (padrão: 4)')
    
    # Subcomando: full
    p_full = subparsers.add_parser('full', help='Pipeline completa')
    p_full.add_argument('source_path', help='Caminho do dataset de origem')
    p_full.add_argument('output_path', help='Caminho para dataset organizado')
    p_full.add_argument('--name', help='Nome do dataset')
    p_full.add_argument('--format', choices=['wav', 'mp3', 'flac', 'ogg'],
                       help='Formato de áudio de saída')
    p_full.add_argument('--include-invalid', action='store_true',
                       help='Incluir arquivos inválidos')
    p_full.add_argument('--no-splits', action='store_true',
                       help='Não criar splits train/val/test')
    p_full.add_argument('-w', '--workers', type=int, default=4,
                       help='Workers paralelos (padrão: 4)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 0
    
    commands = {
        'validate': cmd_validate,
        'organize': cmd_organize,
        'full': cmd_full
    }
    
    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
