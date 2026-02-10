"""
AutoData - Organização de Arquivos de Áudio.

Pipeline async para:
- Listar arquivos de áudio no bucket de origem
- Normalizar nomes para snake_case
- Transferir para bucket de destino
- Gerar manifest

Uso:
    python main.py
    
    ou via Docker/Cloud Run Jobs
"""

from organize import organize
import asyncio
from run_audio_val import run_pipeline


if __name__ == "__main__":
    import os
    
    # Run pipeline to create CSV
    print("=" * 60)
    print("STEP 1: Generating paths_metadata.csv")
    print("=" * 60)
    run_pipeline()
    
    # Verify CSV was created
    csv_path = os.path.join(os.getcwd(), "paths_metadata.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"paths_metadata.csv not found at {csv_path}")
    
    print(f"\n✅ CSV created successfully at: {csv_path}")
    print(f"   File size: {os.path.getsize(csv_path)} bytes\n")
    
    # Run organize pipeline
    print("=" * 60)
    print("STEP 2: Organizing files")
    print("=" * 60)
    asyncio.run(organize())
    
