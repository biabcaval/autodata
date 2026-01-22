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
    asyncio.run(organize())
    run_pipeline()
