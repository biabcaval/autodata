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

from process_audio import main


if __name__ == "__main__":
    main()
