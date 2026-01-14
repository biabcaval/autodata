import os
import re
import subprocess
import json
import hashlib
from datetime import datetime
from google.cloud import storage

# Configurações
BUCKET_ORIGEM = "beatriz-temp"
BUCKET_DESTINO = "beatriz-temp-output"

# Especificações técnicas aceitas (conforme regras.md)
FORMATOS_ACEITOS = ['.wav', '.mp3', '.flac', '.m4a']
SAMPLE_RATES_ACEITOS = [44100, 48000, 88200, 96000]
BIT_DEPTHS_ACEITOS = [16, 24, 32]
CANAIS_ACEITOS = [1, 2]  # Mono ou Estéreo

# Configuração de conversão (padrão de saída)
OUTPUT_SAMPLE_RATE = 44100
OUTPUT_BIT_DEPTH = 16
OUTPUT_CHANNELS = 2


def listar_arquivos(bucket_name, extensoes=FORMATOS_ACEITOS):
    """Lista todos os arquivos de áudio no bucket"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    arquivos = []
    for blob in bucket.list_blobs():
        if any(blob.name.lower().endswith(ext) for ext in extensoes):
            arquivos.append(blob.name)
    
    return arquivos


def get_info_audio(arquivo_local):
    """Usa FFprobe para obter informações detalhadas do áudio"""
    resultado = subprocess.run([
        'ffprobe', '-v', 'error',
        '-select_streams', 'a:0',
        '-show_entries', 'stream=codec_name,sample_rate,bits_per_sample,channels',
        '-show_entries', 'format=duration,format_name',
        '-of', 'json',
        arquivo_local
    ], capture_output=True, text=True)
    
    data = json.loads(resultado.stdout)
    
    stream = data.get('streams', [{}])[0]
    format_info = data.get('format', {})
    
    return {
        'codec': stream.get('codec_name', 'unknown'),
        'sample_rate': int(stream.get('sample_rate', 0)),
        'bit_depth': int(stream.get('bits_per_sample', 0)),
        'channels': int(stream.get('channels', 0)),
        'duration': float(format_info.get('duration', 0)),
        'format': format_info.get('format_name', 'unknown')
    }


def calcular_hash(arquivo_local):
    """Calcula o hash SHA256 do arquivo para detecção de duplicatas"""
    sha256_hash = hashlib.sha256()
    with open(arquivo_local, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def validar_nome_arquivo(nome_arquivo):
    """Valida se o nome do arquivo está em snake_case sem caracteres especiais"""
    # Pega apenas o nome do arquivo sem o path e extensão
    nome_base = os.path.splitext(os.path.basename(nome_arquivo))[0]
    
    # Regex para snake_case: letras minúsculas, números, underscores
    # Permite também números no início (ex: 001_audio)
    pattern = r'^[a-z0-9]+(_[a-z0-9]+)*$'
    
    # Checagens específicas
    erros = []
    
    # Verifica caracteres especiais e acentos
    if re.search(r'[áàãâéèêíìîóòôõúùûç]', nome_base.lower()):
        erros.append("contém acentos")
    
    if ' ' in nome_base:
        erros.append("contém espaços")
    
    if re.search(r'[!@#$%^&*()\[\]{};:\'",<>?/\\|`~=+]', nome_base):
        erros.append("contém caracteres especiais")
    
    if nome_base != nome_base.lower():
        erros.append("não está em minúsculas")
    
    # Verifica se segue snake_case
    if not re.match(pattern, nome_base.lower().replace('-', '_')):
        if not erros:  # Se não temos erros específicos, adiciona genérico
            erros.append("não segue padrão snake_case")
    
    return len(erros) == 0, erros


def verificar_silencio(arquivo_local):
    """Verifica se o arquivo contém apenas silêncio"""
    resultado = subprocess.run([
        'ffmpeg', '-i', arquivo_local,
        '-af', 'volumedetect',
        '-f', 'null', '-'
    ], capture_output=True, text=True)
    
    # Procura pelo volume máximo no output
    output = resultado.stderr
    max_volume_match = re.search(r'max_volume: ([-\d.]+) dB', output)
    
    if max_volume_match:
        max_volume = float(max_volume_match.group(1))
        # Se o volume máximo for muito baixo, considera silêncio
        return max_volume < -60.0
    
    return False


def verificar_clipping(arquivo_local):
    """Verifica se há clipping no áudio"""
    resultado = subprocess.run([
        'ffmpeg', '-i', arquivo_local,
        '-af', 'astats=metadata=1:reset=1',
        '-f', 'null', '-'
    ], capture_output=True, text=True)
    
    # Procura por indicadores de clipping
    output = resultado.stderr
    
    # Verifica se o pico está próximo de 0 dB (clipping)
    peak_match = re.search(r'Peak level dB: ([-\d.]+)', output)
    if peak_match:
        peak = float(peak_match.group(1))
        return peak >= -0.1  # Clipping se pico >= -0.1 dB
    
    return False


def verificar_integridade(arquivo_local):
    """Verifica se o arquivo de áudio está íntegro (não corrompido)"""
    resultado = subprocess.run([
        'ffmpeg', '-v', 'error',
        '-i', arquivo_local,
        '-f', 'null', '-'
    ], capture_output=True, text=True)
    
    # Se houver erros na saída, o arquivo está corrompido
    return len(resultado.stderr.strip()) == 0


def converter_audio(entrada, saida):
    """Converte para formato padronizado (WAV 44.1kHz 16-bit stereo)"""
    subprocess.run([
        'ffmpeg', '-y',
        '-i', entrada,
        '-ar', str(OUTPUT_SAMPLE_RATE),
        '-ac', str(OUTPUT_CHANNELS),
        '-sample_fmt', 's16',
        '-f', 'wav',
        saida
    ], check=True, capture_output=True)


def gerar_id_unico(nome_arquivo):
    """Gera um ID único baseado no nome do arquivo"""
    nome_base = os.path.splitext(os.path.basename(nome_arquivo))[0]
    # Normaliza para snake_case
    id_normalizado = re.sub(r'[^a-z0-9]', '_', nome_base.lower())
    id_normalizado = re.sub(r'_+', '_', id_normalizado).strip('_')
    return id_normalizado


def processar_arquivo(nome_arquivo, hashes_processados):
    """Processa um único arquivo de áudio com validações detalhadas"""
    client = storage.Client()
    
    # Paths temporários
    local_input = f'/tmp/input_{os.path.basename(nome_arquivo)}'
    local_output = f'/tmp/output_{os.path.basename(nome_arquivo)}.wav'
    
    resultado = {
        "arquivo_original": nome_arquivo,
        "checagens": {},
        "status": "pendente",
        "erros": []
    }
    
    try:
        # ═══════════════════════════════════════════════════════════════
        # DOWNLOAD
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'═'*60}")
        print(f"📁 Arquivo: {nome_arquivo}")
        print(f"{'═'*60}")
        
        bucket_origem = client.bucket(BUCKET_ORIGEM)
        blob = bucket_origem.blob(nome_arquivo)
        blob.download_to_filename(local_input)
        
        # ═══════════════════════════════════════════════════════════════
        # CHECAGEM 1: NOMENCLATURA (snake_case)
        # ═══════════════════════════════════════════════════════════════
        nome_valido, erros_nome = validar_nome_arquivo(nome_arquivo)
        resultado["checagens"]["nomenclatura"] = {
            "valido": nome_valido,
            "detalhes": "snake_case válido" if nome_valido else f"Problemas: {', '.join(erros_nome)}"
        }
        
        if nome_valido:
            print(f"✓ Nomenclatura: snake_case válido")
        else:
            print(f"⚠ Nomenclatura: {', '.join(erros_nome)}")
            resultado["erros"].append(f"Nomenclatura: {', '.join(erros_nome)}")
        
        # ═══════════════════════════════════════════════════════════════
        # CHECAGEM 2: INTEGRIDADE DO ARQUIVO
        # ═══════════════════════════════════════════════════════════════
        arquivo_integro = verificar_integridade(local_input)
        resultado["checagens"]["integridade"] = {
            "valido": arquivo_integro,
            "detalhes": "arquivo íntegro" if arquivo_integro else "arquivo corrompido"
        }
        
        if arquivo_integro:
            print(f"✓ Integridade: arquivo íntegro")
        else:
            print(f"✗ Integridade: arquivo corrompido")
            resultado["erros"].append("Arquivo corrompido")
            resultado["status"] = "falha"
            return False, resultado
        
        # ═══════════════════════════════════════════════════════════════
        # CHECAGEM 3: HASH / DUPLICAÇÃO
        # ═══════════════════════════════════════════════════════════════
        hash_arquivo = calcular_hash(local_input)
        is_duplicado = hash_arquivo in hashes_processados
        resultado["checagens"]["hash"] = {
            "valido": not is_duplicado,
            "sha256": hash_arquivo,
            "detalhes": "duplicado encontrado" if is_duplicado else "arquivo único"
        }
        
        if is_duplicado:
            print(f"⚠ Hash: arquivo duplicado (mesmo conteúdo já processado)")
            resultado["erros"].append("Arquivo duplicado")
        else:
            print(f"✓ Hash: arquivo único (SHA256: {hash_arquivo[:16]}...)")
            hashes_processados.add(hash_arquivo)
        
        # ═══════════════════════════════════════════════════════════════
        # CHECAGEM 4: INFORMAÇÕES TÉCNICAS
        # ═══════════════════════════════════════════════════════════════
        info = get_info_audio(local_input)
        
        # 4.1 - Formato/Codec
        extensao = os.path.splitext(nome_arquivo)[1].lower()
        formato_valido = extensao in FORMATOS_ACEITOS
        resultado["checagens"]["formato"] = {
            "valido": formato_valido,
            "valor": extensao,
            "codec": info['codec'],
            "detalhes": f"{extensao.upper()} aceito" if formato_valido else f"{extensao} não aceito"
        }
        
        if formato_valido:
            print(f"✓ Formato: {extensao.upper()} (codec: {info['codec']})")
        else:
            print(f"✗ Formato: {extensao} não aceito")
            resultado["erros"].append(f"Formato {extensao} não aceito")
        
        # 4.2 - Sample Rate
        sample_rate_valido = info['sample_rate'] in SAMPLE_RATES_ACEITOS
        resultado["checagens"]["sample_rate"] = {
            "valido": sample_rate_valido,
            "valor": info['sample_rate'],
            "detalhes": f"{info['sample_rate']}Hz aceito" if sample_rate_valido else f"{info['sample_rate']}Hz fora do padrão"
        }
        
        if sample_rate_valido:
            print(f"✓ Sample Rate: {info['sample_rate']}Hz")
        else:
            print(f"⚠ Sample Rate: {info['sample_rate']}Hz (fora do padrão, será convertido)")
        
        # 4.3 - Bit Depth
        bit_depth_valido = info['bit_depth'] in BIT_DEPTHS_ACEITOS or info['bit_depth'] == 0
        resultado["checagens"]["bit_depth"] = {
            "valido": bit_depth_valido,
            "valor": info['bit_depth'],
            "detalhes": f"{info['bit_depth']}-bit" if info['bit_depth'] > 0 else "não detectado"
        }
        
        if info['bit_depth'] > 0:
            print(f"✓ Bit Depth: {info['bit_depth']}-bit")
        else:
            print(f"⚠ Bit Depth: não detectado (será convertido para 16-bit)")
        
        # 4.4 - Canais
        canais_valido = info['channels'] in CANAIS_ACEITOS
        canais_nome = "estéreo" if info['channels'] == 2 else "mono" if info['channels'] == 1 else f"{info['channels']} canais"
        resultado["checagens"]["canais"] = {
            "valido": canais_valido,
            "valor": info['channels'],
            "detalhes": canais_nome
        }
        
        if canais_valido:
            print(f"✓ Canais: {canais_nome}")
        else:
            print(f"⚠ Canais: {canais_nome} (será convertido para estéreo)")
        
        # ═══════════════════════════════════════════════════════════════
        # CHECAGEM 5: DURAÇÃO
        # ═══════════════════════════════════════════════════════════════
        duracao = info['duration']
        duracao_valida = duracao > 0.1  # Mínimo de 0.1 segundos
        resultado["checagens"]["duracao"] = {
            "valido": duracao_valida,
            "valor_segundos": duracao,
            "detalhes": f"{duracao:.2f}s" if duracao_valida else "duração inválida"
        }
        
        if duracao_valida:
            print(f"✓ Duração: {duracao:.2f} segundos")
        else:
            print(f"✗ Duração: {duracao:.2f}s (inválida)")
            resultado["erros"].append("Duração inválida")
        
        # ═══════════════════════════════════════════════════════════════
        # CHECAGEM 6: SILÊNCIO
        # ═══════════════════════════════════════════════════════════════
        tem_silencio = verificar_silencio(local_input)
        resultado["checagens"]["silencio"] = {
            "valido": not tem_silencio,
            "detalhes": "contém apenas silêncio" if tem_silencio else "áudio detectado"
        }
        
        if tem_silencio:
            print(f"⚠ Silêncio: arquivo contém apenas silêncio")
            resultado["erros"].append("Arquivo contém apenas silêncio")
        else:
            print(f"✓ Silêncio: áudio detectado OK")
        
        # ═══════════════════════════════════════════════════════════════
        # CHECAGEM 7: CLIPPING
        # ═══════════════════════════════════════════════════════════════
        tem_clipping = verificar_clipping(local_input)
        resultado["checagens"]["clipping"] = {
            "valido": not tem_clipping,
            "detalhes": "clipping detectado" if tem_clipping else "sem clipping"
        }
        
        if tem_clipping:
            print(f"⚠ Clipping: distorção detectada")
            resultado["erros"].append("Clipping detectado")
        else:
            print(f"✓ Clipping: sem distorção")
        
        # ═══════════════════════════════════════════════════════════════
        # CONVERSÃO E UPLOAD
        # ═══════════════════════════════════════════════════════════════
        print(f"\n→ Convertendo para WAV {OUTPUT_SAMPLE_RATE}Hz {OUTPUT_BIT_DEPTH}-bit estéreo...")
        converter_audio(local_input, local_output)
        
        # Gera ID único
        id_unico = gerar_id_unico(nome_arquivo)
        
        # Upload do arquivo processado
        bucket_destino = client.bucket(BUCKET_DESTINO)
        nome_saida = f"audio/{id_unico}.wav"
        blob_saida = bucket_destino.blob(nome_saida)
        blob_saida.upload_from_filename(local_output)
        print(f"→ Upload: {nome_saida}")
        
        # Monta metadados completos
        resultado["id"] = id_unico
        resultado["duracao_segundos"] = duracao
        resultado["arquivo_saida"] = nome_saida
        resultado["info_original"] = info
        resultado["hash_sha256"] = hash_arquivo
        resultado["data_processamento"] = datetime.now().isoformat()
        resultado["status"] = "sucesso" if len(resultado["erros"]) == 0 else "sucesso_com_avisos"
        
        # Upload metadados
        blob_meta = bucket_destino.blob(f"metadata/{id_unico}.json")
        blob_meta.upload_from_string(json.dumps(resultado, indent=2, ensure_ascii=False))
        print(f"→ Metadados: metadata/{id_unico}.json")
        
        # Resumo final
        total_checks = len(resultado["checagens"])
        checks_ok = sum(1 for c in resultado["checagens"].values() if c["valido"])
        print(f"\n{'─'*40}")
        print(f"📊 Resultado: {checks_ok}/{total_checks} checagens OK")
        
        if resultado["erros"]:
            print(f"⚠ Avisos: {len(resultado['erros'])}")
        
        print(f"✅ Arquivo processado com sucesso!")
        
        return True, resultado
        
    except Exception as e:
        print(f"\n✗ ERRO FATAL: {e}")
        resultado["status"] = "falha"
        resultado["erros"].append(str(e))
        return False, resultado
        
    finally:
        # Limpa arquivos temporários
        if os.path.exists(local_input):
            os.remove(local_input)
        if os.path.exists(local_output):
            os.remove(local_output)


def gerar_manifest(bucket_destino_nome, resultados):
    """Gera o arquivo manifest.csv com todos os arquivos processados"""
    client = storage.Client()
    bucket = client.bucket(bucket_destino_nome)
    
    # Cabeçalho conforme regras.md
    linhas = ["folder_path,dataset_type,id,duration_seconds,status,hash_sha256"]
    
    for r in resultados:
        if r["status"] in ["sucesso", "sucesso_com_avisos"]:
            linha = f"{r.get('arquivo_saida', '')},audio,{r.get('id', '')},{r.get('duracao_segundos', 0):.2f},{r['status']},{r.get('hash_sha256', '')}"
            linhas.append(linha)
    
    conteudo = "\n".join(linhas)
    
    # Upload do manifest
    blob = bucket.blob("manifest.csv")
    blob.upload_from_string(conteudo, content_type="text/csv")
    
    print(f"\n📋 Manifest gerado: manifest.csv ({len(linhas)-1} entradas)")


def main():
    """Função principal - processa arquivos baseado no índice da task"""
    
    print("\n" + "═"*60)
    print("🎵 AUTODATA - Pipeline de Validação e Organização")
    print("═"*60)
    
    # Pega o índice desta task (0, 1, 2, ...)
    task_index = int(os.environ.get('CLOUD_RUN_TASK_INDEX', 
                     os.environ.get('BATCH_TASK_INDEX', 0)))
    
    # Total de tasks rodando em paralelo
    task_count = int(os.environ.get('CLOUD_RUN_TASK_COUNT', 
                     os.environ.get('BATCH_TASK_COUNT', 1)))
    
    print(f"🚀 Task {task_index + 1} de {task_count}")
    print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Lista todos os arquivos
    todos_arquivos = listar_arquivos(BUCKET_ORIGEM)
    print(f"📁 Total de arquivos no bucket: {len(todos_arquivos)}")
    
    # Divide os arquivos entre as tasks
    meus_arquivos = [
        arquivo for i, arquivo in enumerate(todos_arquivos)
        if i % task_count == task_index
    ]
    
    print(f"📋 Esta task vai processar: {len(meus_arquivos)} arquivos")
    
    # Processa cada arquivo
    sucessos = 0
    falhas = 0
    avisos = 0
    resultados = []
    hashes_processados = set()
    
    for arquivo in meus_arquivos:
        sucesso, resultado = processar_arquivo(arquivo, hashes_processados)
        resultados.append(resultado)
        
        if resultado["status"] == "sucesso":
            sucessos += 1
        elif resultado["status"] == "sucesso_com_avisos":
            avisos += 1
        else:
            falhas += 1
    
    # Gera manifest apenas na task 0
    if task_index == 0 and resultados:
        gerar_manifest(BUCKET_DESTINO, resultados)
    
    # Resumo final
    print("\n" + "═"*60)
    print(f"📊 RESUMO FINAL - Task {task_index + 1}")
    print("═"*60)
    print(f"   ✅ Sucessos: {sucessos}")
    print(f"   ⚠️  Com avisos: {avisos}")
    print(f"   ❌ Falhas: {falhas}")
    print(f"   📁 Total processado: {len(meus_arquivos)}")
    print("═"*60 + "\n")


if __name__ == "__main__":
    main()
