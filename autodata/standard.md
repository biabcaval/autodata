## 🎯 **Objetivo**

Este documento estabelece as **diretrizes obrigatórias** para a entrega de todos os **datasets de áudio**, devendo ser lido em conjunto com os **guias específicos por tipo de dataset**.

O cumprimento destas regras garante a **consistência, rastreabilidade e integridade** dos dados, permitindo uma **ingestão automatizada e eficiente** em nossos pipelines de Machine Learning.

> Toda entrega deve seguir as regras a seguir, independentemente do tipo de dataset.
> 

## ⚙️ **1. Estrutura de Entrega**

Toda entrega deve ser enviada como um **arquivo `.zip`** ou disponibilizada em um **bucket** autorizado, com a seguinte estrutura:

```go
[NOME_DO_DATASET]_v[VERSAO]_[YYYY-MM-DD]/
├── audio/
├── metadata/
└── manifest.csv
```

**Exemplo de nome do pacote:**

**`GALAXY_v4_2025-10-01`**

**Descrição dos diretórios principais:**

| Diretório / Arquivo | Descrição |
| --- | --- |
| **`audio/`** | Contém os arquivos de áudio brutos, organizados conforme o tipo de dataset. |
| **`metadata/`** | Armazena os metadados e anotações associadas a cada item. |
| **`manifest.csv`** | Arquivo central de indexação de todos os arquivos e metadados incluídos. |

## 🎛️ **2. Especificações Técnicas dos Arquivos de Áudio**

| Parâmetro | Valores Aceitos | Observações |
| --- | --- | --- |
| **Formato** | WAV (preferencial) / MP3 / FLAC / M4A | WAV é mandatório para tarefas de análise de alta fidelidade. |
| **Sample Rate** | 44.1kHz / 48kHz / 88.2kHz / 96kHz | Deve ser consistente dentro do mesmo dataset. |
| **Bit Depth** | 16-bit / 24-bit / 32-bit | Especificar no metadado **`data.json`** |
| **Canais** | Estéreo (padrão) / Mono (casos específicos) | Especificar no metadado **`data.json`** |

### **2.1. Validação e Checklist Técnicos**

Toda entrega passa por validação **automatizada (100% dos arquivos)** e **manual (amostragem de 10%, até 20 arquivos)**.

### 🔹 **Validações Automatizadas**

| Categoria | Itens Verificados | Descrição |
| --- | --- | --- |
| **Conformidade Técnica** | Codec, Sample Rate, Bit Depth, Canais | Garante uniformidade entre arquivos. |
| **Integridade do Pacote** | Estrutura, nomenclatura, unicidade de IDs | Nomes de arquivo e pastas seguem padrão **`snake_case`**. Nenhum ID duplicado ou arquivo órfão no **`manifest.csv`**. |
| **Checksums / Duplicação** | MD5 ou SHA256 | Identifica arquivos repetidos mesmo com nomes diferentes. |
| **Metadados / Schema** | JSON Schema | Valida estrutura e presença de campos obrigatórios em **`data.json`**. |
| **Consistência Manifesto** | **`manifest.csv`** × sistema de arquivos | Todos os caminhos (**`file_path`**) devem existir fisicamente. Nenhum arquivo fora do manifesto. |
| **Qualidade de Sinal** | Silêncio, clipping, duração | Verifica duração coerente, ausência de silêncio total e clipping. |

### 🔹 **Validações Manuais**

| Categoria | Itens Verificados | Descrição |
| --- | --- | --- |
| **Sincronização** | Arquivos relacionados (ex.: multistems, dry/wet) | Confirma alinhamento temporal entre as diferentes versões. |
| **Acurácia de Anotação** | Dados MIR, letras, seções | Verifica se as anotações (acordes, beats, seções etc.) correspondem ao conteúdo do áudio. |
| **Qualidade Sonora** | Escuta crítica | Identifica artefatos (ruídos, distorção, cortes). Confere consistência acústica entre amostras. |

> Caso seja encontrando algum problema na etapa de validações técnicas, o lote completo deve ser **devolvido** para o provider para ser ajustado.
> 

## 🧩 **3. Nomenclatura e Identificadores**

| Elemento | Regra | Exemplo |
| --- | --- | --- |
| **Convenção de nomes** | **`snake_case`** (minúsculas, separadas por **`_`**) | **`song_001_master.wav`** |
| **IDs únicos** | Identificador consistente entre **`audio/`** e **`metadata/`** | **`id`**: **`track_045`** |
| **Evitar** | Espaços, acentos, caracteres especiais | — |

## 📦 **4. Arquivo de Manifesto (`manifest.csv`)**

| Item | Especificação |
| --- | --- |
| **Codificação** | UTF-8 |
| **Separador** | Vírgula (**`,`**) |
| **Cabeçalho** | Obrigatório na primeira linha |

**Colunas mínimas obrigatórias:**

| Campo | Descrição |
| --- | --- |
| **`folder_path`** | Caminho relativo do arquivo dentro do pacote. |
| **`dataset_type`** | Tipo de dataset (ex: **`multistem`**, **`vocalset_studio`**, **`mir_chords`**). |
| **`id`** | Identificador único do item. |
| **`duration_seconds`** | Duração em segundos. |

## 🧪 **5. Checklist Final de Submissão**

Antes da entrega, confirme:

- [ ]  Estrutura de pastas conforme especificação.
- [ ]  **`manifest.csv`** completo e válido.
- [ ]  Nomenclatura padronizada (**`snake_case`**).
- [ ]  Arquivos válidos e consistentes (sem corrompidos, vazios ou duplicados).
- [ ]  Metadados validados conforme schema.
- [ ]  Checagens técnicas e manuais concluídas.

## 🔄 **6. Controle de Versão**

| Tipo de alteração | Convenção de versão |
| --- | --- |
| Primeira entrega | **`v1.0`** |
| Correções menores | **`v1.1`** |
| Reestruturação ou nova coleta | **`v2.0`** |

##