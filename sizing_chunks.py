# sizing_chunks.py
from pathlib import Path
import pandas as pd
import re

try:
    import psutil
except ImportError:
    psutil = None

# === ajuste para o seu dataset ===
SEP = ";"
ENC = "latin1"
COLS_EST_30 = [
    "cnpj_basico","cnpj_ordem","cnpj_dv","identificador_matriz_filial",
    "nome_fantasia","situacao_cadastral","data_situacao_cadastral",
    "motivo_situacao_cadastral","nome_cidade_exterior","pais",
    "data_inicio_atividade","cnae_fiscal_principal","cnaes_secundarios",
    "tipo_logradouro","logradouro","numero","complemento","bairro",
    "cep","uf","codigo_municipio","municipio",
    "ddd_1","telefone_1","ddd_2","telefone_2",
    "fax","email","situacao_especial","data_situacao_especial"
]
COLS_EST_31 = [
    "cnpj_basico","cnpj_ordem","cnpj_dv","identificador_matriz_filial",
    "nome_fantasia","situacao_cadastral","data_situacao_cadastral",
    "motivo_situacao_cadastral","nome_cidade_exterior","pais",
    "data_inicio_atividade","cnae_fiscal_principal","cnaes_secundarios",
    "tipo_logradouro","logradouro","numero","complemento","bairro",
    "cep","uf","codigo_municipio","municipio",
    "ddd_1","telefone_1","ddd_2","telefone_2",
    "ddd_fax","fax","email","situacao_especial","data_situacao_especial"
]

USECOLS = [
    "cnpj_basico","cnpj_ordem","cnpj_dv","nome_fantasia",
    "situacao_cadastral",
    "cnae_fiscal_principal","cnaes_secundarios",
    "tipo_logradouro","logradouro","numero","complemento","bairro","cep","uf",
    "codigo_municipio","municipio"
]
DTYPE = {c: "string" for c in USECOLS}  # string é mais previsível que object

def available_ram_bytes() -> int:
    if psutil:
        return int(psutil.virtual_memory().available)
    # fallback em Linux
    try:
        with open("/proc/meminfo") as f:
            memfree = 0
            buffers = 0
            cached = 0
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
                elif line.startswith("MemFree:"):
                    memfree = int(line.split()[1]) * 1024
                elif line.startswith("Buffers:"):
                    buffers = int(line.split()[1]) * 1024
                elif line.startswith("Cached:"):
                    cached = int(line.split()[1]) * 1024
            return memfree + buffers + cached
    except:
        # se não souber, assume 1 GiB disponível
        return 1 * 1024**3

def detect_cols(file_path: Path):
    ncols = pd.read_csv(file_path, sep=SEP, encoding=ENC, header=None, nrows=1).shape[1]
    if ncols == 30:
        return COLS_EST_30
    elif ncols == 31:
        return COLS_EST_31
    else:
        raise ValueError(f"Layout inesperado: {ncols} colunas (esperado 30 ou 31)")

def estimate_bytes_per_row(file_path: Path, sample_rows=10_000) -> float:
    names = detect_cols(file_path)
    df = pd.read_csv(
        file_path, sep=SEP, encoding=ENC, header=None, names=names,
        usecols=USECOLS, dtype=DTYPE,
        nrows=sample_rows, keep_default_na=False, na_filter=False,
        on_bad_lines="skip"
    )
    mem_bytes = df.memory_usage(deep=True).sum()
    rows = max(len(df), 1)
    return mem_bytes / rows

def suggest_chunksize(file_path: Path, target_fraction=0.35, hard_cap_mb=400):
    """
    target_fraction: fração da RAM disponível a usar por chunk (ex.: 0.35 = 35%)
    hard_cap_mb: teto absoluto de memória por chunk (ex.: 400 MB) para evitar exageros
    """
    bpr = estimate_bytes_per_row(file_path)  # bytes por linha
    avail = available_ram_bytes()
    budget = min(int(avail * target_fraction), hard_cap_mb * 1024**2)
    # segurança mínima/máxima
    chunksize = max(10_000, int(budget // max(bpr, 1)))
    # limitar para não exagerar (ajuste a gosto)
    chunksize = min(chunksize, 300_000)
    return chunksize, bpr, budget

if __name__ == "__main__":
    # escolha um dos arquivos grandes para estimar (qualquer K*.ESTABELE*)
    probe = sorted(Path(".").glob("K*.ESTABELE*"))
    if not probe:
        raise SystemExit("Não encontrei arquivos K*.ESTABELE* na pasta atual.")
    probe = probe[0]
    cs, bpr, budget = suggest_chunksize(probe, target_fraction=0.35, hard_cap_mb=400)
    print(f"Arquivo amostra: {probe.name}")
    print(f"Bytes/linha estimado: {bpr:,.0f} B")
    print(f"Orçamento de memória por chunk: {budget/1024/1024:.0f} MB")
    print(f"chunksize sugerido: {cs:,} linhas")

    # Exemplo de uso:
    # for chunk in pd.read_csv(probe, ..., chunksize=cs): processar(chunk)
