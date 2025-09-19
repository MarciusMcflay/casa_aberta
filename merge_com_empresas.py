# merge_com_empresas.py
# Enriquecimento da base (por CNPJ) com dados de EMPRECSV + QUALSCSV
# - junta razão social, porte (decodificado) e qualificação do responsável (decodificada)
#
# Uso (exemplos):
#   python merge_com_empresas.py --in empresas_tecnologia_sao_carlos_ativas.csv \
#       --out empresas_tecnologia_sc_ativas_enriquecidas.csv --chunksize 300000
#
#   # se os arquivos estiverem com nomes/globs diferentes:
#   python merge_com_empresas.py --in base.csv --out saida.csv \
#       --emp-glob "K*.EMPRECSV*" --qual-glob "*QUALSCSV*" --chunksize 200000
#
# Observação:
# - Este script PRESERVA as colunas de contato vindas do passo 1/2 (email, telefone_*),
#   além de outras colunas úteis (endereco, cnaes, municipio, uf, etc.).
# - Não altera geocache; atua apenas nos CSVs do pipeline.

from pathlib import Path
import argparse
import pandas as pd
import re

PORTE_MAP = {
    "00": "Não informado",
    "01": "Microempresa",
    "03": "Empresa de Pequeno Porte",
    "05": "Demais",
    # tolerância para variantes sem zero à esquerda
    "0": "Não informado",
    "1": "Microempresa",
    "3": "Empresa de Pequeno Porte",
    "5": "Demais",
}

# Layout EMPRESAS (7 colunas)
COLS_EMP = [
    "cnpj_basico", "razao_social", "natureza_juridica",
    "qualificacao_responsavel", "capital_social",
    "porte_empresa", "ente_federativo_responsavel"
]

def s(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)

def main():
    ap = argparse.ArgumentParser(description="Enriquece base por CNPJ com EMPRECSV (razão, porte, capital) e QUALSCSV (qualificação).")
    ap.add_argument("--in", dest="input_csv", required=True, help="CSV de entrada contendo coluna 'cnpj' (14 dígitos).")
    ap.add_argument("--out", dest="output_csv", required=True, help="CSV de saída enriquecido.")
    ap.add_argument("--chunksize", type=int, default=300_000, help="Tamanho do chunk para ler EMPRECSV (linhas).")
    ap.add_argument("--emp-glob", default="K*.EMPRECSV*", help="Glob para localizar arquivos EMPRECSV (default: K*.EMPRECSV*).")
    ap.add_argument("--qual-glob", default="*QUALSCSV*", help="Glob para localizar arquivo QUALSCSV (default: *QUALSCSV*).")
    args = ap.parse_args()

    input_path  = Path(args.input_csv)
    output_path = Path(args.output_csv)

    if not input_path.exists():
        raise FileNotFoundError(f"Não encontrei o CSV de entrada: {input_path}")

    # 0) Ler base (com cnpj 14 dígitos)
    base = pd.read_csv(input_path, dtype=str, keep_default_na=False, na_filter=False)
    if "cnpj" not in base.columns:
        raise ValueError(f"'{input_path.name}' precisa ter a coluna 'cnpj' (14 dígitos).")

    base["cnpj"] = base["cnpj"].str.replace(r"\D", "", regex=True)
    base = base[base["cnpj"].str.len() == 14].copy()
    if base.empty:
        raise SystemExit("A base de entrada ficou vazia após normalizar CNPJ (14 dígitos).")

    base["cnpj_basico"] = base["cnpj"].str[:8]
    alvo_basicos = set(base["cnpj_basico"].unique().tolist())
    print(f"CNPJs na base: {len(base):,} | cnpj_basico únicos: {len(alvo_basicos):,}")

    # 1) Localizar QUALSCSV e montar dicionário (codigo -> descrição)
    qual_files = sorted(Path(".").glob(args.qual_glob))
    if not qual_files:
        raise FileNotFoundError(f"Não encontrei '{args.qual_glob}' na pasta (ex.: F.K03200$Z.D50809.QUALSCSV).")

    ncols_qual = pd.read_csv(
        qual_files[0], sep=";", encoding="latin1", header=None,
        nrows=1, keep_default_na=False, na_filter=False
    ).shape[1]
    usecols_qual = [0, 1] if ncols_qual >= 2 else [0]
    quals = pd.read_csv(
        qual_files[0], sep=";", encoding="latin1", header=None,
        usecols=usecols_qual, dtype=str, keep_default_na=False, na_filter=False
    )
    if quals.shape[1] < 2:
        raise ValueError("QUALSCSV não tem ao menos duas colunas (código;descrição).")
    quals.columns = ["cod_qualificacao", "qualificacao_responsavel_txt"]
    quals["cod_qualificacao"] = quals["cod_qualificacao"].astype(str).str.strip()
    map_qual = dict(zip(quals["cod_qualificacao"], quals["qualificacao_responsavel_txt"]))

    # 2) Ler EMPRECSV em chunks e reter somente cnpj_basico-alvo
    emp_files = sorted(Path(".").glob(args.emp_glob))
    if not emp_files:
        raise FileNotFoundError(f"Não encontrei '{args.emp_glob}' na pasta.")

    hits = []
    for emp in emp_files:
        print(f"Lendo: {emp.name}")
        try:
            for chunk in pd.read_csv(
                emp, sep=";", encoding="latin1", header=None, names=COLS_EMP,
                usecols=["cnpj_basico","razao_social","qualificacao_responsavel","capital_social","porte_empresa"],
                dtype=str, keep_default_na=False, na_filter=False,
                low_memory=False, chunksize=args.chunksize, on_bad_lines="skip"
            ):
                m = chunk["cnpj_basico"].isin(alvo_basicos)
                if m.any():
                    hits.append(chunk.loc[m].copy())
        except Exception as e:
            print(f"Falha em {emp.name}: {e}")

    if not hits:
        print("Nenhuma correspondência em EMPRECSV; salvando base original com campos vazios.")
        out = base.copy()
        out["razao_social"] = ""
        out["porte_empresa"] = ""
        out["porte_empresa_txt"] = ""
        out["capital_social"] = ""
        out["qualificacao_responsavel"] = ""
        out["qualificacao_responsavel_txt"] = ""
        # Ordenação e saída preservando colunas úteis/contatos
        possible_contact_cols = [
            "email","ddd_1","telefone_1","ddd_2","telefone_2","fax",
            "telefone_1_full","telefone_2_full","telefones_norm"
        ]
        passthrough_cols = [
            "nome","nome_fantasia","endereco",
            "cnae_fiscal_principal","cnaes_secundarios",
            "municipio","uf"
        ]
        cols_prefer = [
            "razao_social","cnpj",
            *[c for c in passthrough_cols if c in out.columns],
            "porte_empresa","porte_empresa_txt","capital_social",
            "qualificacao_responsavel","qualificacao_responsavel_txt",
            *[c for c in possible_contact_cols if c in out.columns],
        ]
        cols_prefer = [c for c in cols_prefer if c in out.columns]
        saida = out[cols_prefer].drop_duplicates(subset=["cnpj"]).sort_values(["razao_social","cnpj"])
        saida.to_csv(output_path, index=False, encoding="utf-8")
        print(f"Gerado: {output_path} | linhas: {len(saida)}")
        return

    emp = pd.concat(hits, ignore_index=True).drop_duplicates(subset=["cnpj_basico"])

    # Decodificar porte
    emp["porte_empresa_txt"] = emp["porte_empresa"].map(PORTE_MAP).fillna("Desconhecido")
    # Decodificar qualificação do responsável
    emp["qualificacao_responsavel_txt"] = emp["qualificacao_responsavel"].map(lambda x: map_qual.get(s(x).strip(), ""))

    # 3) Merge final (m:1 via cnpj_basico)
    out = base.merge(emp, on="cnpj_basico", how="left", validate="m:1")

    # 4) Ordena e salva (preservando contatos e colunas úteis)
    possible_contact_cols = [
        "email","ddd_1","telefone_1","ddd_2","telefone_2","fax",
        "telefone_1_full","telefone_2_full","telefones_norm"
    ]
    passthrough_cols = [
        "nome","nome_fantasia","endereco",
        "cnae_fiscal_principal","cnaes_secundarios",
        "municipio","uf"
    ]

    cols_prefer = [
        "razao_social","cnpj",
        *[c for c in passthrough_cols if c in out.columns],
        "porte_empresa","porte_empresa_txt",
        "capital_social",
        "qualificacao_responsavel","qualificacao_responsavel_txt",
        *[c for c in possible_contact_cols if c in out.columns],
    ]

    # garante colunas mesmo que vazias
    for c in cols_prefer:
        if c not in out.columns:
            out[c] = ""

    # Seleção & ordenação
    cols_prefer = [c for c in cols_prefer if c in out.columns]
    saida = (
        out[cols_prefer]
        .drop_duplicates(subset=["cnpj"])
        .sort_values(["razao_social","cnpj"], na_position="last")
    )

    saida.to_csv(output_path, index=False, encoding="utf-8")

    print(f"\nGerado: {output_path} | linhas: {len(saida)}")
    try:
        print(saida.head(10))
    except Exception:
        pass

if __name__ == "__main__":
    main()