# merge_socios.py
# Junta a sua base de empresas (com CNPJ) aos sócios dos K*.SOCIOCSV*, decodificando
# qualificação (QUALSCSV) e país (PAISCSV, se existir).
#
# Uso básico:
#   python merge_socios.py --in empresas_tecnologia_sc_ativas_enriquecidas.csv \
#       --out empresas_tecnologia_sc_ativas_com_socios.csv --chunksize 300000
#
# Opções avançadas:
#   --soc-glob "K*.SOCIOCSV*"   (default)
#   --qual-glob "*QUALSCSV*"    (default, obrigatório)
#   --pais-glob "*PAISCSV*"     (default, opcional)
#   --only PF|PJ|EXT|ALL        (filtra tipo de sócio: PF=2, PJ=1, EXT=3; default ALL)
#   --max-n 50000               (limita linhas processadas p/ teste)
#
# Padrão (tudo): PF, PJ e estrangeiros; decodifica qualificação e país
# python merge_socios.py \
#   --in empresas_tecnologia_sc_ativas_enriquecidas.csv \
#   --out empresas_tecnologia_sc_ativas_com_socios.csv \
#   --chunksize 300000

# Somente Pessoa Física
# python merge_socios.py \
#   --in empresas_tecnologia_sc_ativas_enriquecidas.csv \
#   --out empresas_tecnologia_sc_ativas_com_socios_pf.csv \
#   --only PF --chunksize 300000

# Indicando globs diferentes e limitando processamento (teste)
# python merge_socios.py \
#   --in base.csv --out saida_socios.csv \
#   --soc-glob "K*.SOCIOCSV*" \
#   --qual-glob "*QUALSCSV*" \
#   --pais-glob "*PAISCSV*" \
#   --chunksize 200000 --max-n 100000

from pathlib import Path
import argparse
import pandas as pd
import re

IDENT_MAP = {"1": "Pessoa Jurídica", "2": "Pessoa Física", "3": "Estrangeiro"}

# ---------- utils ----------
def s(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)

def so_digitos(x):
    return re.sub(r"\D", "", s(x))

def load_qual_map(qual_glob: str) -> dict:
    qual_files = sorted(Path(".").glob(qual_glob))
    if not qual_files:
        raise FileNotFoundError(f"Não encontrei '{qual_glob}' (ex.: F.K03200$Z.D50809.QUALSCSV).")

    ncols_qual = pd.read_csv(
        qual_files[0], sep=";", encoding="latin1", header=None,
        nrows=1, keep_default_na=False, na_filter=False
    ).shape[1]
    usecols = [0, 1] if ncols_qual >= 2 else [0]
    quals = pd.read_csv(
        qual_files[0], sep=";", encoding="latin1", header=None,
        usecols=usecols, names=["cod","desc"], dtype=str,
        keep_default_na=False, na_filter=False
    )
    if quals.shape[1] < 2:
        raise ValueError("QUALSCSV precisa ter ao menos duas colunas (código; descrição).")
    quals["cod"]  = quals["cod"].astype(str).str.strip()
    quals["desc"] = quals["desc"].astype(str).str.strip()
    return dict(zip(quals["cod"], quals["desc"]))

def load_pais_map(pais_glob: str) -> dict:
    pais_files = sorted(Path(".").glob(pais_glob))
    if not pais_files:
        print(f"Aviso: '{pais_glob}' não encontrado; país ficará em branco.")
        return {}
    pais = pd.read_csv(
        pais_files[0], sep=";", encoding="latin1", header=None,
        usecols=[0,1], names=["cod_pais","pais_txt"], dtype=str,
        keep_default_na=False, na_filter=False
    )
    pais["cod_pais"] = pais["cod_pais"].astype(str).str.strip()
    pais["pais_txt"] = pais["pais_txt"].astype(str).str.strip()
    return dict(zip(pais["cod_pais"], pais["pais_txt"]))

def main():
    ap = argparse.ArgumentParser(description="Merge de empresas com sócios dos SOCIOCSV (decodifica QUALSCSV e PAISCSV).")
    ap.add_argument("--in",  dest="input_empresas", required=True, help="CSV de entrada com coluna 'cnpj' (14 dígitos).")
    ap.add_argument("--out", dest="output_csv",    required=True, help="CSV de saída com sócios.")
    ap.add_argument("--chunksize", type=int, default=300_000, help="Tamanho do chunk (linhas) ao ler SOCIOCSV.")
    ap.add_argument("--soc-glob",  default="K*.SOCIOCSV*", help="Glob para arquivos de sócios (default: K*.SOCIOCSV*).")
    ap.add_argument("--qual-glob", default="*QUALSCSV*",   help="Glob para qualificação (default: *QUALSCSV*).")
    ap.add_argument("--pais-glob", default="*PAISCSV*",    help="Glob para países (default: *PAISCSV*).")
    ap.add_argument("--only", choices=["ALL","PF","PJ","EXT"], default="ALL",
                    help="Filtra tipo de sócio: PF(2), PJ(1), EXT(3). Default ALL.")
    ap.add_argument("--max-n", type=int, default=None, help="Limita total de linhas processadas (debug).")
    args = ap.parse_args()

    in_path  = Path(args.input_empresas)
    out_path = Path(args.output_csv)

    # ---------- 0) base de empresas ----------
    if not in_path.exists():
        raise FileNotFoundError(f"Não encontrei {in_path}")
    base = pd.read_csv(in_path, dtype=str, keep_default_na=False, na_filter=False)
    if "cnpj" not in base.columns:
        raise ValueError(f"{in_path.name} precisa ter a coluna 'cnpj' (14 dígitos).")

    base["cnpj"] = base["cnpj"].str.replace(r"\D", "", regex=True)
    base = base[base["cnpj"].str.len() == 14].copy()
    base["cnpj_basico"] = base["cnpj"].str[:8]

    EMP_FIELDS = [c for c in ["razao_social","porte_empresa_txt","capital_social","nome","endereco"] if c in base.columns]
    base_min = base[["cnpj_basico","cnpj"] + EMP_FIELDS].drop_duplicates("cnpj_basico")
    alvo_basicos = set(base_min["cnpj_basico"].unique().tolist())
    print(f"Empresas na base: {len(base):,} | cnpj_basico únicos: {len(alvo_basicos):,}")

    # ---------- 1) decodificadores ----------
    map_qual = load_qual_map(args.qual_glob)
    map_pais = load_pais_map(args.pais_glob)

    # ---------- 2) SOCIOCSV ----------
    soc_files = sorted(Path(".").glob(args.soc_glob))
    if not soc_files:
        raise FileNotFoundError(f"Nenhum arquivo '{args.soc_glob}' encontrado na pasta.")
    SOC_USECOL_IDX = list(range(11))
    SOC_COLS = [
        "cnpj_basico", "identificador_socio", "nome_socio_razao_social",
        "cnpj_cpf_socio", "cod_qualificacao_socio", "data_entrada_sociedade",
        "cod_pais", "cpf_representante_legal", "nome_representante_legal",
        "cod_qualificacao_representante", "faixa_etaria"
    ]

    # Filtro opcional de tipo de sócio
    only_map = {"ALL": None, "PF": "2", "PJ": "1", "EXT": "3"}
    only_code = only_map[args.only]

    # ---------- 3) varrer e gravar incremental ----------
    out_path.unlink(missing_ok=True)
    header_written = False
    total_out = 0
    processed = 0

    for soc in soc_files:
        print(f"Lendo sócios de: {soc.name}")
        try:
            for chunk in pd.read_csv(
                soc, sep=";", encoding="latin1", header=None,
                names=SOC_COLS, usecols=SOC_USECOL_IDX, dtype=str,
                keep_default_na=False, na_filter=False,
                low_memory=False, chunksize=args.chunksize, on_bad_lines="skip"
            ):
                # limita processamento para debug
                if args.max_n is not None and processed >= args.max_n:
                    break
                processed += len(chunk)

                # recorta universo: só cnpj_basico da base
                chunk = chunk[chunk["cnpj_basico"].isin(alvo_basicos)]
                if chunk.empty:
                    continue

                # filtro tipo de sócio (se solicitado)
                if only_code:
                    chunk = chunk[chunk["identificador_socio"] == only_code]
                    if chunk.empty:
                        continue

                # normalizações / derivados
                chunk["identificador_socio_txt"] = chunk["identificador_socio"].map(
                    lambda x: IDENT_MAP.get(s(x).strip(), "Desconhecido")
                )
                chunk["qualificacao_socio_txt"] = chunk["cod_qualificacao_socio"].map(
                    lambda x: map_qual.get(s(x).strip(), "")
                )
                chunk["qualificacao_representante_txt"] = chunk["cod_qualificacao_representante"].map(
                    lambda x: map_qual.get(s(x).strip(), "")
                )
                chunk["pais_txt"] = chunk["cod_pais"].map(
                    lambda x: map_pais.get(s(x).strip(), "")
                ) if map_pais else ""

                chunk["doc_socio"] = chunk["cnpj_cpf_socio"].map(so_digitos)
                chunk["doc_representante"] = chunk["cpf_representante_legal"].map(so_digitos)

                # merge com a base de empresas (m:1 pelo cnpj_basico)
                merged = chunk.merge(base_min, on="cnpj_basico", how="left", validate="m:1")
                if merged.empty:
                    continue

                # colunas de saída (só adiciona as que existirem)
                out_cols = [
                    # empresa
                    *(["razao_social"] if "razao_social" in merged.columns else []),
                    "cnpj",
                    *[c for c in ["nome","endereco","porte_empresa_txt","capital_social"] if c in merged.columns],
                    # socio
                    "identificador_socio","identificador_socio_txt",
                    "nome_socio_razao_social","doc_socio",
                    "cod_qualificacao_socio","qualificacao_socio_txt",
                    "data_entrada_sociedade","cod_pais","pais_txt",
                    # representante
                    "doc_representante","nome_representante_legal",
                    "cod_qualificacao_representante","qualificacao_representante_txt",
                    "faixa_etaria",
                ]

                merged[out_cols].to_csv(
                    out_path, index=False, mode="a",
                    header=(not header_written), encoding="utf-8"
                )
                header_written = True
                total_out += len(merged)

        except Exception as e:
            print(f"Falha em {soc.name}: {e}")

        if args.max_n is not None and processed >= args.max_n:
            print(f"Interrompido por --max-n ({args.max_n}).")
            break

    print(f"\nGerado: {out_path}  | linhas (sócios vinculados): {total_out}")
    print("Dica: use --only PF (ou PJ/EXT) se quiser restringir por tipo de sócio.")

if __name__ == "__main__":
    main()
