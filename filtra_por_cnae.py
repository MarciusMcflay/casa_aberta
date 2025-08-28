# filtra_por_cnae.py
# Uso:
#   python filtra_por_cnae.py --in empresas_ativas_sc.csv --cnae 6201501 6311900 6319400 --out empresas_sc_cnaes.csv
#
# Entrada: CSV do passo 1 com colunas: nome, cnpj, endereco, cnae_fiscal_principal, cnaes_secundarios, municipio, uf
# Saída:   nome, cnpj, endereco, cnae_fiscal_principal, cnaes_secundarios, match_por, municipio, uf

import argparse
import re
import pandas as pd
from pathlib import Path

def s(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)

def extrai_7digs(txt: str):
    if not txt:
        return set()
    return set(re.findall(r"\d{7}", s(txt)))

def main():
    ap = argparse.ArgumentParser(description="Filtra a saída do filtro_cidades_ativas por uma lista de CNAEs (7 dígitos).")
    ap.add_argument("--in", dest="input_csv", required=True, help="CSV do passo 1 (precisa ter cnae_fiscal_principal e cnaes_secundarios).")
    ap.add_argument("--cnae", nargs="+", required=True, help="Lista de CNAEs (7 dígitos), separados por espaço (e/ou vírgula).")
    ap.add_argument("--out", default="empresas_filtradas_por_cnae.csv", help="Arquivo de saída.")
    args = ap.parse_args()

    # normaliza CNAEs: aceita espaço ou vírgula
    raw = []
    for item in args.cnae:
        raw += re.split(r"[,\s]+", item.strip())
    cnaes = {re.sub(r"\D", "", c) for c in raw if c.strip()}
    cnaes = {c for c in cnaes if len(c) == 7}
    if not cnaes:
        raise SystemExit("Nenhum CNAE válido (7 dígitos) informado.")

    path = Path(args.input_csv)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)

    required = {"nome","cnpj","endereco","cnae_fiscal_principal","cnaes_secundarios","municipio","uf"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"O CSV de entrada precisa conter as colunas: {sorted(required)}. Ausentes: {sorted(missing)}")

    # avalia match principal/secundário
    df["m_principal"] = df["cnae_fiscal_principal"].isin(cnaes)
    df["m_sec"] = df["cnaes_secundarios"].map(lambda x: len(extrai_7digs(x) & cnaes) > 0)
    sel = df[df["m_principal"] | df["m_sec"]].copy()

    # classificador
    def tag(row):
        if row["m_principal"] and row["m_sec"]:
            return "ambos"
        return "principal" if row["m_principal"] else "secundario"

    if sel.empty:
        sel = df.head(0).copy()
        sel["match_por"] = ""

    else:
        sel["match_por"] = sel.apply(tag, axis=1)

    out_cols = ["nome","cnpj","endereco","cnae_fiscal_principal","cnaes_secundarios","match_por","municipio","uf"]
    sel[out_cols].sort_values(["nome","cnpj"]).to_csv(args.out, index=False, encoding="utf-8")
    print(f"Gerado: {args.out}  (linhas: {len(sel)})")

if __name__ == "__main__":
    main()
