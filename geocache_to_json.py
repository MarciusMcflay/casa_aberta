#!/usr/bin/env python
# -*- coding: utf-8 -*-

# python geocache_to_json.py \
#   --geocache geocache_enderecos.csv \
#   --out-json empresas_tecnologia_sao_carlos.json \
#   --city "São Carlos" --uf SP

import argparse, json
from datetime import datetime
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Converte geocache_enderecos.csv em JSON para o mapa.")
    ap.add_argument("--geocache", default="geocache_enderecos.csv", help="CSV com colunas: query,latitude,longitude")
    ap.add_argument("--out-json", default="empresas_tecnologia_sao_carlos.json", help="Arquivo JSON de saída")
    ap.add_argument("--city", default="São Carlos", help="Meta opcional: cidade")
    ap.add_argument("--uf", default="SP", help="Meta opcional: UF")
    args = ap.parse_args()

    gc_path = Path(args.geocache)
    if not gc_path.exists():
        raise SystemExit(f"Arquivo não encontrado: {gc_path}")

    df = pd.read_csv(gc_path, dtype=str, keep_default_na=False)
    # normaliza colunas esperadas
    for c in ["query","latitude","longitude"]:
        if c not in df.columns:
            df[c] = ""
    # tira linhas sem coordenadas
    df = df[(df["latitude"]!="") & (df["longitude"]!="")].copy()
    # remove duplicatas por endereço (query)
    df = df.drop_duplicates(subset=["query"])

    features = []
    for _, r in df.iterrows():
        end = r["query"].strip()
        lat = r["latitude"].strip()
        lon = r["longitude"].strip()
        if not end or not lat or not lon:
            continue
        features.append({
            "cnpj": "",
            "cnpj_formatado": "",
            "nome": end,            # sem base: usamos o próprio endereço como nome
            "endereco": end,
            "latitude": float(lat),
            "longitude": float(lon),
            "query_geocode": end
        })

    data = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "source_geocache": str(gc_path),
            "city_hint": args.city,
            "uf_hint": args.uf,
            "count_input": int(len(df)),
            "count_output": int(len(features)),
        },
        "features": features
    }

    out = Path(args.out_json)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK! JSON gerado: {out}  (itens: {len(features)})")

if __name__ == "__main__":
    main()
