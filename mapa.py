#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# mapa.py — Gera APENAS um JSON com empresas + lat/lon (sem HTML).
#
# Exemplo:
#   python mapa.py \
#     --base empresas_tecnologia_sao_carlos_ativas.csv \
#     --enriched empresas_tecnologia_sc_ativas_com_socios.csv \
#     --geocache geocache_enderecos.csv \
#     --out-json empresas_tecnologia_sao_carlos.json \
#     --city "São Carlos" --uf "SP" \
#     --user-agent "empresas-mapper/1.0 (contact: seu-email@exemplo.com)" \
#     --max-geocode 200 --keep-missing
#
import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
from geopy.geocoders import Nominatim, ArcGIS
from geopy.extra.rate_limiter import RateLimiter

# ================== Utils ==================
def s(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)

def only_digits(x) -> str:
    return re.sub(r"\D", "", s(x))

def format_cnpj(cnpj14: str) -> str:
    c = only_digits(cnpj14).zfill(14)
    return f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:14]}"

def load_base(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for col in ["nome", "cnpj", "endereco"]:
        if col not in df.columns:
            raise ValueError(f"O arquivo {path} precisa ter a coluna '{col}'.")
    df["cnpj"] = df["cnpj"].map(only_digits)
    df = df[df["cnpj"].str.len() == 14].copy()
    df["endereco"] = df["endereco"].map(lambda x: s(x).strip())
    df = df[df["endereco"] != ""].copy()
    return df

def load_enriched_optional(path: Path) -> Optional[pd.DataFrame]:
    if not path or not path.exists():
        return None
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    if "cnpj" not in df.columns:
        return None
    df["cnpj"] = df["cnpj"].map(only_digits)
    df = df[df["cnpj"].str.len() == 14].copy()
    return df

def build_enriched_index(df_enr: Optional[pd.DataFrame], max_socios_in_json: int = 20) -> Dict[str, Any]:
    if df_enr is None or df_enr.empty:
        return {}
    df_enr = df_enr.copy()
    df_enr["cnpj"] = df_enr["cnpj"].astype(str)

    cols = set(df_enr.columns)
    if "nome_socio_razao_social" in cols:
        socios_grp = (
            df_enr[["cnpj", "nome_socio_razao_social"]]
            .assign(nome_socio_razao_social=lambda d: d["nome_socio_razao_social"].astype(str).str.strip())
            .query("nome_socio_razao_social != ''")
            .groupby("cnpj")["nome_socio_razao_social"]
            .apply(lambda serie: list(pd.unique(serie)))
            .rename("socios_list")
        )
    else:
        socios_grp = pd.Series(dtype=object, name="socios_list")

    def first_non_empty(serie: pd.Series) -> str:
        for v in serie:
            vv = s(v).strip()
            if vv:
                return vv
        return ""

    agg = {}
    if "porte_empresa_txt" in cols:
        agg["porte_empresa_txt"] = first_non_empty
    elif "porte_empresa" in cols:
        agg["porte_empresa"] = first_non_empty
    if "capital_social" in cols:
        agg["capital_social"] = first_non_empty
    if "razao_social" in cols:
        agg["razao_social"] = first_non_empty

    base_grp = df_enr.groupby("cnpj").agg(agg) if agg else pd.DataFrame(index=df_enr["cnpj"].unique())
    enr = base_grp.join(socios_grp, how="left")

    out: Dict[str, Any] = {}
    for cnpj, row in enr.iterrows():
        socios_list = row.get("socios_list", [])
        if not isinstance(socios_list, (list, tuple)):
            socios_list = []
        n_soc = len(socios_list)
        socios_short = socios_list[:max_socios_in_json] if n_soc > max_socios_in_json else socios_list
        porte = s(row.get("porte_empresa_txt")) or s(row.get("porte_empresa"))
        out[str(cnpj)] = {
            "porte": porte,
            "capital_social": s(row.get("capital_social")),
            "razao_social": s(row.get("razao_social")),
            "n_socios": n_soc,
            "socios": socios_short,
        }
    return out

def load_or_init_geocache(path: Path) -> pd.DataFrame:
    if path.exists():
        gc = pd.read_csv(path, dtype=str, keep_default_na=False)
        for c in ["query", "latitude", "longitude"]:
            if c not in gc.columns:
                gc[c] = ""
        return gc[["query", "latitude", "longitude"]].drop_duplicates("query")
    return pd.DataFrame(columns=["query", "latitude", "longitude"])

def save_geocache(df_cache: pd.DataFrame, path: Path):
    df_cache.drop_duplicates("query").to_csv(path, index=False)

# -------- normalização e candidatos --------
_PREFIXES = [
    "RUA","AVENIDA","AV","ALAMEDA","TRAVESSA","ESTRADA","RODOVIA","ROD",
    "PRAÇA","PRACA","LARGO","VIA","VIELA","SERVIDAO","SERVIDÃO","PARQUE"
]

def normalize_for_geocode(endereco_raw: str) -> Tuple[str, Optional[str]]:
    if not endereco_raw:
        return "", None
    t = str(endereco_raw).upper().strip()
    for p in _PREFIXES:
        t = re.sub(rf'\b{p}(?=[A-Z0-9])', f'{p} ', t)
    # "KM    148,8" -> "KM 148,8"
    t = re.sub(r'\bKM\s*([0-9]+[,\.]?[0-9]*)', r'KM \1', t)
    # remove " - NN/UF - "
    t = re.sub(r'\s*-\s*\d{2}\s*/\s*[A-Z]{2}\s*-?', ' ', t)
    # extrai CEP
    mcep = re.search(r'CEP\s*([0-9]{5}-?[0-9]{3})', t)
    cep: Optional[str] = None
    if mcep:
        cep = mcep.group(1)
        t = t[:mcep.start()] + t[mcep.end():]
    # limpeza
    t = t.replace(' - ', ', ')
    t = re.sub(r'\s*,\s*', ', ', t)
    t = re.sub(r'\s{2,}', ' ', t).strip(' ,;-')
    return t, cep

# ================== Geocodificação ROBUSTA ==================
def fetch_city_bbox(geolocator: Nominatim, city: str, uf: str, country: str = "Brasil") -> Optional[Tuple[float, float, float, float]]:
    loc = geolocator.geocode({"city": city, "state": uf, "country": country},
                             addressdetails=True, exactly_one=True, country_codes="br")
    if not loc:
        return None
    bb = (loc.raw or {}).get("boundingbox")
    if not bb or len(bb) != 4:
        return None
    south, north, west, east = map(float, bb)  # nominatim: [south, north, west, east]
    return (west, south, east, north)

def point_in_bbox(lat: float, lon: float, bbox: Optional[Tuple[float, float, float, float]]) -> bool:
    if not bbox:
        return True
    west, south, east, north = bbox
    return (south <= lat <= north) and (west <= lon <= east)

def _addr_city_like(addr: Dict[str, str]) -> str:
    return addr.get("city") or addr.get("town") or addr.get("municipality") or addr.get("village") or ""

def is_valid_hit(loc, bbox: Optional[Tuple[float, float, float, float]]) -> bool:
    """Se tenho bbox, aceito coordenada apenas por cair dentro dela (relaxa checagem textual)."""
    raw = getattr(loc, "raw", {}) or {}
    try:
        lat = float(loc.latitude); lon = float(loc.longitude)
    except Exception:
        return False
    # recusa centroides óbvios se tivermos alternativa depois
    cls, typ = raw.get("class"), raw.get("type")
    is_boundary = (cls == "boundary") and (typ in {"administrative", "city", "town", "municipality"})
    if is_boundary and not bbox:
        return False
    return point_in_bbox(lat, lon, bbox)

def build_candidates(endereco_raw: str, cidade: str, uf: str, country: str = "Brasil") -> List[Any]:
    base, cep = normalize_for_geocode(endereco_raw)
    cands: List[Any] = []
    if base and cep:
        cands.append({"street": base, "city": cidade, "state": uf, "country": country, "postalcode": cep})
    if base:
        cands.append({"street": base, "city": cidade, "state": uf, "country": country})
    if cep:
        cands.append({"postalcode": cep, "city": cidade, "state": uf, "country": country})
    if base:
        cands.append(f"{base}, {cidade}, {uf}, {country}")
    # de-dup
    seen = set(); uniq: List[Any] = []
    for q in cands:
        key = json.dumps(q, ensure_ascii=False, sort_keys=True) if isinstance(q, dict) else str(q)
        if key not in seen:
            uniq.append(q); seen.add(key)
    return uniq

def geocode_with_candidates(nominate, arcgis, candidates: List[Any],
                            bbox: Optional[Tuple[float, float, float, float]],
                            cidade: str, uf: str) -> Tuple[str, str, str, str, Dict[str, Any]]:
    """Tenta Nominatim, depois ArcGIS. Retorna (lat, lon, provider, display_name, rawaddr)."""
    vb = None
    if bbox:
        west, south, east, north = bbox
        vb = ((south, west), (north, east))

    # 1) Nominatim
    for q in candidates:
        try:
            loc = nominate(
                q, exactly_one=True, addressdetails=True,
                country_codes="br", viewbox=vb, bounded=bool(vb)
            )
            if loc and is_valid_hit(loc, bbox):
                return (f"{loc.latitude}", f"{loc.longitude}", "nominatim",
                        getattr(loc, "address", None) or getattr(loc, "raw", {}).get("display_name", ""), loc.raw or {})
        except Exception:
            continue

    # 2) ArcGIS (string livre é o que funciona melhor).
    for q in candidates:
        try:
            q_str = q if isinstance(q, str) else ", ".join([str(v) for v in q.values()])
            q_str = f"{q_str}, {cidade}, {uf}, Brasil"
            loc = arcgis(q_str, out_fields="*")
            if not loc:
                continue
            lat, lon = float(loc.latitude), float(loc.longitude)
            if point_in_bbox(lat, lon, bbox):
                # ArcGIS não expõe addressdetails do OSM, guardo raw mínimo
                return (f"{lat}", f"{lon}", "arcgis",
                        getattr(loc, "address", ""), {"provider": "arcgis"})
        except Exception:
            continue

    return "", "", "", "", {}

def geocode_addresses(df_base: pd.DataFrame, geocache: pd.DataFrame,
                      user_agent: str, cidade: str, uf: str,
                      max_geocode: Optional[int]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    nominatim = Nominatim(user_agent=user_agent, timeout=10)
    nominate = RateLimiter(nominatim.geocode, min_delay_seconds=1.0)

    arc = ArcGIS(timeout=10)
    arc_geocode = RateLimiter(arc.geocode, min_delay_seconds=1.0)

    bbox = fetch_city_bbox(nominatim, city=cidade, uf=uf, country="Brasil")
    if not bbox:
        print("Aviso: não consegui obter bounding box da cidade; seguindo sem restrição espacial.")

    df_base = df_base.copy()
    df_base["query"] = df_base["endereco"].astype(str)

    geocache = geocache.copy()
    geocache["query"] = geocache["query"].astype(str)
    cache_map = dict(zip(geocache["query"], zip(geocache["latitude"], geocache["longitude"])))

    pend = df_base[~df_base["query"].isin(cache_map.keys())]["query"].drop_duplicates().tolist()
    print(f"Endereços a geocodificar: {len(pend)} (em cache: {len(cache_map)})")
    if max_geocode is not None:
        pend = pend[:max_geocode]
        print(f"Limitado a {len(pend)} para teste (--max-geocode).")

    novos = []
    succ_rows = []
    fail_rows = []
    ok, falhas = 0, 0

    for i, q in enumerate(pend, 1):
        cands = build_candidates(q, cidade=cidade, uf=uf, country="Brasil")
        lat, lon, provider, disp, raw = geocode_with_candidates(
            nominate, arc_geocode, cands, bbox, cidade, uf
        )
        if lat and lon:
            ok += 1
            succ_rows.append({
                "query": q,
                "provider": provider,
                "latitude": lat,
                "longitude": lon,
                "display": s(disp)
            })
        else:
            falhas += 1
            fail_rows.append({
                "query": q,
                "candidates": json.dumps(cands, ensure_ascii=False),
                "reason": "no_hit_all"
            })

        novos.append({"query": q, "latitude": lat, "longitude": lon})

        if i % 50 == 0:
            tmp = pd.DataFrame(novos)
            if not tmp.empty:
                geocache = pd.concat([geocache, tmp], ignore_index=True)
                novos = []
            # salva parciais de log
            if succ_rows:
                pd.DataFrame(succ_rows).to_csv("geocode_success.csv", index=False)
            if fail_rows:
                pd.DataFrame(fail_rows).to_csv("geocode_failures.csv", index=False)
            print(f"[{i}/{len(pend)}] resolvidos: {ok} | falhas: {falhas} | cache/logs parciais…")

    if novos:
        geocache = pd.concat([geocache, pd.DataFrame(novos)], ignore_index=True)

    # dumps finais de log
    if succ_rows:
        pd.DataFrame(succ_rows).to_csv("geocode_success.csv", index=False)
    if fail_rows:
        pd.DataFrame(fail_rows).to_csv("geocode_failures.csv", index=False)

    print(f"Geocodificação concluída. Sucesso: {ok} | Falhas: {falhas}")

    cache_map = dict(zip(geocache["query"], zip(geocache["latitude"], geocache["longitude"])))
    df_base[["latitude", "longitude"]] = df_base["query"].map(cache_map).apply(pd.Series)
    return df_base, geocache

# ================== MAIN ==================
def main():
    ap = argparse.ArgumentParser(description="Gera JSON com empresas + geocodificação (sem HTML).")
    ap.add_argument("--base", required=True, help="CSV base (colunas: nome, cnpj, endereco).")
    ap.add_argument("--enriched", default=None, help="CSV enriquecido opcional (ex.: *_com_socios.csv).")
    ap.add_argument("--geocache", default="geocache_enderecos.csv", help="CSV de cache de geocodificação.")
    ap.add_argument("--out-json", required=True, help="Caminho do JSON de saída.")
    ap.add_argument("--city", default="São Carlos", help="Cidade a forçar na geocodificação.")
    ap.add_argument("--uf", default="SP", help="UF a forçar na geocodificação.")
    ap.add_argument("--user-agent", default="empresas-mapper/1.0 (contact: seu-email@exemplo.com)",
                    help="User-Agent para Nominatim (coloque um contato válido).")
    ap.add_argument("--max-geocode", type=int, default=None, help="Limite de endereços novos para geocodificar.")
    ap.add_argument("--keep-missing", action="store_true",
                    help="Mantém itens sem lat/lon no JSON (por padrão, são removidos).")
    args = ap.parse_args()

    keep_missing = getattr(args, "keep_missing", False)

    base_path = Path(args.base)
    enr_path = Path(args.enriched) if args.enriched else None
    cache_path = Path(args.geocache)
    out_json = Path(args.out_json)

    print("1) Lendo base…")
    base = load_base(base_path)

    print("2) Carregando enriquecido (opcional)…")
    enr_df = load_enriched_optional(enr_path) if enr_path else None
    enr_idx = build_enriched_index(enr_df) if enr_df is not None else {}

    print("3) Carregando cache…")
    geocache = load_or_init_geocache(cache_path)

    print("4) Geocodificando…")
    df_geo, geocache = geocode_addresses(
        base, geocache, user_agent=args.user_agent,
        cidade=args.city, uf=args.uf, max_geocode=args.max_geocode
    )

    print("5) Salvando cache…")
    save_geocache(geocache, cache_path)

    print("6) Gerando JSON…")
    data: Dict[str, Any] = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "source_base": str(base_path),
            "source_enriched": str(enr_path) if enr_path else None,
            "geocache": str(cache_path),
            "city_hint": args.city,
            "uf_hint": args.uf,
            "count_input": int(len(df_geo)),
        },
        "features": []
    }

    for _, r in df_geo.iterrows():
        cnpj = s(r["cnpj"])
        nome = s(r.get("nome"))
        endereco = s(r.get("endereco"))
        lat = s(r.get("latitude"))
        lon = s(r.get("longitude"))

        if (not keep_missing) and (lat == "" or lon == ""):
            continue

        item: Dict[str, Any] = {
            "cnpj": cnpj,
            "cnpj_formatado": format_cnpj(cnpj),
            "nome": nome,
            "endereco": endereco,
            "latitude": float(lat) if lat else None,
            "longitude": float(lon) if lon else None,
            "query_geocode": s(r.get("query", "")),
        }

        if cnpj in enr_idx:
            info = enr_idx[cnpj]
            item.update({
                "razao_social": info.get("razao_social", ""),
                "porte": info.get("porte", ""),
                "capital_social": info.get("capital_social", ""),
                "n_socios": int(info.get("n_socios", 0) or 0),
                "socios": info.get("socios", []),
            })

        data["features"].append(item)

    data["meta"]["count_output"] = len(data["features"])
    out_json.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"JSON salvo em: {out_json}  (itens: {data['meta']['count_output']})")

if __name__ == "__main__":
    main()
    