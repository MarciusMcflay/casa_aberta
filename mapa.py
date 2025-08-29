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
#     --user-agent "empresas-mapper/1.0 (contact: contato@exemplo.com)" \
#     --max-geocode 200 --keep-missing
#
from __future__ import annotations
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

# -------- normalização e candidatos p/ geocodificação --------
_PREFIXES = [
    "RUA","AVENIDA","AV","ALAMEDA","TRAVESSA","ESTRADA","RODOVIA","ROD",
    "PRAÇA","PRACA","LARGO","VIA","VIELA","SERVIDAO","SERVIDÃO","PARQUE"
]
# palavras de complemento a remover
_COMPLEMENTS = [
    "SALA","SL","TERREO","TÉRREO","FUNDOS","LOJA","APTO","APT","AP",
    "CJ","CONJ","SOBRADO","GALPAO","GALPÃO","BOX","BLOCO","BL","ANDAR"
]

def _strip_parentheses(t: str) -> str:
    return re.sub(r"\([^)]*\)", "", t)

def normalize_for_geocode(endereco_raw: str) -> tuple[str, Optional[str], dict]:
    """
    Retorna:
      - texto base (rua[, numero][, bairro])
      - CEP (ou None)
      - metadados extraídos: {street, number, neighbourhood, has_km, expects_house}
    """
    meta = {"street":"", "number":"", "neighbourhood":"", "has_km":False, "expects_house":False}
    if not endereco_raw:
        return "", None, meta

    t = str(endereco_raw).upper().strip()
    t = _strip_parentheses(t)

    # inserir espaço após prefixos colados
    for p in _PREFIXES:
        t = re.sub(rf'\b{p}(?=[A-Z0-9])', f'{p} ', t)

    # normaliza KM
    t = re.sub(r'\bKM\s*([0-9]+[,\.]?[0-9]*)', r'KM \1', t)
    # vírgula decimal -> ponto
    t = re.sub(r'(\d),(\d)', r'\1.\2', t)

    # remove " - NN/UF - "
    t = re.sub(r'\s*-\s*\d{2}\s*/\s*[A-Z]{2}\s*-?', ' ', t)

    # extrai CEP e remove do texto
    mcep = re.search(r'CEP\s*([0-9]{5}-?[0-9]{3})', t)
    cep: Optional[str] = None
    if mcep:
        cep = mcep.group(1)
        t = (t[:mcep.start()] + t[mcep.end():]).strip()

    # remove complementos muito específicos após traço
    for w in _COMPLEMENTS:
        t = re.sub(rf'\s-\s{w}\b.*$', '', t)

    # normaliza separadores
    t = t.replace(' - ', ', ')
    t = re.sub(r'\s*,\s*', ', ', t)
    t = re.sub(r'\s{2,}', ' ', t).strip(' ,;-')

    # tenta extrair "rua, numero, bairro"
    m = re.match(r'^(?P<street>[^,]+?)(?:,\s*(?P<number>[^,]+))?(?:,\s*(?P<bairro>[^,]+))?$', t)
    if m:
        street = m.group('street') or ''
        number = (m.group('number') or '').strip()
        bairro = (m.group('bairro') or '').strip()
    else:
        street, number, bairro = t, '', ''

    # flag de KM
    if re.search(r'\bKM\s*\d', t):
        meta["has_km"] = True

    # quando há número explícito (ou S/N) esperamos precisão de casa
    if number and (number.isdigit() or "S/N" in number or "S\\N" in number):
        meta["expects_house"] = True

    meta["street"] = street.strip()
    meta["number"] = number
    meta["neighbourhood"] = bairro

    # base para candidates:
    base_parts = [street.strip()]
    if number:
        base_parts.append(number.strip())
    if bairro:
        base_parts.append(bairro.strip())
    base = ", ".join([p for p in base_parts if p])

    return base, cep, meta

# ================== Geocodificação ROBUSTA ==================
def fetch_city_bbox(geolocator: Nominatim, city: str, uf: str, country: str = "Brasil") -> tuple[Optional[Tuple[float,float,float,float]], Optional[Tuple[float,float]]]:
    """Retorna (bbox_wsen, centroid_latlon)."""
    loc = geolocator.geocode(
        {"city": city, "state": uf, "country": country},
        addressdetails=True, exactly_one=True, country_codes="br"
    )
    if not loc:
        return None, None
    bb_raw = (getattr(loc, "raw", {}) or {}).get("boundingbox")
    bbox = None
    if bb_raw and len(bb_raw) == 4:
        south, north, west, east = map(float, bb_raw)
        bbox = (west, south, east, north)
    centroid = (float(loc.latitude), float(loc.longitude))
    return bbox, centroid

def point_in_bbox(lat: float, lon: float, bbox: Optional[Tuple[float, float, float, float]]) -> bool:
    if not bbox:
        return True
    west, south, east, north = bbox
    return (south <= lat <= north) and (west <= lon <= east)

def _addr_city_like(addr: Dict[str, str]) -> str:
    return addr.get("city") or addr.get("town") or addr.get("municipality") or addr.get("village") or ""

def is_boundary_or_place_city(raw: dict) -> bool:
    cl = (raw or {}).get("class")
    ty = (raw or {}).get("type")
    if cl == "boundary" and ty in {"administrative","city","town","municipality"}:
        return True
    if cl == "place" and ty in {"city","town","village","hamlet","municipality"}:
        return True
    return False

def is_valid_nominatim(loc, city_expect: str, uf_expect: str, bbox: Optional[Tuple[float,float,float,float]], expects_house: bool) -> bool:
    raw = getattr(loc, "raw", {}) or {}
    if is_boundary_or_place_city(raw):
        return False

    addr = raw.get("address", {}) or {}
    if addr.get("country_code", "").lower() != "br":
        return False

    city_hit = _addr_city_like(addr)
    state_code = (addr.get("state_code") or "").upper()
    state = (addr.get("state") or "").upper()
    ok_city = city_expect.upper() in city_hit.upper()
    ok_uf = (uf_expect.upper() == state_code) or (uf_expect.upper() in state)
    if not (ok_city and ok_uf):
        return False

    try:
        lat = float(loc.latitude); lon = float(loc.longitude)
    except Exception:
        return False

    if not point_in_bbox(lat, lon, bbox):
        return False

    # precisão mínima: se esperamos casa, tente garantir número ou tipo aderente
    if expects_house:
        house_num = (addr.get("house_number") or "").strip()
        ty = (raw.get("type") or "").lower()
        if not house_num and ty not in {"house","building","residential"}:
            return False

    return True

def is_valid_arcgis(loc, city_expect: str, uf_expect: str, bbox: Optional[Tuple[float,float,float,float]], expects_house: bool) -> bool:
    # ArcGIS: confere UF/cidade no address + bbox
    try:
        addr_text = (getattr(loc, "address", "") or "").upper()
        lat = float(loc.latitude); lon = float(loc.longitude)
    except Exception:
        return False
    if not point_in_bbox(lat, lon, bbox):
        return False
    ok_city = city_expect.upper() in addr_text
    ok_uf = (f", {uf_expect.upper()} " in addr_text) or addr_text.endswith(f", {uf_expect.upper()}")
    if not (ok_city and ok_uf):
        return False
    # sem house number explícito no raw — relaxa levemente
    return True

def build_candidates(endereco_raw: str, cidade: str, uf: str, country: str = "Brasil") -> Tuple[List[Any], dict]:
    base, cep, meta = normalize_for_geocode(endereco_raw)
    cands: List[Any] = []

    # preferir consulta estruturada
    if base and cep:
        cands.append({"street": base, "city": cidade, "state": uf, "country": country, "postalcode": cep})
    if base:
        cands.append({"street": base, "city": cidade, "state": uf, "country": country})
    # variantes: só rua (sem bairro/num) ajuda quando parser erra
    rua = meta["street"]
    if rua:
        street_only = rua + (f", {meta['number']}" if meta["number"] else "")
        cands.append({"street": street_only, "city": cidade, "state": uf, "country": country})
    if cep:
        cands.append({"postalcode": cep, "city": cidade, "state": uf, "country": country})
    # fallback livre
    if base:
        cands.append(f"{base}, {cidade}, {uf}, {country}")

    # de-dup mantendo ordem
    seen = set(); uniq: List[Any] = []
    for q in cands:
        key = json.dumps(q, ensure_ascii=False, sort_keys=True) if isinstance(q, dict) else str(q)
        if key not in seen:
            uniq.append(q); seen.add(key)
    return uniq, meta

def geocode_with_providers(
    nom_geocode, arc_geocode, candidates: List[Any],
    bbox: Optional[Tuple[float,float,float,float]],
    cidade: str, uf: str, expects_house: bool
) -> Tuple[str, str, str, str]:
    """
    Tenta Nominatim e depois ArcGIS. Retorna (lat, lon, provider, reason_if_fail_or_empty_string).
    """
    # tenta Nominatim
    for q in candidates:
        try:
            loc = nom_geocode(q, exactly_one=True, addressdetails=True, country_codes="br",
                              viewbox=((bbox[1], bbox[0]), (bbox[3], bbox[2])) if bbox else None,
                              bounded=bool(bbox))
            if loc and is_valid_nominatim(loc, cidade, uf, bbox, expects_house):
                return f"{loc.latitude}", f"{loc.longitude}", "nominatim", ""
        except Exception as e:
            pass  # tenta próximo candidato/provedor

    # tenta ArcGIS
    for q in candidates:
        try:
            # ArcGIS não entende dicts — usa string sempre
            q_str = q if isinstance(q, str) else ", ".join([str(v) for v in q.values()])
            loc = arc_geocode(q_str, exactly_one=True)
            if loc and is_valid_arcgis(loc, cidade, uf, bbox, expects_house):
                return f"{loc.latitude}", f"{loc.longitude}", "arcgis", ""
        except Exception as e:
            pass

    return "", "", "", "no_valid_hit"

def geocode_addresses(
    df_base: pd.DataFrame, geocache: pd.DataFrame,
    user_agent: str, cidade: str, uf: str,
    max_geocode: Optional[int],
    failures_log: Path
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    nominatim = Nominatim(user_agent=user_agent, timeout=10)
    nom_geocode = RateLimiter(nominatim.geocode, min_delay_seconds=1.0)

    arcgis = ArcGIS(timeout=10)
    arc_geocode = RateLimiter(arcgis.geocode, min_delay_seconds=0.5)  # ArcGIS tolera um pouco mais

    # bbox e centroide da cidade
    bbox, centroid = fetch_city_bbox(nominatim, city=cidade, uf=uf, country="Brasil")
    if not bbox:
        print("Aviso: não consegui obter bounding box da cidade; seguindo sem restrição espacial.")
    city_centroid = centroid  # (lat, lon) para detectar centroide

    df_base = df_base.copy()
    df_base["query"] = df_base["endereco"].astype(str)

    # cache -> dict
    geocache = geocache.copy()
    geocache["query"] = geocache["query"].astype(str)
    cache_map = dict(zip(geocache["query"], zip(geocache["latitude"], geocache["longitude"])))

    # ignora do cache pontos “suspeitos” (centroide da cidade) quando endereço exige precisão
    cleaned_cache = {}
    for q, (lat, lon) in cache_map.items():
        try:
            latf, lonf = float(lat), float(lon)
        except Exception:
            continue
        expects_house = bool(re.search(r',\s*\d|S/?N\b|KM\s*\d', q.upper()))
        if city_centroid and expects_house:
            clat, clon = city_centroid
            # ~1km de raio em graus ~ 0.009; se muito perto do centroide, descarta
            if abs(latf - clat) < 0.009 and abs(lonf - clon) < 0.009:
                continue
        cleaned_cache[q] = (lat, lon)
    cache_map = cleaned_cache

    pend = df_base[~df_base["query"].isin(cache_map.keys())]["query"].drop_duplicates().tolist()
    print(f"Endereços a geocodificar: {len(pend)} (em cache útil: {len(cache_map)})")
    if max_geocode is not None:
        pend = pend[:max_geocode]
        print(f"Limitado a {len(pend)} para teste (--max-geocode).")

    # logger de falhas
    fail_rows: List[dict] = []

    novos = []
    ok, falhas = 0, 0
    for i, q in enumerate(pend, 1):
        cands, meta = build_candidates(q, cidade=cidade, uf=uf, country="Brasil")
        lat, lon, provider, why = geocode_with_providers(
            nom_geocode, arc_geocode, cands, bbox, cidade, uf, meta.get("expects_house", False)
        )
        if lat and lon:
            ok += 1
        else:
            falhas += 1
            fail_rows.append({
                "query": q,
                "norm_base": cands[0] if cands else "",
                "expects_house": meta.get("expects_house", False),
                "has_km": meta.get("has_km", False),
                "candidates": json.dumps(cands, ensure_ascii=False),
                "reason": why
            })
        novos.append({"query": q, "latitude": lat, "longitude": lon})

        if i % 50 == 0:
            tmp = pd.DataFrame(novos)
            if not tmp.empty:
                geocache = pd.concat([geocache, tmp], ignore_index=True)
                novos = []
            if fail_rows:
                pd.DataFrame(fail_rows).to_csv(failures_log, index=False, encoding="utf-8")
            print(f"[{i}/{len(pend)}] resolvidos: {ok} | falhas: {falhas} | cache/log parcial…")

    if novos:
        geocache = pd.concat([geocache, pd.DataFrame(novos)], ignore_index=True)
    if fail_rows:
        pd.DataFrame(fail_rows).to_csv(failures_log, index=False, encoding="utf-8")

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
    ap.add_argument("--city", default="São Carlos", help="Cidade a forçar na geocodificação (default: São Carlos).")
    ap.add_argument("--uf", default="SP", help="UF a forçar na geocodificação (default: SP).")
    ap.add_argument("--user-agent", default="empresas-mapper/1.0 (contact: contato@exemplo.com)",
                    help="User-Agent para Nominatim (coloque um contato válido).")
    ap.add_argument("--max-geocode", type=int, default=None, help="Limite de endereços novos para geocodificar (teste).")
    ap.add_argument("--keep-missing", action="store_true",
                    help="Mantém itens sem lat/lon no JSON (por padrão, são removidos).")
    ap.add_argument("--fail-log", default="geocode_failures.csv",
                    help="Arquivo CSV para logar falhas de geocodificação.")
    args = ap.parse_args()

    keep_missing = getattr(args, "keep_missing", False)

    base_path = Path(args.base)
    enr_path = Path(args.enriched) if args.enriched else None
    cache_path = Path(args.geocache)
    out_json = Path(args.out_json)
    fail_log = Path(args.fail_log)

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
        cidade=args.city, uf=args.uf, max_geocode=args.max_geocode,
        failures_log=fail_log
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
    if fail_log.exists():
        print(f"Falhas logadas em: {fail_log}")

if __name__ == "__main__":
    main()
