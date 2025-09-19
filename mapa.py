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

def split_multi(cell: str) -> List[str]:
    """Divide strings com múltiplos valores (separados por ; , /) e limpa."""
    vals = []
    for part in re.split(r"[;,/]\s*", s(cell)):
        p = part.strip()
        if p:
            vals.append(p)
    return list(dict.fromkeys(vals))  # dedup preservando ordem

# -------- coleta contatos do ROW (fallback/local) --------
def collect_contacts_from_row(row: pd.Series) -> Dict[str, List[str]]:
    emails: List[str] = []
    phones: List[str] = []

    # E-mails diretos
    for c in row.index:
        cl = c.lower()
        if "email" in cl:
            emails.extend(split_multi(row.get(c)))

    # Telefones normalizados preferenciais
    for c in ["telefone_1_full", "telefone_2_full", "telefones_norm"]:
        if c in row:
            phones.extend(split_multi(row.get(c)))

    # Telefones brutos (ddd + tel) — normaliza para +55DDDTEL apenas se não existir acima
    def norm_tel(ddd, tel):
        d = re.sub(r"\D", "", s(ddd))
        t = re.sub(r"\D", "", s(tel))
        if not t:
            return ""
        if d and not t.startswith(d):
            return f"+55{d}{t}"
        return f"+55{t}" if not t.startswith("55") else f"+{t}"

    if not phones:
        d1, t1 = row.get("ddd_1"), row.get("telefone_1")
        d2, t2 = row.get("ddd_2"), row.get("telefone_2")
        cand = [norm_tel(d1, t1), norm_tel(d2, t2)]
        phones.extend([x for x in cand if x])

    # Limpa/dedup
    emails = [e for e in emails if e]
    phones = [p for p in phones if p]
    emails = list(dict.fromkeys(emails))
    phones = list(dict.fromkeys(phones))
    return {"emails": emails, "phones": phones}

# constroi dados
def build_enriched_index(df_enr: Optional[pd.DataFrame], max_socios_in_json: int = 20) -> Dict[str, Any]:
    """
    Constrói índice por CNPJ com: razao_social, porte, capital_social,
    lista de socios (strings ou dicts), e também emails[] e phones[].
    """
    if df_enr is None or df_enr.empty:
        return {}

    df = df_enr.copy()
    df["cnpj"] = df["cnpj"].astype(str)

    # Detect email / phone columns (heurística)
    email_cols = [c for c in df.columns if "email" in c.lower()]
    phone_cols = [c for c in df.columns if any(k in c.lower() for k in ("fone", "tel", "telefone", "phone"))]

    has_socio_name = "nome_socio_razao_social" in df.columns
    socio_email_cols = [c for c in email_cols if "socio" in c.lower() or "sócio" in c.lower()]
    socio_phone_cols = [c for c in phone_cols if "socio" in c.lower() or "sócio" in c.lower()]

    def first_non_empty(serie: pd.Series) -> str:
        for v in serie:
            vv = s(v).strip()
            if vv:
                return vv
        return ""

    # Agregação básica (razao, porte, capital)
    agg = {}
    if "porte_empresa_txt" in df.columns:
        agg["porte_empresa_txt"] = first_non_empty
    elif "porte_empresa" in df.columns:
        agg["porte_empresa"] = first_non_empty
    if "capital_social" in df.columns:
        agg["capital_social"] = first_non_empty
    if "razao_social" in df.columns:
        agg["razao_social"] = first_non_empty

    base_grp = df.groupby("cnpj").agg(agg) if agg else pd.DataFrame(index=df["cnpj"].unique())

    def collect_unique(group, cols_to_check):
        vals = []
        for c in cols_to_check:
            if c in group:
                vals.extend([s(v).strip() for v in group[c].tolist() if s(v).strip()])
        cleaned = []
        for v in vals:
            parts = re.split(r'[;,/]\s*', v)
            for p in parts:
                p = p.strip()
                if p and p not in cleaned:
                    cleaned.append(p)
        return cleaned

    emails_grp: Dict[str, List[str]] = {}
    phones_grp: Dict[str, List[str]] = {}
    socios_grp: Dict[str, List[Any]] = {}

    for cnpj, group in df.groupby("cnpj"):
        # company emails / phones
        emails = collect_unique(group, email_cols) if email_cols else []
        phones = collect_unique(group, phone_cols) if phone_cols else []

        # sócios com possíveis contatos
        socios_out = []
        if has_socio_name:
            for _, row in group.iterrows():
                nome = s(row.get("nome_socio_razao_social")).strip()
                if not nome:
                    continue
                socio = {"name": nome}
                for ec in (socio_email_cols or email_cols):
                    if ec in row and s(row.get(ec)).strip():
                        socio.setdefault("emails", []).extend(split_multi(row.get(ec)))
                for pc in (socio_phone_cols or phone_cols):
                    if pc in row and s(row.get(pc)).strip():
                        socio.setdefault("phones", []).extend(split_multi(row.get(pc)))
                if "emails" in socio:
                    socio["emails"] = list(dict.fromkeys([x for x in socio["emails"] if x]))
                if "phones" in socio:
                    socio["phones"] = list(dict.fromkeys([x for x in socio["phones"] if x]))
                socios_out.append(socio)

        emails = list(dict.fromkeys(emails))
        phones = list(dict.fromkeys(phones))
        emails_grp[str(cnpj)] = emails
        phones_grp[str(cnpj)] = phones
        socios_grp[str(cnpj)] = socios_out

    enr = base_grp.copy()
    out: Dict[str, Any] = {}
    keys = set(list(enr.index.astype(str)) + list(emails_grp.keys()) + list(phones_grp.keys()) + list(socios_grp.keys()))
    for cnpj in keys:
        row = enr.loc[cnpj] if cnpj in enr.index else {}
        porte = s(row.get("porte_empresa_txt") if hasattr(row, "get") else row.get("porte_empresa") if hasattr(row, "get") else "")
        out[cnpj] = {
            "porte": porte,
            "capital_social": s(row.get("capital_social") if hasattr(row, "get") else ""),
            "razao_social": s(row.get("razao_social") if hasattr(row, "get") else ""),
            "emails": emails_grp.get(cnpj, []),
            "phones": phones_grp.get(cnpj, []),
            "n_socios": len(socios_grp.get(cnpj, [])) if socios_grp.get(cnpj, []) else int(s(row.get("n_socios")) or 0),
            "socios": socios_grp.get(cnpj, []) or (
                list(pd.unique(df[df["cnpj"] == cnpj]["nome_socio_razao_social"].dropna()))[:max_socios_in_json]
                if "nome_socio_razao_social" in df.columns else []
            ),
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
    t = re.sub(r'\bKM\s*([0-9]+[,\.]?[0-9]*)', r'KM \1', t)
    t = re.sub(r'\s*-\s*\d{2}\s*/\s*[A-Z]{2}\s*-?', ' ', t)
    mcep = re.search(r'CEP\s*([0-9]{5}-?[0-9]{3})', t)
    cep: Optional[str] = None
    if mcep:
        cep = mcep.group(1)
        t = t[:mcep.start()] + t[mcep.end():]
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

def is_valid_hit(loc, bbox: Optional[Tuple[float, float, float, float]]) -> bool:
    raw = getattr(loc, "raw", {}) or {}
    try:
        lat = float(loc.latitude); lon = float(loc.longitude)
    except Exception:
        return False
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
    seen = set(); uniq: List[Any] = []
    for q in cands:
        key = json.dumps(q, ensure_ascii=False, sort_keys=True) if isinstance(q, dict) else str(q)
        if key not in seen:
            uniq.append(q); seen.add(key)
    return uniq

def geocode_with_candidates(nominate, arcgis, candidates: List[Any],
                            bbox: Optional[Tuple[float, float, float, float]],
                            cidade: str, uf: str) -> Tuple[str, str, str, str, Dict[str, Any]]:
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
            if succ_rows:
                pd.DataFrame(succ_rows).to_csv("geocode_success.csv", index=False)
            if fail_rows:
                pd.DataFrame(fail_rows).to_csv("geocode_failures.csv", index=False)
            print(f"[{i}/{len(pend)}] resolvidos: {ok} | falhas: {falhas} | cache/logs parciais…")

    if novos:
        geocache = pd.concat([geocache, pd.DataFrame(novos)], ignore_index=True)

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

        # Contatos vindos do enriquecido (preferência) + fallback da base/row
        enr_info = enr_idx.get(cnpj, {})
        emails_from_enr = list(enr_info.get("emails", []) or [])
        phones_from_enr = list(enr_info.get("phones", []) or [])

        fallback_contacts = collect_contacts_from_row(r)
        emails = list(dict.fromkeys(emails_from_enr + fallback_contacts.get("emails", [])))
        phones = list(dict.fromkeys(phones_from_enr + fallback_contacts.get("phones", [])))

        item: Dict[str, Any] = {
            "cnpj": cnpj,
            "cnpj_formatado": format_cnpj(cnpj),
            "nome": nome,
            "endereco": endereco,
            "latitude": float(lat) if lat else None,
            "longitude": float(lon) if lon else None,
            "query_geocode": s(r.get("query", "")),
            # NOVOS CAMPOS:
            "emails": emails,
            "phones": phones,
        }

        if enr_info:
            item.update({
                "razao_social": enr_info.get("razao_social", ""),
                "porte": enr_info.get("porte", ""),
                "capital_social": enr_info.get("capital_social", ""),
                "n_socios": int(enr_info.get("n_socios", 0) or 0),
                "socios": enr_info.get("socios", []),  # pode conter strings ou objetos {name, emails?, phones?}
            })

        data["features"].append(item)

    data["meta"]["count_output"] = len(data["features"])
    out_json.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"JSON salvo em: {out_json}  (itens: {data['meta']['count_output']})")

if __name__ == "__main__":
    main()