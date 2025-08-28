# filtro_cidades_ativas.py
# Uso:
#   python filtro_cidades_ativas.py -c "São Carlos" --uf "SP" --out empresas_ativas_sc.csv --chunksize 300000
#   python filtro_cidades_ativas.py -c "São Carlos" -c "Araraquara" --uf "SP" --out empresas_ativas_cidades.csv --chunksize 300000
#
# Saída: empresas_ativas_filtradas.csv  com colunas:
#   nome, cnpj, endereco, cnae_fiscal_principal, cnaes_secundarios, municipio, uf

import argparse
import unicodedata
import re
from pathlib import Path
import pandas as pd

# ---------- util ----------
def norm_txt(x: str) -> str:
    """Normaliza para busca: remove acentos, colapsa espaços, UPPER."""
    s = (x or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s).upper().strip()
    return s

def s(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)

def cnpj_full(b, o, d) -> str:
    return f"{s(b).zfill(8)}{s(o).zfill(4)}{s(d).zfill(2)}"

def monta_endereco(row) -> str:
    tl  = s(row.get("tipo_logradouro")).strip()
    log = s(row.get("logradouro")).strip()
    num = s(row.get("numero")).strip()
    cpl = s(row.get("complemento")).strip()
    bai = s(row.get("bairro")).strip()
    # prioridade para os valores digitados na chamada:
    mun = s(row.get("municipio_out")).strip() or s(row.get("municipio")).strip()
    uf  = s(row.get("uf_out")).strip() or s(row.get("uf")).strip()
    cep = re.sub(r"\D", "", s(row.get("cep")))
    ped = []
    if tl:  ped.append(f"{tl} ")
    if log: ped.append(log)
    if num: ped.append(f", {num}")
    if cpl: ped.append(f" - {cpl}")
    if bai: ped.append(f" - {bai}")
    if mun: ped.append(f" - {mun}")
    if uf:  ped.append(f"/{uf}")
    if cep:
        ped.append(f" - CEP {cep[:5]}-{cep[5:]}" if len(cep) == 8 else f" - CEP {cep}")
    return "".join(ped).strip(" -,")

def detect_cols_est(path: Path):
    n = pd.read_csv(
        path, sep=";", encoding="latin1", header=None, nrows=1,
        keep_default_na=False, na_filter=False
    ).shape[1]
    if n == 30:
        cols = [
            "cnpj_basico","cnpj_ordem","cnpj_dv","identificador_matriz_filial",
            "nome_fantasia","situacao_cadastral","data_situacao_cadastral",
            "motivo_situacao_cadastral","nome_cidade_exterior","pais",
            "data_inicio_atividade","cnae_fiscal_principal","cnaes_secundarios",
            "tipo_logradouro","logradouro","numero","complemento","bairro",
            "cep","uf","codigo_municipio","municipio",
            "ddd_1","telefone_1","ddd_2","telefone_2",
            "fax","email","situacao_especial","data_situacao_especial"
        ]
    elif n == 31:
        cols = [
            "cnpj_basico","cnpj_ordem","cnpj_dv","identificador_matriz_filial",
            "nome_fantasia","situacao_cadastral","data_situacao_cadastral",
            "motivo_situacao_cadastral","nome_cidade_exterior","pais",
            "data_inicio_atividade","cnae_fiscal_principal","cnaes_secundarios",
            "tipo_logradouro","logradouro","numero","complemento","bairro",
            "cep","uf","codigo_municipio","municipio",
            "ddd_1","telefone_1","ddd_2","telefone_2",
            "ddd_fax","fax","email","situacao_especial","data_situacao_especial"
        ]
    else:
        raise ValueError(f"Layout ESTABELECIMENTOS inesperado: {n} colunas (esperado 30/31).")
    return cols

def load_municipios():
    cand = sorted(Path(".").glob("*MUNIC*"))
    if not cand:
        raise FileNotFoundError("Arquivo *MUNIC* não encontrado na pasta.")
    ncols = pd.read_csv(
        cand[0], sep=";", encoding="latin1", header=None, nrows=1,
        keep_default_na=False, na_filter=False
    ).shape[1]
    if ncols == 2:
        mun = pd.read_csv(
            cand[0], sep=";", encoding="latin1", header=None,
            names=["cod_munic_rf","nome_municipio"],
            dtype=str, keep_default_na=False, na_filter=False
        )
        mun["uf"] = ""
    else:
        mun = pd.read_csv(
            cand[0], sep=";", encoding="latin1", header=None,
            usecols=[0,1,2], names=["cod_munic_rf","nome_municipio","uf"],
            dtype=str, keep_default_na=False, na_filter=False
        )
    mun["nome_norm"] = mun["nome_municipio"].map(norm_txt)
    mun["uf_norm"]   = mun["uf"].map(norm_txt)
    return mun

# ---------- leitura resiliente por chunks ----------
def iter_chunks_csv(path, engine_mode: str, **kwargs):
    """
    Gera chunks de um CSV com fallback:
    - engine_mode='c'      -> sempre C-engine
    - engine_mode='python' -> sempre Python engine
    - engine_mode='auto'   -> tenta C; se falhar, cai para Python
    """
    if engine_mode not in {"c", "python", "auto"}:
        engine_mode = "auto"

    def _yield(engine_name):
        return pd.read_csv(path, engine=engine_name, **kwargs)

    if engine_mode == "c":
        yield from _yield("c")
    elif engine_mode == "python":
        yield from _yield("python")
    else:
        try:
            yield from _yield("c")
        except Exception as e:
            print(f"Aviso: C-engine falhou em {Path(path).name}: {e}\n→ Tentando engine='python'…")
            yield from _yield("python")

def main():
    ap = argparse.ArgumentParser(description="Filtra ESTABELE por cidade(s) e status ATIVA, já incluindo CNAEs.")
    ap.add_argument("-c", "--city", action="append", required=True,
                    help="Nome da cidade (repita a opção para múltiplas). Ex.: -c 'São Carlos' -c 'Araraquara'")
    ap.add_argument("--uf", required=True, help="UF a filtrar (ex.: SP)")
    ap.add_argument("--out", default="empresas_ativas_filtradas.csv", help="Arquivo de saída (CSV)")
    ap.add_argument("--chunksize", type=int, default=300_000, help="Tamanho do chunk (linhas)")
    ap.add_argument("--engine", choices=["auto","c","python"], default="auto",
                    help="Engine do pandas: 'auto' (tenta C e cai p/ Python), 'c' ou 'python'.")
    args = ap.parse_args()

    # preserva acentos/ç exatamente como digitado (para montar endereço)
    cidades_user = args.city[:]
    uf_user = args.uf

    # versões normalizadas só para matching
    cidades_norm = [norm_txt(c) for c in cidades_user]
    uf_norm = norm_txt(args.uf)

    mun = load_municipios()
    if "uf_norm" in mun.columns and mun["uf_norm"].str.strip().any():
        alvo = mun[(mun["nome_norm"].isin(cidades_norm)) & (mun["uf_norm"] == uf_norm)]
    else:
        alvo = mun[(mun["nome_norm"].isin(cidades_norm))]

    if alvo.empty:
        raise SystemExit(f"Nenhum município encontrado em *MUNIC* para: {cidades_user} / UF={args.uf}")

    # mapeia codigo_municipio -> rótulo exatamente como digitado
    nome_norm_to_display = {norm_txt(orig): orig for orig in cidades_user}
    code_to_city_display = {
        row["cod_munic_rf"]: nome_norm_to_display.get(row["nome_norm"], row["nome_municipio"])
        for _, row in alvo.iterrows()
    }

    cods_rf = set(alvo["cod_munic_rf"].tolist())
    print(f"Cidades alvo: {sorted(set(code_to_city_display.values()))} | UF filtro: {uf_user}")
    print(f"Códigos Receita (municipio): {sorted(cods_rf)}")

    est_files = sorted(Path(".").glob("K*.ESTABELE*"))
    if not est_files:
        raise FileNotFoundError("Nenhum K*.ESTABELE* encontrado.")

    COLS_EST = detect_cols_est(est_files[0])
    USECOLS = [
        "cnpj_basico","cnpj_ordem","cnpj_dv","nome_fantasia",
        "situacao_cadastral",
        "cnae_fiscal_principal","cnaes_secundarios",
        "tipo_logradouro","logradouro","numero","complemento","bairro","cep",
        "uf","codigo_municipio","municipio"
    ]

    out_path = Path(args.out)
    out_path.unlink(missing_ok=True)
    header_written = False
    seen_cnpjs = set()
    total = 0
    skipped_chunks = 0

    for est in est_files:
        print(f"Processando: {est.name}")
        try:
            for chunk in iter_chunks_csv(
                est,
                engine_mode=args.engine,
                sep=";",
                encoding="latin1",
                encoding_errors="ignore",   # tolera bytes inválidos
                header=None,
                names=COLS_EST,
                usecols=USECOLS,
                dtype=str,
                low_memory=False,
                chunksize=args.chunksize,
                keep_default_na=False,
                na_filter=False,
                on_bad_lines="skip",
                lineterminator="\n"        # ajuda em CRLF misto/linhas longas
            ):
                try:
                    # UF + cidades (via codigo_municipio) + apenas ATIVAS
                    m = (
                        (chunk["uf"] == uf_user) &
                        (chunk["codigo_municipio"].isin(cods_rf)) &
                        (chunk["situacao_cadastral"].isin({"02", "2"}))
                    )
                    if not m.any():
                        continue
                    sel = chunk.loc[m].copy()

                    # normaliza campos para compor endereço
                    for c in ["tipo_logradouro","logradouro","numero","complemento","bairro","cep","municipio","uf",
                              "cnae_fiscal_principal","cnaes_secundarios"]:
                        sel[c] = sel[c].map(s)

                    # injeta a cidade/UF exatamente como digitadas
                    sel["municipio_out"] = sel["codigo_municipio"].map(lambda k: code_to_city_display.get(k, ""))
                    sel["uf_out"] = uf_user

                    sel["cnpj"] = sel.apply(lambda r: cnpj_full(r["cnpj_basico"], r["cnpj_ordem"], r["cnpj_dv"]), axis=1)
                    sel["endereco"] = sel.apply(monta_endereco, axis=1)
                    sel["nome"] = sel["nome_fantasia"].map(s)

                    out = sel[[
                        "nome","cnpj","endereco",
                        "cnae_fiscal_principal","cnaes_secundarios",
                        "municipio_out","uf_out"
                    ]].rename(columns={"municipio_out":"municipio","uf_out":"uf"})

                    # dedupe inter-arquivos
                    out = out[~out["cnpj"].isin(seen_cnpjs)]
                    if out.empty:
                        continue
                    seen_cnpjs.update(out["cnpj"].tolist())

                    out.sort_values(["nome","cnpj"]).to_csv(
                        out_path, index=False, mode="a",
                        header=(not header_written), encoding="utf-8"
                    )
                    header_written = True
                    total += len(out)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    skipped_chunks += 1
                    print(f"Aviso: falha ao processar chunk de {est.name}: {e} — chunk ignorado.")
                    continue
        except KeyboardInterrupt:
            print("Interrompido pelo usuário (Ctrl+C).")
            raise
        except Exception as e:
            print(f"Falha em {est.name}: {e}")

    print(f"\nGerado: {out_path}  (linhas: {total})")
    if skipped_chunks:
        print(f"Aviso: {skipped_chunks} chunk(s) foram ignorados por erro de parsing/processamento.")

if __name__ == "__main__":
    main()