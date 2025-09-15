#!/usr/bin/env bash
set -euo pipefail

#####################################
# CONFIGURÁVEIS (ajuste aqui)
#####################################

# 1) Parâmetros do filtro de cidades e UF
CITIES=("São Carlos")          # pode listar várias: ("São Carlos" "Araraquara")
UF="SP"

# 2) Lista de CNAEs (7 dígitos) para o passo 2
CNAES=("6201501" "6201502" "6202300" "6203100" "6204000" "6209100" \
       "6311900" "6319400" "6190601" "6190602" \
       "2621300" "2622100" "4651601" "4651602" "4751201" \
       "8599603" "9511800" "1830003" "7733100")

# 3) Tamanho de chunk para leitura (ajuste à sua RAM)
CHUNKSIZE=300000

# 4) User-Agent para Nominatim (coloque um contato seu real)
USER_AGENT="empresas-mapper/1.0 (contact: marcius@shinier.com.br)"

# 5) Saídas (padrão segue os nomes esperados pelos .py)
OUT_STEP1="empresas_ativas_filtradas.csv"
OUT_STEP2="empresas_filtradas_por_cnae.csv"
OUT_STEP3="empresas_tecnologia_sc_ativas_enriquecidas.csv"
OUT_STEP4="empresas_tecnologia_sc_ativas_com_socios.csv"
JSON_OUT="empresas_tecnologia_sao_carlos.json"
GEOCACHE="geocache_enderecos.csv"

# 6) Servidor local para abrir o mapa
HTTP_PORT=8000

# 7) (opcional) use venv local para isolar dependências Python
USE_VENV=1
VENV_DIR=".venv"

# 8) flags de controle da geocodificação:
KEEP_MISSING_JSON=0      # 1 = mantém itens sem lat/lon no JSON
MAX_GEOCODE=""           # ex.: 500 para testes; vazio = sem limite

#####################################
# Funções utilitárias
#####################################
log() { printf "\e[1;34m[INFO]\e[0m %s\n" "$*"; }
warn(){ printf "\e[1;33m[WARN]\e[0m %s\n" "$*"; }
err() { printf "\e[1;31m[ERRO]\e[0m %s\n" "$*" >&2; }

need_cmd() { command -v "$1" >/dev/null 2>&1 || { err "Comando '$1' não encontrado."; return 1; }; }

# AAAA-MM -> AAAAMM
yyyymm_from_month_url() {
  local murl="$1"
  local seg="${murl%/}"; seg="${seg##*/}"     # 2025-09
  echo "${seg/-/}"                            # 202509
}

# já tem arquivos extraídos/normalizados para um grupo?
has_group() {
  local key="$1"
  shopt -s nullglob
  case "$key" in
    ESTABELE) comp=(K*.ESTABELE* Estabelecimentos*.csv) ;;
    EMPRECSV) comp=(K*.EMPRECSV* Empresas*.csv) ;;
    SOCIOCSV) comp=(K*.SOCIOCSV* Socios*.csv) ;;
    MUNICCSV) comp=(*MUNIC* Municipios*.csv) ;;
    CNAECSV)  comp=(*CNAECSV* Cnaes*.csv) ;;
    QUALSCSV) comp=(*QUALSCSV* Qualificacoes*.csv) ;;
    PAISCSV)  comp=(*PAISCSV* Paises*.csv) ;;
    *) return 1 ;;
  esac
  for f in "${comp[@]}"; do [[ -s "$f" ]] && return 0; done
  return 1
}

#####################################
# 0) Checagens e dependências (só instala se faltar)
#####################################
log "Checando dependências de sistema…"
for pkg in curl unzip wget lsof; do
  if ! need_cmd "$pkg"; then
    sudo apt-get update && sudo apt-get install -y "$pkg"
  fi
done

if ! need_cmd python3; then sudo apt-get update && sudo apt-get install -y python3; fi
if ! need_cmd pip3;    then sudo apt-get update && sudo apt-get install -y python3-pip; fi

if [[ "$USE_VENV" -eq 1 ]]; then
  if [[ ! -d "$VENV_DIR" ]]; then
    log "Criando venv em $VENV_DIR…"
    if ! python3 -m venv "$VENV_DIR" 2>/dev/null; then
      warn "python3-venv ausente — instalando…"
      sudo apt-get update
      sudo apt-get install -y python3-venv || true
      PYVER="$(python3 -V | awk '{print $2}' | cut -d. -f1,2)"
      sudo apt-get install -y "python${PYVER}-venv" || true
      python3 -m venv "$VENV_DIR"
    fi
  fi
  # shellcheck disable=SC1090
  source "$VENV_DIR/bin/activate"
  PY=python
  PIP=pip
else
  PY=python3
  PIP=pip3
fi

# Só instala libs Python se faltar algo
if ! $PY - <<'PYCHK' >/dev/null 2>&1
import importlib
mods = ["pandas","geopy","folium"]
missing = [m for m in mods if importlib.util.find_spec(m) is None]
if missing: raise SystemExit(1)
PYCHK
then
  log "Instalando dependências Python (pandas, geopy, folium)…"
  $PIP -q install --upgrade pip
  $PIP -q install pandas geopy folium
else
  log "Dependências Python já presentes — OK"
fi

#####################################
# 1) Verificar/baixar da Receita (SÓ se faltar algo)
#####################################
BASE_URL="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"

# imprime só a URL do mês mais recente (AAAA-MM/)
get_latest_month_url() {
  curl -fsSL "$BASE_URL/" \
    | grep -Eo 'href="20[0-9]{2}-[01][0-9]/"' \
    | sed -E 's/^href="|\/"$//g' \
    | sort -u \
    | sort \
    | tail -n 1 \
    | awk -v base="$BASE_URL" '{print base "/" $0 "/"}'
}

if has_group ESTABELE && has_group EMPRECSV && has_group SOCIOCSV \
   && has_group MUNICCSV && has_group CNAECSV && has_group QUALSCSV; then
  log "Todos os datasets necessários já estão presentes — pulando downloads."
  MONTH_URL="$(get_latest_month_url)" || MONTH_URL="$BASE_URL/"
else
  MONTH_URL="$(get_latest_month_url)"
  log "Usando pasta do mês: $MONTH_URL"
  YYYYMM="$(yyyymm_from_month_url "$MONTH_URL")"

  # Padrões (novos e legados)
  REQ_GROUPS=(
    "ESTABELE:(Estabelecimentos[0-9]+\\.zip|K.*ESTABELE.*\\.zip)"
    "EMPRECSV:(Empresas[0-9]+\\.zip|K.*EMPRECSV.*\\.zip)"
    "SOCIOCSV:(Socios[0-9]+\\.zip|K.*SOCIOCSV.*\\.zip)"
    "MUNICCSV:(Municipios\\.zip|.*MUNICCSV.*\\.zip)"
    "CNAECSV:(Cnaes\\.zip|.*CNAECSV.*\\.zip)"
    "QUALSCSV:(Qualificacoes\\.zip|.*QUALSCSV.*\\.zip)"
    "PAISCSV:(Paises\\.zip|.*PAISCSV.*\\.zip)"
  )

  list_links() { curl -fsSL "$MONTH_URL" | grep -Eio 'href="[^"]+"' | sed -E 's/^href="|"$//g'; }

  download_group() {
    local key="$1" regex="$2"
    if has_group "$key"; then
      log "$key: já presente — pulando download."
      return 0
    fi
    log "Baixando $key…"
    local links
    links="$(list_links | grep -E -i "$regex" || true)"
    if [[ -z "$links" ]]; then
      if [[ "$key" == "PAISCSV" ]]; then
        warn "PAISCSV não encontrado — seguindo sem ele."
        return 0
      fi
      err "$key: nenhum arquivo encontrado em $MONTH_URL (regex: $regex)"
      exit 1
    fi
    while IFS= read -r href; do
      [[ -z "$href" ]] && continue
      local url="$MONTH_URL$href"
      log "  wget -c $url"
      wget -q -c "$url"
    done <<< "$links"
  }

  # baixa o que faltar
  for grp in "${REQ_GROUPS[@]}"; do
    KEY="${grp%%:*}"
    RGX="${grp#*:}"
    download_group "$KEY" "$RGX"
  done

  # ---------- extração + normalização ----------
  # mapeia categoria pelo nome do ZIP
  category_from_zip() {
    local zbl="${1,,}"
    if [[ "$zbl" == *"estabelecimentos"* ]]; then echo "ESTABELE"; return; fi
    if [[ "$zbl" == *"empresas"* ]];          then echo "EMPRECSV"; return; fi
    if [[ "$zbl" == *"socios"* ]];            then echo "SOCIOCSV"; return; fi
    if [[ "$zbl" == *"municipios"* ]];        then echo "MUNICCSV"; return; fi
    if [[ "$zbl" == *"cnaes"* ]];             then echo "CNAECSV";  return; fi
    if [[ "$zbl" == *"qualificacoes"* ]];     then echo "QUALSCSV"; return; fi
    if [[ "$zbl" == *"paises"* ]];            then echo "PAISCSV";  return; fi
    echo ""
  }

  # gera nome canônico de destino (sempre termina com .csv)
  canon_dest_name() {
    local category="$1" idx="$2" yyyymm="$3"
    case "$category" in
      EMPRECSV) echo "K${yyyymm}.EMPRECSV${idx}.csv" ;;
      ESTABELE) echo "K${yyyymm}.ESTABELE${idx}.csv" ;;
      SOCIOCSV) echo "K${yyyymm}.SOCIOCSV${idx}.csv" ;;
      MUNICCSV) echo "F.K03200\$Z.D${yyyymm}.MUNICCSV.csv" ;;
      CNAECSV)  echo "F.K03200\$Z.D${yyyymm}.CNAECSV.csv" ;;
      QUALSCSV) echo "F.K03200\$Z.D${yyyymm}.QUALSCSV.csv" ;;
      PAISCSV)  echo "F.K03200\$Z.D${yyyymm}.PAISCSV.csv" ;;
      *)        echo "" ;;
    esac
  }

  # tenta extrair índice a partir do nome do ZIP (Ex.: Empresas7.zip -> 7)
  idx_from_zip() {
    local base="${1##*/}"; base="${base%.*}"
    if [[ "$base" =~ ([0-9]+)$ ]]; then echo "${BASH_REMATCH[1]}"; else echo ""; fi
  }

  extract_and_normalize_one() {
    local zip="$1" ; local yyyymm="$2"
    local cat="$(category_from_zip "$zip")"
    [[ -z "$cat" ]] && { warn "Categoria desconhecida para $(basename "$zip") — pulando."; return; }
    local idx="$(idx_from_zip "$zip")"

    local tmp; tmp="$(mktemp -d)"
    # protege contra falha de unzip mesmo com 'set -e'
    if ! unzip -q -o "$zip" -d "$tmp"; then
      warn "Falha ao extrair $(basename "$zip"); mantendo o ZIP para diagnóstico."
      rm -rf "$tmp"
      return
    fi

    shopt -s nullglob
    local moved=0
    # pega QUALQUER arquivo extraído (com ou sem extensão)
    for f in "$tmp"/*; do
      [[ -f "$f" ]] || continue
      local dest
      case "$cat" in
        MUNICCSV|CNAECSV|QUALSCSV|PAISCSV)
          dest="$(canon_dest_name "$cat" "" "$yyyymm")"
          ;;
        *)
          dest="$(canon_dest_name "$cat" "$idx" "$yyyymm")"
          ;;
      esac
      if [[ -n "$dest" ]]; then
        [[ "$dest" != *.csv ]] && dest="${dest}.csv"
        if [[ ! -s "$dest" ]]; then
          mv -f "$f" "$dest"
          moved=1
        fi
      fi
    done

    rm -rf "$tmp"

    # apaga o ZIP após extração (independente de ter movido algo,
    # pois pode já existir com esse mês/índice)
    if rm -f "$zip"; then
      log "Apagado ZIP: $(basename "$zip")"
    else
      warn "Não foi possível apagar: $(basename "$zip")"
    fi

    if [[ "$moved" -eq 0 ]]; then
      warn "Nenhum arquivo novo movido de $(basename "$zip") (provavelmente já existiam)."
    fi
  }


  extract_all() {
    local yyyymm="$1"
    shopt -s nullglob
    for z in *.zip; do
      extract_and_normalize_one "$z" "$yyyymm"
    done
  }

  extract_all "$YYYYMM"

  # sanity check pós-extração
  for g in ESTABELE EMPRECSV SOCIOCSV MUNICCSV CNAECSV QUALSCSV; do
    if ! has_group "$g"; then
      err "Após extração, grupo $g ainda não encontrado — verifique os zips baixados."
      exit 1
    fi
  done
fi

#####################################
# 2) Executar pipeline de .py (com retomada)
#####################################

# Passo 1
if [[ -s "$OUT_STEP1" ]]; then
  log "P1 já existe ($OUT_STEP1) — pulando."
else
  log "P1: filtro_cidades_ativas.py → $OUT_STEP1"
  CITY_ARGS=()
  for c in "${CITIES[@]}"; do CITY_ARGS+=("-c" "$c"); done
  $PY filtro_cidades_ativas.py "${CITY_ARGS[@]}" --uf "$UF" --out "$OUT_STEP1" --chunksize "$CHUNKSIZE"
fi

# Passo 2
if [[ -s "$OUT_STEP2" ]]; then
  log "P2 já existe ($OUT_STEP2) — pulando."
else
  log "P2: filtra_por_cnae.py → $OUT_STEP2"
  CNAE_ARGS=()
  for k in "${CNAES[@]}"; do CNAE_ARGS+=("$k"); done
  $PY filtra_por_cnae.py --in "$OUT_STEP1" --cnae "${CNAE_ARGS[@]}" --out "$OUT_STEP2"
fi

# Passo 3
if [[ -s "$OUT_STEP3" ]]; then
  log "P3 já existe ($OUT_STEP3) — pulando."
else
  log "P3: merge_com_empresas.py → $OUT_STEP3"
  $PY merge_com_empresas.py --in "$OUT_STEP2" --out "$OUT_STEP3" --chunksize "$CHUNKSIZE"
fi

# Passo 4
if [[ -s "$OUT_STEP4" ]]; then
  log "P4 já existe ($OUT_STEP4) — pulando."
else
  log "P4: merge_socios.py → $OUT_STEP4"
  $PY merge_socios.py --in "$OUT_STEP3" --out "$OUT_STEP4" --chunksize "$CHUNKSIZE"
fi

# Passo 5 (gera JSON)
if [[ -s "$JSON_OUT" ]]; then
  log "P5 já existe ($JSON_OUT) — pulando geocodificação."
else
  log "P5: mapa.py (gera JSON) → $JSON_OUT"
  KEEP_FLAG=(); [[ "$KEEP_MISSING_JSON" -eq 1 ]] && KEEP_FLAG+=(--keep-missing)
  MAX_FLAG=();  [[ -n "${MAX_GEOCODE:-}" ]] && MAX_FLAG+=(--max-geocode "$MAX_GEOCODE")

  $PY mapa.py \
    --base "$OUT_STEP2" \
    --enriched "$OUT_STEP4" \
    --geocache "$GEOCACHE" \
    --out-json "$JSON_OUT" \
    --city "${CITIES[0]}" \
    --uf "$UF" \
    --user-agent "$USER_AGENT" \
    "${KEEP_FLAG[@]}" \
    "${MAX_FLAG[@]}"
fi

#####################################
# 3) Abrir mapa.html apontando para o JSON
#####################################
if [[ ! -f "mapa.html" ]]; then
  err "mapa.html não encontrado na pasta. Coloque o arquivo e rode novamente."
  exit 1
fi

log "Subindo servidor local em http://localhost:${HTTP_PORT} (CTRL+C para parar)…"
# mata servidor anterior na mesma porta (se houver)
if lsof -i TCP:"$HTTP_PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then
  warn "Porta $HTTP_PORT já ocupada; tentando encerrar processo existente…"
  kill "$(lsof -i TCP:"$HTTP_PORT" -sTCP:LISTEN -t)" || true
  sleep 1
fi

# inicia servidor em background
$PY -m http.server "$HTTP_PORT" >/dev/null 2>&1 &
SERV_PID=$!
sleep 1

URL="http://localhost:${HTTP_PORT}/mapa.html?data=$(printf '%s' "$JSON_OUT" | sed 's/ /%20/g')"
log "Abrindo navegador em: $URL"
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$URL" >/dev/null 2>&1 || true
else
  warn "xdg-open não disponível. Abra manualmente: $URL"
  echo "$URL"
fi

log "Pipeline concluído com sucesso."
wait $SERV_PID || true