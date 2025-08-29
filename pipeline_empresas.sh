#!/usr/bin/env bash
set -euo pipefail

#####################################
# CONFIGURÁVEIS
#####################################
CITIES=("São Carlos")          # pode listar várias: ("São Carlos" "Araraquara")
UF="SP"

CNAES=("6201501" "6201502" "6202300" "6203100" "6204000" "6209100" \
       "6311900" "6319400" "6190601" "6190602" \
       "2621300" "2622100" "4651601" "4651602" "4751201" \
       "8599603" "9511800" "1830003" "7733100")

CHUNKSIZE=300000
USER_AGENT="empresas-mapper/1.0 (contact: seu-email@exemplo.com)"

OUT_STEP1="empresas_ativas_filtradas.csv"
OUT_STEP2="empresas_filtradas_por_cnae.csv"
OUT_STEP3="empresas_tecnologia_sc_ativas_enriquecidas.csv"
OUT_STEP4="empresas_tecnologia_sc_ativas_com_socios.csv"
JSON_OUT="empresas_tecnologia_sao_carlos.json"
GEOCACHE="geocache_enderecos.csv"

HTTP_PORT=8000

# Controle do JSON do mapa
KEEP_MISSING_JSON=0          # 1 = mantém itens sem lat/lon no JSON; 0 = descarta
MAX_GEOCODE=""               # ex.: "200" para limitar novos geocodes; "" = sem limite

# Ambiente Python
USE_VENV=1
VENV_DIR=".venv"

#####################################
# Utils
#####################################
log() { printf "\e[1;34m[INFO]\e[0m %s\n" "$*"; }
warn(){ printf "\e[1;33m[WARN]\e[0m %s\n" "$*"; }
err() { printf "\e[1;31m[ERRO]\e[0m %s\n" "$*" >&2; }

need_cmd() { command -v "$1" >/dev/null 2>&1 || return 1; }
join_by() { local IFS="$1"; shift; echo "$*"; }

#####################################
# 0) Dependências
#####################################
log "Checando dependências…"
sudo apt-get update -y

# binários usados pelo script
for pkg in curl unzip wget lsof python3 python3-pip; do
  if ! need_cmd "$pkg"; then
    sudo apt-get install -y "$pkg" || true
  fi
done

# Garante ensurepip/venv para a MESMA versão do python3
if ! python3 -c "import ensurepip" >/dev/null 2>&1; then
  pyver="$(python3 -V | awk '{print $2}' | cut -d. -f1,2)"   # ex.: 3.10, 3.12
  log "Instalando python3-venv e python${pyver}-venv…"
  sudo apt-get install -y python3-venv "python${pyver}-venv" || true
fi

if [[ "$USE_VENV" -eq 1 ]]; then
  # Se existir venv quebrado (sem pip), remove e recria
  if [[ -d "$VENV_DIR" ]] && [[ ! -x "$VENV_DIR/bin/pip" ]]; then
    warn "Venv existente sem pip — removendo $VENV_DIR…"
    rm -rf "$VENV_DIR"
  fi

  [[ -d "$VENV_DIR" ]] || python3 -m venv "$VENV_DIR"

  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    err "Falha ao criar venv em $VENV_DIR. Verifique se 'python3-venv' e 'pythonX.Y-venv' foram instalados."
    exit 1
  fi

  # shellcheck disable=SC1090
  source "$VENV_DIR/bin/activate"
  PY=python
  PIP=pip

  $PIP -q install --upgrade pip setuptools wheel
else
  PY=python3
  PIP=pip3
fi

log "Instalando libs Python…"
$PIP -q install --upgrade pandas geopy folium

#####################################
# 1) Baixar/extrair da Receita (mês mais recente)
#####################################
BASE_URL="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"

get_latest_month_url() {
  log "Descobrindo pasta do mês mais recente…"
  local months
  months="$(curl -fsSL "$BASE_URL/" | grep -Eo 'href="20[0-9]{2}-[01][0-9]/"' | sed -E 's/^href="|\/"$//g' | sort -u)"
  [[ -n "$months" ]] || { err "Não consegui listar meses em $BASE_URL"; exit 1; }
  echo "$BASE_URL/$(echo "$months" | sort | tail -n 1)/"
}
MONTH_URL="$(get_latest_month_url)"
log "Usando $MONTH_URL"

REQ_GROUPS=(
  "ESTABELE:K.*ESTABELE.*\\.zip"
  "EMPRECSV:K.*EMPRECSV.*\\.zip"
  "SOCIOCSV:K.*SOCIOCSV.*\\.zip"
  "MUNICCSV:.*MUNICCSV.*\\.zip"
  "CNAECSV:.*CNAECSV.*\\.zip"
  "QUALSCSV:.*QUALSCSV.*\\.zip"
  "PAISCSV:.*PAISCSV.*\\.zip"
)

has_extracted() {
  local key="$1" comp
  case "$key" in
    ESTABELE) comp='K*.ESTABELE*';;
    EMPRECSV) comp='K*.EMPRECSV*';;
    SOCIOCSV) comp='K*.SOCIOCSV*';;
    MUNICCSV) comp='*MUNICCSV*';;
    CNAECSV)  comp='*CNAECSV*';;
    QUALSCSV) comp='*QUALSCSV*';;
    PAISCSV)  comp='*PAISCSV*';;
  esac
  shopt -s nullglob
  for f in $comp; do
    [[ "$f" == *.zip ]] && continue
    [[ -s "$f" ]] && return 0
  done
  return 1
}

download_group() {
  local key="$1" regex="$2"
  if has_extracted "$key"; then
    log "$key já extraído — OK"
    return
  fi
  log "Baixando $key…"
  local links
  links="$(curl -fsSL "$MONTH_URL" | grep -Eoi 'href="([^"]+)"' | sed -E 's/^href="|"$//g' | grep -E "$regex" || true)"
  if [[ -z "$links" ]]; then
    if [[ "$key" == "PAISCSV" ]]; then
      warn "PAISCSV não encontrado; seguindo sem ele."
      return
    fi
    err "Não achei arquivos para $key (regex: $regex)"
    exit 1
  fi
  while IFS= read -r href; do
    url="$MONTH_URL$href"
    log "wget -c $url"
    wget -c -q "$url"
  done <<< "$links"
  shopt -s nullglob
  for z in *.zip; do
    log "Extraindo $z…"
    unzip -n -q "$z"
  done
}

for grp in "${REQ_GROUPS[@]}"; do
  KEY="${grp%%:*}"; RGX="${grp#*:}"
  download_group "$KEY" "$RGX"
done

#####################################
# 2) Pipeline de .py (retomável)
#####################################
if [[ -s "$OUT_STEP1" ]]; then
  log "P1 já existe ($OUT_STEP1) — pulando."
else
  log "P1: filtro_cidades_ativas.py → $OUT_STEP1"
  CITY_ARGS=()
  for c in "${CITIES[@]}"; do CITY_ARGS+=("-c" "$c"); done
  # NOTA: não passamos --engine porque pode não existir no seu .py
  $PY filtro_cidades_ativas.py "${CITY_ARGS[@]}" --uf "$UF" --out "$OUT_STEP1" --chunksize "$CHUNKSIZE"
fi

if [[ -s "$OUT_STEP2" ]]; then
  log "P2 já existe ($OUT_STEP2) — pulando."
else
  log "P2: filtra_por_cnae.py → $OUT_STEP2"
  CNAE_ARGS=()
  for k in "${CNAES[@]}"; do CNAE_ARGS+=("$k"); done
  $PY filtra_por_cnae.py --in "$OUT_STEP1" --cnae "${CNAE_ARGS[@]}" --out "$OUT_STEP2"
fi

if [[ -s "$OUT_STEP3" ]]; then
  log "P3 já existe ($OUT_STEP3) — pulando."
else
  log "P3: merge_com_empresas.py → $OUT_STEP3"
  $PY merge_com_empresas.py --in "$OUT_STEP2" --out "$OUT_STEP3" --chunksize "$CHUNKSIZE"
fi

if [[ -s "$OUT_STEP4" ]]; then
  log "P4 já existe ($OUT_STEP4) — pulando."
else
  log "P4: merge_socios.py → $OUT_STEP4"
  $PY merge_socios.py --in "$OUT_STEP3" --out "$OUT_STEP4" --chunksize "$CHUNKSIZE"
fi

# P5: Gera JSON com mapa.py (sem HTML)
if [[ -s "$JSON_OUT" ]]; then
  log "P5 já existe ($JSON_OUT) — pulando geocodificação."
else
  log "P5: mapa.py (gera JSON) → $JSON_OUT"
  KEEP_FLAG=()
  [[ "$KEEP_MISSING_JSON" -eq 1 ]] && KEEP_FLAG=(--keep-missing)

  MAX_FLAG=()
  [[ -n "${MAX_GEOCODE}" ]] && MAX_FLAG=(--max-geocode "$MAX_GEOCODE")

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
# 3) Abrir mapa.html
#####################################
if [[ ! -f "mapa.html" ]]; then
  err "mapa.html não encontrado. Coloque-o na pasta e rode novamente."
  exit 1
fi

log "Subindo http.server em http://localhost:${HTTP_PORT} …"
if lsof -i TCP:"$HTTP_PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then
  warn "Porta $HTTP_PORT ocupada; tentando encerrar processo…"
  kill "$(lsof -i TCP:"$HTTP_PORT" -sTCP:LISTEN -t)" || true
  sleep 1
fi
$PY -m http.server "$HTTP_PORT" >/dev/null 2>&1 &
SERV_PID=$!
sleep 1

URL="http://localhost:${HTTP_PORT}/mapa.html?data=$(printf '%s' "$JSON_OUT" | sed 's/ /%20/g')"
log "Abrindo navegador: $URL"
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$URL" >/dev/null 2>&1 || true
else
  warn "xdg-open não disponível. Abra manualmente: $URL"
  echo "$URL"
fi

log "Pipeline concluído."
trap '[[ -n "${SERV_PID:-}" ]] && kill ${SERV_PID} >/dev/null 2>&1 || true' EXIT
wait $SERV_PID || true
