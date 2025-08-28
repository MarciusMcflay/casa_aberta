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

#####################################
# Funções utilitárias
#####################################
log() { printf "\e[1;34m[INFO]\e[0m %s\n" "$*"; }
warn(){ printf "\e[1;33m[WARN]\e[0m %s\n" "$*"; }
err() { printf "\e[1;31m[ERRO]\e[0m %s\n" "$*" >&2; }

need_cmd() { command -v "$1" >/dev/null 2>&1 || { err "Comando '$1' não encontrado."; return 1; }; }

# Normaliza lista em string para linha de comando
join_by() { local IFS="$1"; shift; echo "$*"; }

#####################################
# 0) Checagens e dependências
#####################################
log "Checando dependências de sistema…"
if ! need_cmd curl;  then sudo apt-get update && sudo apt-get install -y curl;  fi
if ! need_cmd unzip; then sudo apt-get update && sudo apt-get install -y unzip; fi
if ! need_cmd python3; then sudo apt-get update && sudo apt-get install -y python3; fi
if ! need_cmd pip3;    then sudo apt-get update && sudo apt-get install -y python3-pip; fi

if [[ "$USE_VENV" -eq 1 ]]; then
  if [[ ! -d "$VENV_DIR" ]]; then
    log "Criando venv em $VENV_DIR…"
    python3 -m venv "$VENV_DIR"
  fi
  # shellcheck disable=SC1090
  source "$VENV_DIR/bin/activate"
  PY=python
  PIP=pip
else
  PY=python3
  PIP=pip3
fi

log "Instalando dependências Python (pandas, geopy, folium)…"
$PIP -q install --upgrade pip
$PIP -q install pandas geopy folium

#####################################
# 1) Verificar/baixar arquivos da Receita
#####################################
BASE_URL="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"

# Função: escolhe URL do mês mais recente
get_latest_month_url() {
  log "Descobrindo pasta do mês mais recente em $BASE_URL…"
  # pega todos diretórios AAAA-MM/
  local months
  months="$(curl -fsSL "$BASE_URL/" | grep -Eo 'href="20[0-9]{2}-[01][0-9]/"' | sed -E 's/^href="|\/"$//g' | sort -u)"
  if [[ -z "$months" ]]; then
    err "Não foi possível listar os meses em $BASE_URL/"
    exit 1
  fi
  # escolhe o mais recente (último após sort)
  local latest
  latest="$(echo "$months" | sort | tail -n 1)"
  echo "$BASE_URL/$latest/"
}

MONTH_URL="$(get_latest_month_url)"
log "Usando pasta do mês: $MONTH_URL"

# Precisamos destes padrões
REQ_GROUPS=(
  "ESTABELE:K.*ESTABELE.*\\.zip"
  "EMPRECSV:K.*EMPRECSV.*\\.zip"
  "SOCIOCSV:K.*SOCIOCSV.*\\.zip"
  "MUNICCSV:.*MUNICCSV.*\\.zip"
  "CNAECSV:.*CNAECSV.*\\.zip"
  "QUALSCSV:.*QUALSCSV.*\\.zip"
  "PAISCSV:.*PAISCSV.*\\.zip"     # opcional mas tentamos baixar
)

# Verifica se já existe algum arquivo extraído (não .zip) para um grupo
has_extracted() {
  local key="$1"
  case "$key" in
    ESTABELE) comp='K*.ESTABELE*';;
    EMPRECSV) comp='K*.EMPRECSV*';;
    SOCIOCSV) comp='K*.SOCIOCSV*';;
    MUNICCSV) comp='*MUNICCSV*';;
    CNAECSV)  comp='*CNAECSV*';;
    QUALSCSV) comp='*QUALSCSV*';;
    PAISCSV)  comp='*PAISCSV*';;
  esac
  # existe arquivo correspondente NÃO .zip?
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
    log "Arquivos $key já estão extraídos — OK"
    return 0
  fi

  log "Baixando $key do mês atual…"
  # lista links que casam com o regex
  local links
  links="$(curl -fsSL "$MONTH_URL" | grep -Eoi "href=\"([^\"]+)\"" | sed -E 's/^href="|"$//g' | grep -E "$regex" || true)"
  if [[ -z "$links" ]]; then
    if [[ "$key" == "PAISCSV" ]]; then
      warn "PAISCSV não encontrado no mês; seguiremos sem ele."
      return 0
    fi
    err "Nenhum arquivo encontrado para padrão $regex em $MONTH_URL"
    exit 1
  fi

  # baixa cada .zip (retomando se interrompido)
  while IFS= read -r href; do
    url="$MONTH_URL$href"
    log "wget -c '$url'"
    wget -c -q "$url"
  done <<< "$links"

  # extrai cada zip (sem sobrescrever se já existir)
  shopt -s nullglob
  for z in *.zip; do
    log "Extraindo $z…"
    unzip -n -q "$z"
  done
}

for grp in "${REQ_GROUPS[@]}"; do
  KEY="${grp%%:*}"
  RGX="${grp#*:}"
  download_group "$KEY" "$RGX"
done

#####################################
# 2) Executar pipeline de .py (com retomada)
#####################################

# Passo 1: filtro por cidade(s) ativas + inclui CNAEs no CSV (colunas: nome,cnpj,endereco,cnae_*,municipio,uf)
if [[ -s "$OUT_STEP1" ]]; then
  log "P1 já existe ($OUT_STEP1) — pulando."
else
  log "P1: filtro_cidades_ativas.py → $OUT_STEP1"
  CITY_ARGS=()
  for c in "${CITIES[@]}"; do CITY_ARGS+=("-c" "$c"); done
  $PY filtro_cidades_ativas.py "${CITY_ARGS[@]}" --uf "$UF" --out "$OUT_STEP1" --chunksize "$CHUNKSIZE"
fi

# Passo 2: filtra pelos CNAEs desejados
if [[ -s "$OUT_STEP2" ]]; then
  log "P2 já existe ($OUT_STEP2) — pulando."pipeline_empresas
else
  log "P2: filtra_por_cnae.py → $OUT_STEP2"
  CNAE_ARGS=()
  for k in "${CNAES[@]}"; do CNAE_ARGS+=("$k"); done
  $PY filtra_por_cnae.py --in "$OUT_STEP1" --cnae "${CNAE_ARGS[@]}" --out "$OUT_STEP2"
fi

# Passo 3: merge com EMPRECSV + QUALSCSV (razão social, porte decod., qualificação)
if [[ -s "$OUT_STEP3" ]]; then
  log "P3 já existe ($OUT_STEP3) — pulando."
else
  log "P3: merge_com_empresas.py → $OUT_STEP3"
  $PY merge_com_empresas.py --in "$OUT_STEP2" --out "$OUT_STEP3" --chunksize "$CHUNKSIZE"
fi

# Passo 4: merge com SOCIOCSV (+ QUALSCSV e PAISCSV) — gera *_com_socios.csv
if [[ -s "$OUT_STEP4" ]]; then
  log "P4 já existe ($OUT_STEP4) — pulando."
else
  log "P4: merge_socios.py → $OUT_STEP4"
  $PY merge_socios.py --in "$OUT_STEP3" --out "$OUT_STEP4" --chunksize "$CHUNKSIZE"
fi

# Passo 5: geocodifica e gera JSON (usando mapa.py = gerador de JSON)
if [[ -s "$JSON_OUT" ]]; then
  log "P5 já existe ($JSON_OUT) — pulando geocodificação."
else
  log "P5: mapa.py (gera JSON) → $JSON_OUT"
  $PY mapa.py \
    --base "$OUT_STEP2" \
    --enriched "$OUT_STEP4" \
    --geocache "$GEOCACHE" \
    --out-json "$JSON_OUT" \
    --city "${CITIES[0]}" \
    --uf "$UF" \
    --user-agent "$USER_AGENT"
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
fi

log "Pipeline concluído com sucesso."
wait $SERV_PID || true
