#!/usr/bin/env bash
set -Eeuo pipefail
# shellcheck disable=SC2016

#####################################
# DIAGNÓSTICO EM CASO DE ERRO
#####################################
trap 'status=$?; cmd=${BASH_COMMAND}; line=${BASH_LINENO[0]}; printf "\e[1;31m[ERRO]\e[0m Falha (status=%s) na linha %s: %s\n" "$status" "$line" "$cmd" >&2' ERR

#####################################
# CONFIGURÁVEIS
#####################################
CITIES=("São Carlos")
UF="SP"

CNAES=("6201501" "6201502" "6202300" "6203100" "6204000" "6209100" \
       "6311900" "6319400" "6190601" "6190602" \
       "2621300" "2622100" "4651601" "4651602" "4751201" \
       "8599603" "9511800" "1830003" "7733100")

CHUNKSIZE=300000
USER_AGENT="empresas-mapper/1.0 (contact: marcius@shinier.com.br)"

OUT_STEP1="empresas_ativas_filtradas.csv"
OUT_STEP2="empresas_filtradas_por_cnae.csv"
OUT_STEP3="empresas_tecnologia_sc_ativas_enriquecidas.csv"
OUT_STEP4="empresas_tecnologia_sc_ativas_com_socios.csv"
JSON_OUT="empresas_tecnologia_sao_carlos.json"
GEOCACHE="geocache_enderecos.csv"

HTTP_PORT=8000
USE_VENV=1
VENV_DIR=".venv"

KEEP_MISSING_JSON=0
MAX_GEOCODE=""
RESET_GEOCACHE=0   # 1 = apaga o geocache antes de geocodificar (opcional)

#####################################
# Utilitários
#####################################
log()  { printf "\e[1;34m[INFO]\e[0m %s\n" "$*"; }
warn() { printf "\e[1;33m[WARN]\e[0m %s\n" "$*"; }
err()  { printf "\e[1;31m[ERRO]\e[0m %s\n" "$*" >&2; }

need_cmd() { command -v "$1" >/dev/null 2>&1 || { err "Comando '$1' não encontrado."; return 1; }; }

join_by() { local IFS="$1"; shift; echo "$*"; }

parse_yyyymm_from_name() {
  local f="$1" mm=""
  if [[ "$f" =~ K([0-9]{6})\. ]]; then
    mm="${BASH_REMATCH[1]}"
  elif [[ "$f" =~ \.D([0-9]{6})\. ]]; then
    mm="${BASH_REMATCH[1]}"
  fi
  printf '%s' "$mm"
}

# Verifica se um CSV possui TODAS as colunas informadas no header
have_cols() {
  local csv="$1"; shift
  [[ -s "$csv" ]] || return 1
  local hdr; hdr="$(head -n1 "$csv" | tr -d '\r')"
  for col in "$@"; do
    echo "$hdr" | grep -q -i -w "$col" || return 1
  done
  return 0
}

#####################################
# Dependências de sistema e Python
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
# Receita: utilidades de rede e nomes
#####################################
BASE_URL="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"

have_internet() {
  curl -fsSL --max-time 8 "$BASE_URL/" >/dev/null 2>&1
}

get_latest_month_url() {
  local html months latest
  if ! html="$(curl -fsSL "$BASE_URL/" 2>/dev/null)"; then
    return 1
  fi
  months="$(printf '%s' "$html" | grep -Eo 'href="20[0-9]{2}-[01][0-9]/"' | sed -E 's/^href="|\/"$//g' | sort -u || true)"
  [[ -z "$months" ]] && return 1
  latest="$(printf '%s\n' "$months" | sort | tail -n 1)"
  printf '%s/%s/\n' "$BASE_URL" "$latest"
}

yyyymm_from_month_url() {
  # Converte ".../2025-09/" -> "202509"
  if [[ $# -lt 1 || -z "${1:-}" ]]; then
    return 1
  fi
  local murl="$1"
  local seg
  seg="${murl%/}"
  seg="${seg##*/}"
  printf '%s\n' "${seg/-/}"
}

list_links() {
  local month_url="$1"
  curl -fsSL "$month_url" | grep -Eio 'href="[^"]+"' | sed -E 's/^href="|"$//g'
}

CATS=(ESTABELE EMPRECSV SOCIOCSV MUNICCSV CNAECSV QUALSCSV PAISCSV)

zip_regex_for_cat() {
  case "$1" in
    ESTABELE)  echo 'Estabelecimentos([0-9]+)\.zip' ;;
    EMPRECSV)  echo 'Empresas([0-9]+)\.zip' ;;
    SOCIOCSV)  echo 'Socios([0-9]+)\.zip' ;;
    MUNICCSV)  echo 'Municipios\.zip' ;;
    CNAECSV)   echo 'Cnaes\.zip' ;;
    QUALSCSV)  echo 'Qualificacoes\.zip' ;;
    PAISCSV)   echo 'Paises\.zip' ;;
    *)         echo '' ;;
  esac
}

canon_name() {
  local cat="$1" idx="$2" yyyymm="$3"
  case "$cat" in
    EMPRECSV)  printf 'K%s.EMPRECSV%s.csv\n'   "$yyyymm" "$idx" ;;
    ESTABELE)  printf 'K%s.ESTABELE%s.csv\n'   "$yyyymm" "$idx" ;;
    SOCIOCSV)  printf 'K%s.SOCIOCSV%s.csv\n'   "$yyyymm" "$idx" ;;
    MUNICCSV)  printf 'F.K03200$Z.D%s.MUNICCSV.csv\n' "$yyyymm" ;;
    CNAECSV)   printf 'F.K03200$Z.D%s.CNAECSV.csv\n'  "$yyyymm" ;;
    QUALSCSV)  printf 'F.K03200$Z.D%s.QUALSCSV.csv\n' "$yyyymm" ;;
    PAISCSV)   printf 'F.K03200$Z.D%s.PAISCSV.csv\n'  "$yyyymm" ;;
    *)         echo "" ;;
  esac
}

list_local_csvs_for_cat() {
  local cat="$1"
  shopt -s nullglob
  case "$cat" in
    EMPRECSV)  printf '%s\n' K*.EMPRECSV*.csv 2>/dev/null || true ;;
    ESTABELE)  printf '%s\n' K*.ESTABELE*.csv 2>/dev/null || true ;;
    SOCIOCSV)  printf '%s\n' K*.SOCIOCSV*.csv 2>/dev/null || true ;;
    MUNICCSV)  printf '%s\n' F.K03200\$Z.D*.MUNICCSV.csv 2>/dev/null || true ;;
    CNAECSV)   printf '%s\n' F.K03200\$Z.D*.CNAECSV.csv  2>/dev/null || true ;;
    QUALSCSV)  printf '%s\n' F.K03200\$Z.D*.QUALSCSV.csv 2>/dev/null || true ;;
    PAISCSV)   printf '%s\n' F.K03200\$Z.D*.PAISCSV.csv  2>/dev/null || true ;;
    *)         return 0 ;;
  esac
  return 0
}

expected_indices_from_html() {
  local cat="$1" html="$2" re
  re="$(zip_regex_for_cat "$cat")"
  [[ -z "$re" ]] && { echo ""; return 0; }
  if [[ "$cat" == "MUNICCSV" || "$cat" == "CNAECSV" || "$cat" == "QUALSCSV" || "$cat" == "PAISCSV" ]]; then
    if echo "$html" | grep -Eiq "$re"; then
      echo ""
    else
      echo ""
    fi
  else
    echo "$html" | grep -Eio "$re" | sed -E 's/[^0-9]//g' | sort -n | uniq | xargs || true
  fi
  return 0
}

any_local_zips_for_cat() {
  local cat="$1"
  shopt -s nullglob
  case "$cat" in
    EMPRECSV)  ls -1 Empresas*.zip K*.EMPRECSV*.zip  >/dev/null 2>&1 ;;
    ESTABELE)  ls -1 Estabelecimentos*.zip K*.ESTABELE*.zip >/dev/null 2>&1 ;;
    SOCIOCSV)  ls -1 Socios*.zip K*.SOCIOCSV*.zip >/dev/null 2>&1 ;;
    MUNICCSV)  ls -1 Municipios*.zip *MUNICCSV*.zip >/dev/null 2>&1 ;;
    CNAECSV)   ls -1 Cnaes*.zip *CNAECSV*.zip >/dev/null 2>&1 ;;
    QUALSCSV)  ls -1 Qualificacoes*.zip *QUALSCSV*.zip >/dev/null 2>&1 ;;
    PAISCSV)   ls -1 Paises*.zip *PAISCSV*.zip >/dev/null 2>&1 ;;
    *)         return 1 ;;
  esac
}

extract_one_zip_to_canonical() {
  local zip="$1" yyyymm="$2"
  local cat idx tmp moved dest

  local zbl="${zip,,}"
  if   [[ "$zbl" == *"estabelecimentos"* ]]; then cat="ESTABELE"
  elif [[ "$zbl" == *"empresas"* ]];          then cat="EMPRECSV"
  elif [[ "$zbl" == *"socios"* ]];            then cat="SOCIOCSV"
  elif [[ "$zbl" == *"municipios"* ]];        then cat="MUNICCSV"
  elif [[ "$zbl" == *"cnaes"* ]];             then cat="CNAECSV"
  elif [[ "$zbl" == *"qualificacoes"* ]];     then cat="QUALSCSV"
  elif [[ "$zbl" == *"paises"* ]];            then cat="PAISCSV"
  elif [[ "$zbl" == *"emrecsv"* || "$zbl" == *"estabele"* || "$zbl" == *"sociocsv"* || "$zbl" == *"qualscsv"* || "$zbl" == *"cnaecsv"* || "$zbl" == *"municcsv"* ]]; then
    if   [[ "$zbl" == *"emrecsv"*   ]]; then cat="EMPRECSV"
    elif [[ "$zbl" == *"estabele"*  ]]; then cat="ESTABELE"
    elif [[ "$zbl" == *"sociocsv"*  ]]; then cat="SOCIOCSV"
    elif [[ "$zbl" == *"qualscsv"*  ]]; then cat="QUALSCSV"
    elif [[ "$zbl" == *"cnaecsv"*   ]]; then cat="CNAECSV"
    elif [[ "$zbl" == *"municcsv"*  ]]; then cat="MUNICCSV"
    elif [[ "$zbl" == *"paiscsv"*   ]]; then cat="PAISCSV"
    fi
  else
    warn "Não consegui inferir a categoria do ZIP: $zip — pulando."
    return 0
  fi

  local base="${zip##*/}"; base="${base%.*}"
  if [[ "$base" =~ ([0-9]+)$ ]]; then
    idx="${BASH_REMATCH[1]}"
  else
    idx=""
  fi

  tmp="$(mktemp -d)"
  if ! unzip -q -o "$zip" -d "$tmp"; then
    warn "Falha ao extrair $(basename "$zip"); mantendo o ZIP."
    rm -rf "$tmp"
    return 0
  fi

  shopt -s nullglob
  moved=0
  for f in "$tmp"/*; do
    [[ -f "$f" ]] || continue
    case "$cat" in
      MUNICCSV|CNAECSV|QUALSCSV|PAISCSV) dest="$(canon_name "$cat" "" "$yyyymm")" ;;
      *)                         dest="$(canon_name "$cat" "$idx" "$yyyymm")" ;;
    esac
    [[ -z "$dest" ]] && continue
    if [[ ! -s "$dest" ]]; then
      mv -f "$f" "$dest"
      moved=1
    fi
  done
  rm -rf "$tmp"

  if rm -f "$zip"; then
    log "Apagado ZIP: $(basename "$zip")"
  fi

  # Não deixar um [[ ... ]] “seco” com set -e
  if [[ "$moved" -eq 0 ]]; then
    warn "Nenhum arquivo novo movido de $(basename "$zip") (provável duplicata)."
  fi

  return 0
}

extract_local_zips_for_cat() {
  local cat="$1" yyyymm="$2"
  shopt -s nullglob
  case "$cat" in
    EMPRECSV)  set -- Empresas*.zip K*.EMPRECSV*.zip ;;
    ESTABELE)  set -- Estabelecimentos*.zip K*.ESTABELE*.zip ;;
    SOCIOCSV)  set -- Socios*.zip K*.SOCIOCSV*.zip ;;
    MUNICCSV)  set -- Municipios*.zip *MUNICCSV*.zip ;;
    CNAECSV)   set -- Cnaes*.zip *CNAECSV*.zip ;;
    QUALSCSV)  set -- Qualificacoes*.zip *QUALSCSV*.zip ;;
    PAISCSV)   set -- Paises*.zip *PAISCSV*.zip ;;
    *)         return 0 ;;
  esac
  for z in "$@"; do
    [[ -f "$z" ]] && extract_one_zip_to_canonical "$z" "$yyyymm"
  done
  return 0
}

#####################################
# 1) Descoberta remota (mês vigente) e HTML
#####################################
REMOTE_MONTH_URL=""
REMOTE_YYYYMM=""
REMOTE_HTML=""

if have_internet; then
  if REMOTE_MONTH_URL="$(get_latest_month_url)"; then
    REMOTE_YYYYMM="$(yyyymm_from_month_url "$REMOTE_MONTH_URL")"
    REMOTE_HTML="$(curl -fsSL "$REMOTE_MONTH_URL")"
    log "Mês vigente na Receita: ${REMOTE_MONTH_URL} (YYYYMM=${REMOTE_YYYYMM})"
  else
    warn "Não consegui obter a pasta do mês vigente — seguirei sem validação remota."
  fi
else
  warn "Sem internet — validação remota de mês e completude indisponível."
fi

#####################################
# 2) Aquisição por CATEGORIA
#####################################
declare -A ACTION_SUMMARY=()
for CAT in "${CATS[@]}"; do
  log "Categoria ${CAT}: verificando CSV local…"
  local_csvs="$(list_local_csvs_for_cat "$CAT" || true)"
  have_csv=0
  latest_local=""
  if [[ -n "$local_csvs" ]]; then
    have_csv=1
    while IFS= read -r f; do
      [[ -z "$f" || ! -e "$f" ]] && continue
      mm="$(parse_yyyymm_from_name "$f")"
      [[ -z "$mm" ]] && continue
      if [[ -z "$latest_local" || "$mm" > "$latest_local" ]]; then
        latest_local="$mm"
      fi
    done <<< "$local_csvs"
  fi

  need_update=0
  complete_ok=1

  if [[ "$have_csv" -eq 1 ]]; then
    if [[ -n "$REMOTE_YYYYMM" ]]; then
      if [[ -n "$latest_local" && "$latest_local" == "$REMOTE_YYYYMM" ]]; then
        exp_idx="$(expected_indices_from_html "$CAT" "$REMOTE_HTML" || true)"
        if [[ "$CAT" == "MUNICCSV" || "$CAT" == "CNAECSV" || "$CAT" == "QUALSCSV"  || "$CAT" == "PAISCSV" ]]; then
          want="$(canon_name "$CAT" "" "$REMOTE_YYYYMM")"
          if [[ ! -s "$want" ]]; then complete_ok=0; fi
        else
          for i in $exp_idx; do
            want="$(canon_name "$CAT" "$i" "$REMOTE_YYYYMM")"
            [[ -s "$want" ]] || complete_ok=0
          done
        fi
        if [[ "$complete_ok" -eq 1 ]]; then
          ACTION_SUMMARY["$CAT"]="CSV OK (mês $REMOTE_YYYYMM) — completo."
          continue
        else
          need_update=1
          warn "Categoria $CAT está no mês vigente mas incompleta — vou tentar preencher."
        fi
      elif [[ -n "$latest_local" && "$latest_local" != "$REMOTE_YYYYMM" ]]; then
        need_update=1
        warn "Categoria $CAT desatualizada (local=$latest_local, remoto=$REMOTE_YYYYMM) — vou atualizar."
      else
        need_update=1
      fi
    else
      ACTION_SUMMARY["$CAT"]="CSV presente (mês desconhecido: $latest_local)."
      continue
    fi
  else
    need_update=1
    log "Sem CSV local para $CAT."
  fi

  if [[ "$need_update" -eq 1 ]]; then
    if any_local_zips_for_cat "$CAT"; then
      yyyymm_use="${REMOTE_YYYYMM:-$(date +%Y%m)}"
      [[ -z "${REMOTE_YYYYMM:-}" ]] && warn "Sem mês remoto; usando YYYYMM atual ($yyyymm_use) para nomear."
      log "Extraindo ZIPs locais de $CAT…"
      extract_local_zips_for_cat "$CAT" "$yyyymm_use"
    fi

    missing_list=()
    if [[ -n "$REMOTE_YYYYMM" && -n "$REMOTE_HTML" ]]; then
      exp_idx="$(expected_indices_from_html "$CAT" "$REMOTE_HTML" || true)"
      if [[ "$CAT" == "MUNICCSV" || "$CAT" == "CNAECSV" || "$CAT" == "QUALSCSV" ]]; then
        want="$(canon_name "$CAT" "" "$REMOTE_YYYYMM")"
        [[ -s "$want" ]] || missing_list+=("$want")
      else
        for i in $exp_idx; do
          want="$(canon_name "$CAT" "$i" "$REMOTE_YYYYMM")"
          [[ -s "$want" ]] || missing_list+=("$want")
        done
      fi
    fi

    if [[ ${#missing_list[@]} -gt 0 ]]; then
      if [[ -z "$REMOTE_MONTH_URL" ]]; then
        err "Faltam arquivos para $CAT e não há conexão para baixar. Coloque os ZIPs na pasta e rode novamente."
        exit 1
      fi
      log "Baixando partes em falta para $CAT…"
      links="$(list_links "$REMOTE_MONTH_URL" || true)"
      re="$(zip_regex_for_cat "$CAT")"
      mapfile -t cat_links < <(printf '%s\n' "${links:-}" | grep -E -i "$re" || true)
      if [[ ${#cat_links[@]} -eq 0 ]]; then
        err "Não encontrei ZIPs de $CAT em $REMOTE_MONTH_URL."
        exit 1
      fi
      for href in "${cat_links[@]}"; do
        url="${REMOTE_MONTH_URL}${href}"
        log "  wget -c $url"
        wget -q -c "$url" || { err "Falha ao baixar $url"; exit 1; }
      done
      extract_local_zips_for_cat "$CAT" "$REMOTE_YYYYMM"
    fi

    final_ok=1
    if [[ -n "$REMOTE_YYYYMM" && -n "$REMOTE_HTML" ]]; then
      exp_idx="$(expected_indices_from_html "$CAT" "$REMOTE_HTML" || true)"
      if [[ "$CAT" == "MUNICCSV" || "$CAT" == "CNAECSV" || "$CAT" == "QUALSCSV" ]]; then
        want="$(canon_name "$CAT" "" "$REMOTE_YYYYMM")"
        [[ -s "$want" ]] || final_ok=0
      else
        for i in $exp_idx; do
          want="$(canon_name "$CAT" "$i" "$REMOTE_YYYYMM")"
          [[ -s "$want" ]] || final_ok=0
        done
      fi
      if [[ "$final_ok" -eq 1 ]]; then
        ACTION_SUMMARY["$CAT"]="CSV pronto após atualização (mês $REMOTE_YYYYMM)."
      else
        err "Ainda faltam partes para $CAT após tentativa de atualização."
        exit 1
      fi
    else
      ACTION_SUMMARY["$CAT"]="CSV pronto (mês local, sem validação remota)."
    fi
  fi
done

log "Resumo de aquisição por categoria:"
for CAT in "${CATS[@]}"; do
  printf "  - %s: %s\n" "$CAT" "${ACTION_SUMMARY[$CAT]:-sem ação}"
done

#####################################
# 3) Executar pipeline de .py (com retomada + validação de contatos)
#####################################
# Passo 1: se não existir OU não tiver colunas de contato, refaz
if [[ -s "$OUT_STEP1" ]] && have_cols "$OUT_STEP1" "email" "telefone_1_full" "telefone_2_full" "telefones_norm"; then
  log "P1 já existe ($OUT_STEP1) — colunas de contato OK — pulando."
else
  log "P1: filtro_cidades_ativas.py → $OUT_STEP1"
  rm -f "$OUT_STEP1"
  CITY_ARGS=()
  for c in "${CITIES[@]}"; do CITY_ARGS+=("-c" "$c"); done
  $PY filtro_cidades_ativas.py "${CITY_ARGS[@]}" --uf "$UF" --out "$OUT_STEP1" --chunksize "$CHUNKSIZE"
fi

# Passo 2: idem — precisa carregar as colunas de contato adiante
if [[ -s "$OUT_STEP2" ]] && have_cols "$OUT_STEP2" "email" "telefone_1_full" "telefone_2_full" "telefones_norm"; then
  log "P2 já existe ($OUT_STEP2) — colunas de contato OK — pulando."
else
  log "P2: filtra_por_cnae.py → $OUT_STEP2"
  rm -f "$OUT_STEP2"
  CNAE_ARGS=()
  for k in "${CNAES[@]}"; do CNAE_ARGS+=("$k"); done
  $PY filtra_por_cnae.py --in "$OUT_STEP1" --cnae "${CNAE_ARGS[@]}" --out "$OUT_STEP2"
fi

# P3 e P4 agora preservam contatos, então só refaça se não existirem
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

# (Opcional) reset geocache
if [[ "$RESET_GEOCACHE" -eq 1 ]]; then
  log "Limpando geocache ($GEOCACHE)…"
  rm -f "$GEOCACHE"
fi

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
# 4) Abrir mapa.html apontando para o JSON
#####################################
if [[ ! -f "mapa.html" ]]; then
  err "mapa.html não encontrado na pasta. Coloque o arquivo e rode novamente."
  exit 1
fi

log "Subindo servidor local em http://localhost:${HTTP_PORT} (CTRL+C para parar)…"
if lsof -i TCP:"$HTTP_PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then
  warn "Porta $HTTP_PORT já ocupada; tentando encerrar processo existente…"
  kill "$(lsof -i TCP:"$HTTP_PORT" -sTCP:LISTEN -t)" || true
  sleep 1
fi

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