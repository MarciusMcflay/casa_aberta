# Casa Aberta — SENAC

## Extração, filtragem e enriquecimento de dados públicos do CNPJ

Este repositório traz um projeto demonstrativo do **Casa Aberta do SENAC** para analisar o ecossistema de **empresas de tecnologia de São Carlos (SP)** com base nos **Dados Abertos do CNPJ** (Receita Federal do Brasil). A arquitetura é **modular, robusta e extensível**, permitindo replicar o pipeline para **qualquer cidade/UF** e **qualquer conjunto de CNAEs** (não apenas tecnologia), com enriquecimentos adicionais a partir dos arquivos oficiais.

---

## Objetivos

* Mapear o ecossistema local (ex.: tecnologia em São Carlos) a partir de dados oficiais.
* Construir uma base **confiável e deduplicada** de CNPJs **ativos**, com **endereço** e **CNAEs** principal/segundários.
* **Enriquecer** com razão social, porte, capital social, qualificação do responsável e sócios.
* **Geocodificar** endereços e produzir um **JSON georreferenciado** para visualização em mapa (com *clustering*).
* Generalizar o processo para **qualquer segmento** e **qualquer município/UF**.

---

## Arquitetura do pipeline (resumo)

1. **Download/extração** (mês vigente) dos conjuntos necessários (ESTABELE, EMPRECSV, SOCIOCSV, MUNICCSV, CNAECSV, QUALSCSV e opcionalmente PAISCSV).
2. **Filtragem**: estabelecimentos **ATIVOS** por cidade(s)/UF, já trazendo CNAEs e endereço.
3. **Segmentação por CNAE**: filtra por lista de CNAEs (principal ou secundários).
4. **Enriquecimento**: *join* com EMPRECSV (razão social, porte, capital; decodificação de qualificação via QUALSCSV) e *join* com SOCIOCSV (opcional PAISCSV).
5. **Geocodificação** com cache e geração de **JSON** para o **viewer** (`mapa.html`).

> O pipeline é **retomável**: se algum passo já gerou saída, ele pula para o próximo.

---

## Requisitos

* **SO**: Ubuntu/WSL2/Linux (testado em Ubuntu 22.04).
* **Python**: 3.10+ (3.12 também funciona).
* **Ferramentas**: `curl`, `wget`, `unzip`, `lsof`.
* **Bibliotecas Python**: `pandas`, `geopy`, `folium` (instaladas automaticamente pelo script).

> Em WSL, se houver problemas de rede/DNS, ajuste `/etc/resolv.conf` (ex.: `1.1.1.1` e `8.8.8.8`) e reinicie o WSL.

---

## Arquivos principais

| Arquivo                    | Função                                                                             |
| -------------------------- | ---------------------------------------------------------------------------------- |
| `pipeline_empresas.sh`     | Orquestra a execução completa; baixa/extrai, roda os .py, gera JSON e abre o mapa. |
| `filtro_cidades_ativas.py` | Filtra ATIVAS por cidade/UF em *chunks*; inclui CNAEs e endereço.                  |
| `filtra_por_cnae.py`       | Filtra por lista de CNAEs (principal e secundários).                               |
| `merge_com_empresas.py`    | *Join* com EMPRECSV; adiciona razão social, porte, capital; decodifica QUALSCSV.   |
| `merge_socios.py`          | *Join* com SOCIOCSV; agrega sócios/representante; usa PAISCSV (opcional).          |
| `mapa.py`                  | Geocodifica (com cache) e gera **JSON** para o viewer.                             |
| `mapa.html`                | Viewer estático que lê o **JSON** e exibe o mapa com *marker cluster*.             |

> O código detecta automaticamente diferenças de layout (ex.: ESTABELE com 30/31 colunas; MUNICCSV com 2/3 colunas).

---

## Como executar (one-click)

```bash
chmod +x pipeline_empresas.sh
./pipeline_empresas.sh
```

O script:

* Instala dependências (inclui `python3-venv` compatível com sua versão).
* Cria `venv` (padrão) e instala libs (`pandas`, `geopy`, `folium`).
* Descobre o **mês mais recente** no diretório oficial da Receita, baixa e **extrai** os arquivos necessários.
* Roda os `.py` em ordem e **retoma** se a saída de algum passo já existir.
* **Geocodifica** com cache (1 req/s Nominatim) e **gera o JSON**.
* Sobe um `http.server` local e **abre o `mapa.html`** apontando para o JSON produzido.

Saídas típicas:

* `empresas_ativas_filtradas.csv`
* `empresas_filtradas_por_cnae.csv`
* `empresas_tecnologia_sc_ativas_enriquecidas.csv`
* `empresas_tecnologia_sc_ativas_com_socios.csv`
* `geocache_enderecos.csv`
* `empresas_tecnologia_sao_carlos.json`

---

## Customização

### Cidades / UF

Edite no topo do `pipeline_empresas.sh`:

```bash
CITIES=("São Carlos" "Araraquara")
UF="SP"
```

A filtragem por cidades usa `MUNICCSV` e é **case/acentos-insensitive**. O endereço final preserva a **forma digitada** (ex.: “São Carlos”).

### CNAEs

Edite a lista:

```bash
CNAES=("6201501" "6201502" "6202300" "6311900")
```

Qualquer segmento pode ser analisado (saúde, indústria, comércio, etc.).

### Sem venv (instala no sistema)

```bash
USE_VENV=0 ./pipeline_empresas.sh
```

### Geocodificação — limitar/permitir missing

```bash
MAX_GEOCODE=200 KEEP_MISSING_JSON=1 ./pipeline_empresas.sh
```

---

## Execução manual (avançado)

1. **Filtrar ATIVAS por cidades/UF**:

```bash
python filtro_cidades_ativas.py \
  -c "São Carlos" -c "Araraquara" \
  --uf "SP" \
  --out empresas_ativas_filtradas.csv \
  --chunksize 300000
```

2. **Filtrar por CNAEs**:

```bash
python filtra_por_cnae.py \
  --in empresas_ativas_filtradas.csv \
  --cnae 6201501 6202300 6311900 \
  --out empresas_filtradas_por_cnae.csv
```

3. **Enriquecer com EMPRECSV (+ QUALSCSV)**:

```bash
python merge_com_empresas.py \
  --in empresas_filtradas_por_cnae.csv \
  --out empresas_tecnologia_sc_ativas_enriquecidas.csv \
  --chunksize 300000
```

4. **Anexar SÓCIOS** (SOCIOCSV; PAISCSV opcional):

```bash
python merge_socios.py \
  --in empresas_tecnologia_sc_ativas_enriquecidas.csv \
  --out empresas_tecnologia_sc_ativas_com_socios.csv \
  --chunksize 300000
```

5. **Geocodificar e gerar JSON**:

```bash
python mapa.py \
  --base empresas_filtradas_por_cnae.csv \
  --enriched empresas_tecnologia_sc_ativas_com_socios.csv \
  --geocache geocache_enderecos.csv \
  --out-json empresas_tecnologia_sao_carlos.json \
  --city "São Carlos" --uf "SP" \
  --user-agent "seu-projeto/1.0 (contato: voce@dominio.com)"
```

Abra o `mapa.html` no navegador e informe:

```
mapa.html?data=empresas_tecnologia_sao_carlos.json
```

---

## Desempenho e memória

* Leituras CSV em **chunks** para não estourar RAM (padrão: `300000`; ajuste conforme hardware).
* Em máquinas com pouca memória: use `100000` ou `50000`.
* Há utilitário opcional de *sizing* para estimar bytes/linha e sugerir `chunksize`.

---

## Resiliência a formatos

* `ESTABELE` com **30** ou **31** colunas → detecção dinâmica.
* `MUNICCSV` com **2** (código/nome) ou **3** colunas (com UF) → tratado automaticamente.
* Leituras com `on_bad_lines="skip"` e `keep_default_na=False` para reduzir quebras.
* Normalização robusta (evita erros como `float` sem `.strip()`).

---

## Geocodificação (boas práticas)

* Usa **Nominatim** (OpenStreetMap). Respeite:

  * **Rate limit** \~1 req/s (já aplicado por `RateLimiter`).
  * **User-Agent** válido com contato.
  * **Cache local** (`geocache_enderecos.csv`) para reexecuções.
* Para volumes muito grandes, considere provedores com SLA.

---

## Conformidade (LGPD/termos)

* Respeitar **termos de uso** dos Dados Abertos do CNPJ e diretrizes de **LGPD**.
* Dados são **públicos**; evite usos discriminatórios e preserve a finalidade informada.
* Em publicações, **documente a fonte e a data** de extração.

---

## Troubleshooting

* **WSL sem rede/DNS** → ajuste `resolv.conf` e reinicie WSL.
* **`ensurepip is not available`** → instale `python3-venv` e `pythonX.Y-venv` e recrie o `venv`.
* **Erro de parsing CSV** → reduza `--chunksize`; já usamos `on_bad_lines="skip"`.
* **Timeouts de geocode** → reexecute; o cache mantém resultados.
* **Ausência de registros** → valide UF/cidades no `MUNICCSV` e parâmetros da linha de comando.

---

## Extensões

* Novos enriquecimentos (parques tecnológicos, fomento, inovação).
* Painéis (Dash/Streamlit) lendo o **JSON** georreferenciado.
* Séries temporais para acompanhar aberturas/fechamentos.

---

## Licença e créditos

* Código: defina sua **licença** (MIT/Apache-2.0, etc.).
* Dados: **Receita Federal do Brasil — Dados Abertos do CNPJ**.
* Geocodificação: **Nominatim (OpenStreetMap)** — respeite termos e políticas.
