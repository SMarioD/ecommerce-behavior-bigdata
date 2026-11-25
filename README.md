# E-commerce User Behavior Analytics (Big Data)

Proiect Big Data — Master An 1. Echipa **Data Shoppers**: Mario-Daniel Stoian, Octavian-Mihai Mutică.

Analiza comportamentului utilizatorilor unei platforme e-commerce: pipeline Big Data pentru colectarea, curățarea și analiza clickstream-ului și a tranzacțiilor, identificarea tiparelor de comportament și a segmentelor de clienți, plus un dashboard interactiv cu indicatori de performanță.

## Dataset

**REES46 — eCommerce behavior data from multi category store**
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

Fișierele `2019-Oct.csv` și `2019-Nov.csv` se descarcă de pe Kaggle și se pun în `data/raw/rees46/`. Datele nu sunt incluse în repo.

Dacă fișierele vin arhivate (`.csv.gz`), se pot citi direct cu `--files 2019-Oct.csv.gz` la pasul 2, dar ingestia e mai lentă, pentru că un fișier gzip nu se poate împărți pe bucăți. Varianta rapidă este dezarhivarea cu păstrarea arhivei: `docker exec bd-spark gunzip -k /workspace/data/raw/rees46/2019-Oct.csv.gz`.

## Stack

Python, Apache Spark (PySpark), Parquet (bronze / silver / gold), Apache Kafka + Spark Structured Streaming, Spark MLlib (K-Means, FP-Growth), Streamlit + Plotly, Docker.

## Arhitectură

```
CSV  ->  bronze  ->  silver/events  ->  silver/sessions  ->  gold  ->  dashboard
         (brut)      (curățat)          (o linie/sesiune)    (KPI, segmente, reguli)
```

| Strat | Conținut | Scris de |
|---|---|---|
| `data/bronze/events` | datele brute în Parquet, partiționate pe zi | `src.ingest.csv_to_bronze` |
| `data/silver/events` | evenimente curățate și tipizate | `src.clean.bronze_to_silver` |
| `data/silver/sessions` | o linie pe sesiune, cu outlierii marcați | `src.clean.build_sessions` |
| `data/gold/*` | tabele de rezultate pentru dashboard | `src.analysis.*` |
| `data/sample/*` | eșantion de o zi pentru dezvoltare rapidă | `src.ingest.make_sample` și `src.analysis.* --sample` |

## Structură

```
data/        date locale (ignorate de Git)
src/
  common/    sesiunea Spark și căile proiectului
  ingest/    CSV -> bronze, eșantion
  clean/     bronze -> silver (curățare, sesiuni)
  analysis/  KPI, trăsături per utilizator, K-Means, FP-Growth
  streaming/ Kafka + Spark Structured Streaming
dashboard/   aplicația Streamlit
notebooks/   explorare
```

## Rulare

Cerințe: Docker Desktop pornit (în colțul din stânga jos trebuie să apară „Engine running”) și cel puțin 16 GB RAM pe calculator.

### A. Pipeline de date

```
# 1. Pornește mediul (prima dată construiește imaginea, ~2 minute)
docker compose up -d --build

# 2. CSV -> bronze (Parquet partiționat pe zi), ~2 minute pentru octombrie
docker exec bd-spark python -m src.ingest.csv_to_bronze

# 3. bronze -> silver/events (curățare), ~8 minute
docker exec bd-spark python -m src.clean.bronze_to_silver

# 4. silver/events -> silver/sessions, ~4 minute
docker exec bd-spark python -m src.clean.build_sessions

# 5. Eșantion de o zi pentru dezvoltare rapidă -> data/sample/events, data/sample/sessions
docker exec bd-spark python -m src.ingest.make_sample
```

### B. Analiză (gold)

Rulează după pasul 4 (sau după 5, cu `--sample`). Fiecare script citește din silver și scrie în `data/gold`.

```
# 6. KPI, funnel, clasamente produse/branduri/categorii, activitate pe oră
docker exec bd-spark python -m src.analysis.kpis

# 7. Trăsături per utilizator: RFM + comportament
docker exec bd-spark python -m src.analysis.user_features

# 8. Segmentare K-Means (--tune afișează silhouette pentru k = 2..8)
docker exec bd-spark python -m src.analysis.segments --k 5 --tune

# 9. Reguli de asociere FP-Growth (categorii; --level brand pentru branduri)
docker exec bd-spark python -m src.analysis.patterns --min-support 0.001
```

Toate scripturile din `src.analysis` acceptă `--sample`: citesc eșantionul de o zi și scriu în `data/sample/gold`. Merg în câteva zeci de secunde, bune pentru un test rapid sau pentru demo:

```
docker exec bd-spark python -m src.analysis.kpis --sample
docker exec bd-spark python -m src.analysis.user_features --sample
docker exec bd-spark python -m src.analysis.segments --sample --k 4
docker exec bd-spark python -m src.analysis.patterns --sample --min-support 0.003
```

Pragul `--min-support` de la FP-Growth depinde de volumul datelor. Dacă nu apare nicio regulă, se coboară (și eventual `--min-confidence`).

### C. Dashboard

Pe calculator, nu în container (portul 8501 nu e expus în `docker-compose.yml`), în folderul proiectului:

```
pip install streamlit plotly pandas pyarrow
streamlit run dashboard/app.py
```

Se deschide la http://localhost:8501. Dashboard-ul citește din `data/gold`, iar dacă acesta nu există încă, din `data/sample/gold`. Paginile:

- **KPI**: sesiuni, venit, conversie, valoare medie a comenzii, top produse, branduri, categorii.
- **Funnel**: vizualizare, coș, cumpărare, plus abandonul de coș.
- **Segmente**: segmentele K-Means, profilul lor și proiecția 2D.
- **Tipare**: activitatea pe oră și zi, reguli de asociere.
- **Live**: replay al eșantionului de o zi; se leagă la fluxul Kafka când e gata partea de streaming.

## Ce produce analiza (`data/gold`)

| Tabel | Conținut |
|---|---|
| `kpi_daily` | pe zi: evenimente, utilizatori, sesiuni, venit, conversie, valoare medie comandă |
| `funnel_daily` | pe zi: sesiuni cu view, cu coș, cu cumpărare |
| `top_products`, `top_brands`, `top_categories` | clasamente după venit |
| `hourly_activity` | evenimente pe zi a săptămânii, oră și tip |
| `user_features` | un rând pe utilizator: recență, frecvență, valoare monetară, comportament |
| `user_segments`, `segment_profile`, `segments_pca`, `kmeans_tuning` | rezultatele K-Means |
| `assoc_rules_*`, `freq_itemsets_*` | reguli și seturi frecvente FP-Growth |

Definiții folosite în analiză:

- **Funnel**: „ajunge la coș” înseamnă sesiune cu cart sau purchase. Peste jumătate dintre sesiunile cu cumpărare nu au eveniment cart, deci un funnel pe evenimente brute ar ieși greșit.
- **Abandon de coș**: sesiuni care ajung la coș, dar nu cumpără.
- **Valoarea medie a comenzii**: venit împărțit la sesiunile cu cumpărare (datele nu au id de comandă).
- **Outlieri**: sesiunile cu peste 200 de evenimente sau peste 24h sunt marcate în silver și excluse din toate analizele.
- **Coș pentru FP-Growth**: elementele distincte puse în coș sau cumpărate în aceeași sesiune.

## Unde vezi rezultatele

JupyterLab: http://localhost:8888/lab?token=bigdata (pentru citire rapidă: `pd.read_parquet("data/gold/kpi_daily")`)
Spark UI (doar cât rulează un job): http://localhost:4040

Memoria pentru Spark se poate schimba cu variabila `SPARK_DRIVER_MEMORY` (implicit `6g`). Fișierele temporare Spark se scriu în `data/tmp`, lângă date, nu în discul Docker.
