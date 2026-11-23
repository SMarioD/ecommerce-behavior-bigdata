# E-commerce User Behavior Analytics (Big Data)

Proiect Big Data — Master An 1. Echipa **Data Shoppers**: Mario-Daniel Stoian, Octavian-Mihai Mutică.

Analiza comportamentului utilizatorilor unei platforme e-commerce: pipeline Big Data pentru colectarea, curățarea și analiza clickstream-ului și a tranzacțiilor, identificarea tiparelor de comportament și a segmentelor de clienți, plus un dashboard interactiv cu indicatori de performanță.

## Dataset

**REES46 — eCommerce behavior data from multi category store**
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

Fișierele `2019-Oct.csv` și `2019-Nov.csv` se descarcă de pe Kaggle și se pun în `data/raw/rees46/`. Datele nu sunt incluse în repo.

## Stack

Python, Apache Spark (PySpark), Parquet (bronze / silver / gold), Apache Kafka + Spark Structured Streaming, Spark MLlib (K-Means, FP-Growth), Streamlit + Plotly, Docker.

## Structură

```
data/        date locale (ignorate de Git)
src/         ingestie, curățare, analiză, streaming
dashboard/   aplicația Streamlit
notebooks/   explorare
docs/        schema datelor, raport, prezentare
```

## Rulare

Instrucțiunile se adaugă pe măsură ce pipeline-ul este construit.
