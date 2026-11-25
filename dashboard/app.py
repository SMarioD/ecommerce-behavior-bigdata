"""Dashboard Streamlit: KPI, Funnel, Segmente, Tipare, Live.

Citește tabelele din data/gold (generate de src/analysis/*). Dacă nu există încă,
folosește data/sample/gold (eșantionul de o zi), ca demo-ul să meargă oricând.

Rulare (pe calculator, în folderul proiectului):
    pip install streamlit plotly pandas pyarrow
    streamlit run dashboard/app.py
"""
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
GOLD = DATA / "gold" if (DATA / "gold" / "kpi_daily").exists() else DATA / "sample" / "gold"
SAMPLE_EVENTS = DATA / "sample" / "events"

WEEKDAYS = {1: "Dum", 2: "Lun", 3: "Mar", 4: "Mie", 5: "Joi", 6: "Vin", 7: "Sâm"}
TYPE_RO = {"view": "Vizualizări", "cart": "Adăugări în coș", "purchase": "Cumpărări"}

st.set_page_config(page_title="E-commerce User Behavior Analytics", page_icon="🛒", layout="wide")


@st.cache_data
def load(name: str):
    path = GOLD / name
    return pd.read_parquet(path) if path.exists() else None


def need(df, script: str):
    """Mesaj prietenos când un tabel nu a fost generat încă."""
    if df is None:
        st.info(f"Tabelul lipsește. Rulează `{script}` și reîncarcă pagina.")
        st.stop()
    return df


def fmt_money(x: float) -> str:
    return f"{x:,.0f}"


# ---------------------------------------------------------------------------------------------
def page_kpi():
    st.header("Indicatori de performanță")
    daily = need(load("kpi_daily"), "python -m src.analysis.kpis")

    sessions, purchases = daily["n_sessions"].sum(), daily["sessions_purchase"].sum()
    revenue = daily["revenue"].sum()
    c = st.columns(5)
    c[0].metric("Sesiuni", f"{sessions:,}")
    c[1].metric("Utilizatori (suma pe zile)", f"{daily['n_users'].sum():,}")
    c[2].metric("Venit total", fmt_money(revenue))
    c[3].metric("Conversie (sesiuni cu cumpărare)", f"{purchases / sessions:.2%}")
    c[4].metric("Valoare medie comandă", f"{revenue / max(purchases, 1):,.2f}")

    if len(daily) > 1:
        left, right = st.columns(2)
        left.plotly_chart(px.line(daily, x="event_date", y="revenue", markers=True,
                                  title="Venit pe zi"), use_container_width=True)
        right.plotly_chart(px.line(daily, x="event_date", y="conversion_rate", markers=True,
                                   title="Rata de conversie pe zi"), use_container_width=True)
    else:
        st.caption("Datele conțin o singură zi (eșantion), de aceea nu se afișează evoluția în timp.")

    st.subheader("Clasamente")
    t_prod, t_brand, t_cat = st.tabs(["Produse", "Branduri", "Categorii"])
    with t_prod:
        prod = need(load("top_products"), "python -m src.analysis.kpis")
        n = st.slider("Câte produse", 5, min(50, len(prod)), min(15, len(prod)), key="np")
        top = prod.head(n).copy()
        top["produs"] = top["product_id"].astype(str)
        fig = px.bar(top, x="revenue", y="produs", orientation="h", color="brand",
                     hover_data=["category_code", "n_views", "n_carts", "n_purchases"],
                     title="Top produse după venit")
        fig.update_yaxes(autorange="reversed", type="category")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(prod, use_container_width=True, hide_index=True)
    with t_brand:
        br = need(load("top_brands"), "python -m src.analysis.kpis")
        n = st.slider("Câte branduri", 5, min(30, len(br)), min(15, len(br)), key="nb")
        fig = px.bar(br.head(n), x="revenue", y="brand", orientation="h",
                     color="view_to_purchase", color_continuous_scale="Blues",
                     title="Top branduri după venit (culoarea = cumpărări / vizualizări)")
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)
    with t_cat:
        cat = need(load("top_categories"), "python -m src.analysis.kpis")
        l1 = cat.groupby("category_l1", as_index=False)[["revenue", "n_views", "n_purchases"]].sum()
        l1 = l1.sort_values("revenue", ascending=False)
        st.plotly_chart(px.treemap(cat[cat["revenue"] > 0], path=["category_l1", "category_l2"],
                                   values="revenue", title="Venit pe categorii"),
                        use_container_width=True)
        st.dataframe(l1, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------------------------
def page_funnel():
    st.header("Funnel de conversie")
    f = need(load("funnel_daily"), "python -m src.analysis.kpis")
    total = f[["n_sessions", "sessions_view", "sessions_cart", "sessions_purchase"]].sum()

    steps = ["Sesiuni cu vizualizare", "Ajung la coș", "Cumpără"]
    values = [total["sessions_view"], total["sessions_cart"], total["sessions_purchase"]]
    st.plotly_chart(go.Figure(go.Funnel(y=steps, x=values, textinfo="value+percent previous")),
                    use_container_width=True)

    abandon = 1 - values[2] / max(values[1], 1)
    c = st.columns(3)
    c[0].metric("Vizualizare → coș", f"{values[1] / max(values[0], 1):.1%}")
    c[1].metric("Abandon de coș", f"{abandon:.1%}")
    c[2].metric("Conversie globală", f"{values[2] / max(total['n_sessions'], 1):.2%}")
    st.caption("„Ajung la coș” = sesiuni cu cart sau purchase. Peste jumătate dintre sesiunile cu "
               "cumpărare nu au eveniment cart, așa că un funnel pe evenimente brute ar ieși greșit. "
               "Abandon de coș = ajung la coș, dar nu cumpără.")
    if len(f) > 1:
        f = f.assign(abandon=1 - f["sessions_purchase"] / f["sessions_cart"].clip(lower=1))
        st.plotly_chart(px.line(f, x="event_date", y="abandon", markers=True,
                                title="Abandon de coș pe zi"), use_container_width=True)


# ---------------------------------------------------------------------------------------------
def page_segments():
    st.header("Segmente de clienți (K-Means)")
    prof = need(load("segment_profile"), "python -m src.analysis.segments")
    prof = prof.sort_values("n_users", ascending=False)

    left, right = st.columns([1, 1])
    left.plotly_chart(px.pie(prof, names="segment", values="n_users", hole=0.45,
                             title="Ponderea segmentelor"), use_container_width=True)
    pca = load("segments_pca")
    if pca is not None:
        right.plotly_chart(px.scatter(pca, x="x", y="y", color="segment", opacity=0.6,
                                      title="Utilizatori proiectați în 2D (PCA)"),
                           use_container_width=True)

    st.subheader("Profilul segmentelor")
    cols = {"segment": "Segment", "n_users": "Utilizatori", "share": "Pondere",
            "buyer_share": "% cumpărători", "revenue": "Venit mediu", "n_purchases": "Cumpărări medii",
            "recency_days": "Recență (zile)", "n_sessions": "Sesiuni medii",
            "cart_session_rate": "Sesiuni cu coș", "avg_events_per_session": "Evenimente/sesiune"}
    st.dataframe(prof[list(cols)].rename(columns=cols), use_container_width=True, hide_index=True)

    metric = st.selectbox("Compară segmentele după", ["revenue", "n_purchases", "recency_days",
                                                      "n_sessions", "cart_session_rate",
                                                      "avg_events_per_session", "avg_view_price"],
                          format_func=lambda m: cols.get(m, m.replace("_", " ").capitalize()))
    st.plotly_chart(px.bar(prof, x="segment", y=metric, color="segment"), use_container_width=True)

    tune = load("kmeans_tuning")
    if tune is not None:
        with st.expander("Alegerea lui k"):
            st.plotly_chart(px.line(tune, x="k", y="silhouette", markers=True,
                                    title="Silhouette în funcție de k"), use_container_width=True)
    st.caption("Trăsături: recență, frecvență, valoare monetară (RFM) și comportament în sesiuni; "
               "logaritmate și standardizate înainte de K-Means. Denumirile sunt atribuite din profil.")


# ---------------------------------------------------------------------------------------------
def page_patterns():
    st.header("Tipare de comportament")
    hourly = need(load("hourly_activity"), "python -m src.analysis.kpis")

    st.subheader("Când sunt activi utilizatorii")
    etype = st.radio("Tip de eveniment", list(TYPE_RO), format_func=TYPE_RO.get, horizontal=True)
    h = hourly[hourly["event_type"] == etype]
    if h["weekday"].nunique() > 1:
        grid = h.pivot_table(index="weekday", columns="hour", values="n_events", aggfunc="sum").fillna(0)
        grid.index = [WEEKDAYS[i] for i in grid.index]
        st.plotly_chart(px.imshow(grid, aspect="auto", color_continuous_scale="Viridis",
                                  labels=dict(x="Ora (UTC)", y="Ziua", color="Evenimente")),
                        use_container_width=True)
    else:
        by_hour = h.groupby("hour", as_index=False)["n_events"].sum()
        st.plotly_chart(px.bar(by_hour, x="hour", y="n_events",
                               labels=dict(hour="Ora (UTC)", n_events="Evenimente")),
                        use_container_width=True)

    st.subheader("Reguli de asociere (FP-Growth)")
    level = st.radio("Elemente", ["category", "brand"], horizontal=True,
                     format_func=lambda x: "Categorii" if x == "category" else "Branduri")
    rules = load(f"assoc_rules_{level}")
    if rules is None:
        st.info(f"Rulează `python -m src.analysis.patterns --level {level}`.")
        return
    if rules.empty:
        st.warning("Nicio regulă la pragurile alese. Coboară min-support sau min-confidence.")
        return
    min_lift = st.slider("Lift minim", 1.0, float(max(rules["lift"].max(), 1.5)), 1.0, 0.1)
    r = rules[rules["lift"] >= min_lift].sort_values("lift", ascending=False)
    st.dataframe(r, use_container_width=True, hide_index=True)
    st.plotly_chart(px.scatter(r, x="support", y="confidence", size="lift", color="lift",
                               hover_data=["antecedent", "consequent"],
                               title="Suport vs. încredere (mărimea = lift)"),
                    use_container_width=True)
    st.caption("Lift > 1: elementele apar împreună mai des decât ar apărea întâmplător. "
               "Coșul = elementele puse în coș sau cumpărate în aceeași sesiune.")


# ---------------------------------------------------------------------------------------------
@st.cache_data
def load_replay() -> pd.DataFrame | None:
    """Evenimentele eșantionului agregate pe minut și tip (pentru replay)."""
    if not SAMPLE_EVENTS.exists():
        return None
    ev = pd.read_parquet(SAMPLE_EVENTS, columns=["event_ts", "event_type", "price"])
    ev["minute"] = ev["event_ts"].dt.floor("min")
    ev["revenue"] = ev["price"].where(ev["event_type"] == "purchase", 0.0)
    out = ev.groupby(["minute", "event_type"]).agg(n=("price", "size"), revenue=("revenue", "sum"))
    return out.reset_index()


def page_live():
    st.header("Live")
    st.info("Replay al eșantionului de o zi, ca demonstrație. Când e gata fluxul Kafka + Spark "
            "Structured Streaming, această pagină va citi agregatele lui în loc de eșantion.")
    rp = need(load_replay(), "python -m src.ingest.make_sample")

    minutes = sorted(rp["minute"].unique())
    if "live_pos" not in st.session_state:
        st.session_state["live_pos"] = len(minutes) // 3
    if st.session_state.get("live_play"):  # redare: înaintează 5 minute la fiecare reîncărcare
        st.session_state["live_pos"] = (st.session_state["live_pos"] + 5) % len(minutes)

    window = st.select_slider("Fereastră", [15, 30, 60, 120], value=60,
                              format_func=lambda m: f"ultimele {m} min")
    play = st.toggle("Redare automată", key="live_play")
    pos = st.slider("Minutul din zi", 0, len(minutes) - 1, key="live_pos")

    now = pd.Timestamp(minutes[pos])
    st.subheader(f"Ora {now:%H:%M} UTC")
    win = rp[(rp["minute"] > now - pd.Timedelta(minutes=window)) & (rp["minute"] <= now)]
    cum = rp[rp["minute"] <= now]
    c = st.columns(4)
    c[0].metric(f"Evenimente (ult. {window} min)", f"{win['n'].sum():,}")
    c[1].metric("Cumpărări", f"{win.loc[win['event_type'] == 'purchase', 'n'].sum():,}")
    c[2].metric("Venit în fereastră", fmt_money(win["revenue"].sum()))
    c[3].metric("Venit cumulat azi", fmt_money(cum["revenue"].sum()))

    win = win.assign(tip=win["event_type"].map(TYPE_RO))
    st.plotly_chart(px.line(win, x="minute", y="n", color="tip",
                            labels=dict(minute="", n="Evenimente / minut", tip="")),
                    use_container_width=True)
    if play:
        time.sleep(0.7)
        st.rerun()


# ---------------------------------------------------------------------------------------------
PAGES = {"KPI": page_kpi, "Funnel": page_funnel, "Segmente": page_segments,
         "Tipare": page_patterns, "Live": page_live}

st.sidebar.title("🛒 E-commerce Analytics")
choice = st.sidebar.radio("Pagina", list(PAGES))
st.sidebar.caption(f"Date: `{GOLD.relative_to(ROOT)}`" + (" (eșantion de o zi)" if "sample" in GOLD.parts else ""))
st.sidebar.caption("Dataset REES46, octombrie 2019")
PAGES[choice]()
