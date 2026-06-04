import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import random
from datetime import datetime

st.set_page_config(
    page_title="🎬 Content Recommendation Lakehouse",
    page_icon="🎬",
    layout="wide",
)

API_BASE = "http://localhost:8000"
ICEBERG_WAREHOUSE = "/tmp/iceberg-recommender"


@st.cache_resource
def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("RecommenderDashboard") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", ICEBERG_WAREHOUSE) \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def load_iceberg(spark, table):
    try:
        return spark.read.format("iceberg").load(f"local.{table}").toPandas()
    except Exception:
        return pd.DataFrame()


def load_catalog():
    try:
        return pd.read_csv("data/content_catalog.csv")
    except Exception:
        return pd.DataFrame()


st.title("🎬 Real-Time Content Recommendation Lakehouse")
st.markdown("**Apache Spark ALS · Apache Iceberg · Kafka · Medallion Architecture**")
st.divider()

spark = get_spark()

popularity_df  = load_iceberg(spark, "gold.content_popularity")
genre_df       = load_iceberg(spark, "gold.genre_trends")
profiles_df    = load_iceberg(spark, "silver.user_profiles")
interactions_df = load_iceberg(spark, "silver.user_content_interactions")
catalog_df     = load_catalog()

# ── KPIs ────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("👥 Total Users", f"{profiles_df['user_id'].nunique():,}" if not profiles_df.empty else "0")
with c2:
    st.metric("🎬 Content Items", f"{len(catalog_df):,}" if not catalog_df.empty else "500")
with c3:
    total_watches = popularity_df["total_watches"].sum() if not popularity_df.empty else 0
    st.metric("▶️ Total Watches", f"{int(total_watches):,}")
with c4:
    avg_rating = popularity_df["avg_rating"].mean() if not popularity_df.empty else 0
    st.metric("⭐ Avg Rating", f"{avg_rating:.2f}")

st.divider()

# ── Top Content & Genre Trends ──────────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("🏆 Top 10 Most Watched Content")
    if not popularity_df.empty:
        top10 = popularity_df.nsmallest(10, "popularity_rank")
        if not catalog_df.empty:
            top10 = top10.merge(catalog_df[["content_id", "content_name", "genre"]], on="content_id", how="left")
        fig = px.bar(
            top10,
            x="total_watches",
            y=top10.get("content_name", top10["content_id"].astype(str)),
            orientation="h",
            color="avg_rating",
            color_continuous_scale="Viridis",
            labels={"x": "Total Watches", "y": "Content"},
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Run the pipeline to see content popularity.")

with col_b:
    st.subheader("🎭 Genre Popularity Breakdown")
    if not genre_df.empty:
        fig2 = px.pie(
            genre_df,
            names="genre",
            values="total_watches",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        st.plotly_chart(fig2, use_container_width=True)
    elif not catalog_df.empty:
        genre_counts = catalog_df["genre"].value_counts().reset_index()
        genre_counts.columns = ["genre", "count"]
        fig2 = px.pie(genre_counts, names="genre", values="count", hole=0.4)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Genre data loading...")

# ── Genre Rating Heatmap ────────────────────────────────────────────────────
st.subheader("🌡️ Genre Avg Rating vs Watch Volume")
if not genre_df.empty:
    fig3 = px.scatter(
        genre_df,
        x="total_watches",
        y="avg_rating",
        size="unique_users",
        color="genre",
        text="genre",
        labels={"total_watches": "Total Watches", "avg_rating": "Avg Rating"},
    )
    fig3.update_traces(textposition="top center")
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("Waiting for genre data...")

# ── User Activity Leaderboard ───────────────────────────────────────────────
st.subheader("🥇 Most Active Users")
if not profiles_df.empty:
    top_users = profiles_df.nlargest(10, "total_watch_mins")[
        ["user_id", "total_watch_mins", "total_content_seen",
         "avg_rating_given", "activity_score"]
    ]
    st.dataframe(
        top_users.style.format({
            "total_watch_mins": "{:.0f} mins",
            "avg_rating_given": "{:.2f}",
            "activity_score": "{:.2f}",
        }),
        use_container_width=True,
    )
else:
    st.info("User profile data loading...")

st.divider()

# ── Live Recommendation Tester ───────────────────────────────────────────────
st.subheader("🤖 Live Recommendation Tester")
col_r1, col_r2 = st.columns([1, 2])

with col_r1:
    user_id_input = st.number_input("Enter User ID", min_value=1, max_value=1000, value=42)
    top_n = st.slider("Top N Recommendations", 5, 20, 10)
    get_recs_btn = st.button("Get Recommendations 🚀")

with col_r2:
    if get_recs_btn:
        try:
            resp = requests.get(
                f"{API_BASE}/recommend/{user_id_input}",
                params={"top_n": top_n},
                timeout=5,
            )
            data = resp.json()
            recs = data.get("recommendations", [])
            source = data.get("source", "unknown")

            st.success(f"Source: **{source}** | {len(recs)} recommendations")

            if recs and not catalog_df.empty:
                rec_df = pd.DataFrame(recs)
                rec_df = rec_df.merge(
                    catalog_df[["content_id", "content_name", "genre", "avg_rating"]],
                    on="content_id", how="left"
                )
                st.dataframe(rec_df.style.format({"score": "{:.4f}", "avg_rating": "{:.1f}"}),
                             use_container_width=True)
            else:
                st.json(recs)
        except Exception as e:
            st.error(f"API error: {e} — Is FastAPI running?")

st.divider()

# ── Iceberg Time Travel ──────────────────────────────────────────────────────
st.subheader("⏳ Iceberg Time-Travel Query")
with st.expander("Query historical interactions at a specific timestamp"):
    snap_ts = st.text_input("Timestamp (e.g. 2026-05-27 10:00:00)")
    if st.button("Run Time-Travel Query") and snap_ts:
        try:
            result = spark.sql(f"""
                SELECT user_id, content_id, implicit_score
                FROM local.silver.user_content_interactions
                TIMESTAMP AS OF '{snap_ts}'
                ORDER BY implicit_score DESC
                LIMIT 20
            """).toPandas()
            st.dataframe(result)
        except Exception as e:
            st.error(f"Error: {e}")

st.caption("🎬 Built with Apache Spark ALS · Apache Iceberg · Kafka · FastAPI · Streamlit")