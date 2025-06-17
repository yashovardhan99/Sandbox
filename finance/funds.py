import polars as pl

df = pl.scan_csv(
    "https://www.amfiindia.com/spages/NAVAll.txt?t=15012022025700",
    separator=";",
    null_values=["N.A.", "-"],
    infer_schema=False,
)
df = df.drop_nulls(subset=["Scheme Code"])
df = df.with_columns(
    group_header=pl.when(
        pl.col("Date").is_null()
        & pl.col("Scheme Code").str.to_lowercase().str.contains("scheme")
    )
    .then(pl.col("Scheme Code"))
    .forward_fill(),
    fund_house=pl.when(
        pl.col("Date").is_null()
        & pl.col("Scheme Code").str.to_lowercase().str.contains("fund")
    )
    .then(pl.col("Scheme Code"))
    .forward_fill(),
)

df = df.filter(pl.col("Date").is_not_null())
df = df.select(
    pl.col("Scheme Code").alias("schme_code"),
    pl.col("Net Asset Value").cast(pl.Decimal(None, 4)).alias("nav"),
    pl.col("group_header").alias("scheme_type"),
    pl.col("fund_house").alias("fund_house"),
    pl.col("Scheme Name").alias("scheme_name"),
    pl.col("Date").str.to_date("%d-%b-%Y").alias("date"),
)
# Write Metadata
df_meta = df.select(
    pl.col("schme_code"),
    pl.col("scheme_type"),
    pl.col("fund_house"),
    pl.col("scheme_name"),
)
df_meta.collect().write_parquet("metadata.parquet")

# Write Data
df_data = df.select(
    pl.col("schme_code"),
    pl.col("nav"),
    pl.col("date"),
)
df_data.collect().write_parquet("data.parquet")