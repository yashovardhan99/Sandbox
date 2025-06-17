import polars as pl
import datetime

start_date = datetime.date(2022, 1, 1)
end_date = datetime.date.today()
date_range = pl.date_range(
    start=start_date,
    end=end_date,
    interval="1d",
    closed="both",
    eager=True,
)

df2 = pl.Series("date", date_range).to_frame().filter(pl.col("date").dt.is_business_day())
print(df2)
df = pl.DataFrame(
    {
        "date": [
            datetime.date(2024, 1, 1),
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
            datetime.date(2024, 1, 4),
            datetime.date(2024, 1, 5),
            datetime.date(2024, 1, 6),
            datetime.date(2024, 1, 7),
            datetime.date(2024, 1, 8),
            datetime.date(2024, 1, 9),
            datetime.date(2024, 1, 10),
            datetime.date(2024, 12, 31),
            datetime.date(2025, 1, 1),
        ]
    }
)

df = df2.join(df, on="date", how="anti")
df = df.group_by_dynamic("date", every="1mo").agg(
    [
        pl.col("date").first().alias("start_date"),
        pl.col("date").last().alias("end_date"),
    ]
)

print(df)