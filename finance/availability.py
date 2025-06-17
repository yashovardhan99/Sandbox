import datetime
import pathlib
from typing import List

import polars as pl


def check_availability(
    schemes: List[str], start_date: datetime.date, end_date: datetime.date
):
    """
    Check the availability of schemes for a given date range.
    This function should return a list of date ranges for which the schemes are not available.
    """

    date_range = pl.date_range(
        start=start_date,
        end=end_date,
        interval="1d",
        closed="both",
        eager=True,
    )

    # Filter the date range to only include business days
    date_range = date_range.filter(date_range.dt.is_business_day()).to_frame("date")
    print(date_range)

    # Check if availability file exists
    if pathlib.Path("navavailability.parquet").exists():
        # If it exists, read the file and check for availability
        df_availability = pl.read_parquet("navavailability.parquet")
        # Check if the date range is available in the file
        found = df_availability.join_where(
            date_range,
            pl.col("date")
            >= pl.col("start_date") & pl.col("date")
            <= pl.col("end_date"),
        )
        # Filter the date range to only include dates that are not in the availability file
        date_range = date_range.join(found, on="date", how="anti")

    # Check if NAV data file exists
    if pathlib.Path("navdata.parquet").exists():
        # If it exists, read the file and check for availability
        df_navdata = pl.read_parquet("navdata.parquet").filter(
            pl.col("scheme_code").is_in(schemes)
        )

        df_schemes = pl.DataFrame({"scheme_code": schemes})
        cross_date_range = df_schemes.join(date_range, how="cross")
        # Filter the date range to only include dates that are not in the NAV data file
        cross_date_range = cross_date_range.join(
            df_navdata, on=["date", "scheme_code"], how="anti"
        )
        date_range = cross_date_range.select(pl.col("date").unique())
    return date_range.sort("date")


start_date = datetime.date(2022, 1, 1)
end_date = datetime.date.today()

date_range = check_availability(["143341", "143340"], start_date, end_date)
df = date_range.group_by_dynamic("date", every="1w").agg(
    [
        pl.col("date").first().alias("start_date"),
        pl.col("date").last().alias("end_date"),
        pl.len().alias("count"),
    ]
)
print(df)
print(df.mean()["count"][0])
