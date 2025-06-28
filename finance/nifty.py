# Compare the results of a regular SIP between Nifty 50 and Nifty 50 Equal Weight

import polars as pl
from datetime import date

from xirr import xirr
import sys
import pathlib

PATH = "data/indices/*.csv"


def build_sip(
    df_price: pl.DataFrame,
    inv_amount: float,
    step_up: float,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """
    Build a SIP DataFrame with investment amounts and dates.
    """

    # print(df_price)

    df_dates = pl.date_range(
        start=start_date, end=end_date, interval="1mo", eager=True
    ).to_frame(name="Date")
    # print(df_dates)

    df_sip = (
        df_dates.join_asof(
            df_price.sort("Date"),
            on="Date",
            strategy="forward",
            suffix="_price",
        )
        .with_columns(
            # years=((pl.col("Date") - start_date) / pl.duration(days=365)).floor(),
            inv_amount=(
                pl.lit(inv_amount)
                * (1 + pl.lit(step_up)).pow(
                    ((pl.col("Date") - start_date) / pl.duration(days=365)).floor()
                )
            ).cast(pl.Decimal(None, 2)),
            nav=pl.col("Close").cast(pl.Decimal(None, 4)),
        )
        .with_columns(
            units=(pl.col("inv_amount") / pl.col("nav")).cast(pl.Decimal(None, 4)),
        )
        .select(
            pl.col("Index Name"),
            pl.col("Date"),
            pl.col("inv_amount").alias("Investment Amount"),
            pl.col("nav").alias("NAV"),
            pl.col("units"),
        )
    )

    # print(df_sip)
    return df_sip


def get_rolling_returns(
    df: pl.DataFrame,
    start_date: date,
    end_date: date,
    period: str = "1y",
    group_by: str = "Index Name",
) -> pl.DataFrame:
    """
    Calculate rolling returns for a given period and group by specified column.
    """

    pl.duration()
    return (
        df.set_sorted("Date")
        .group_by_dynamic(
            "Date",
            every="1d",
            period=period,
            group_by=group_by,
            include_boundaries=True,
            label="datapoint",
        )
        .agg(
            (
                (pl.col("Close").last() / pl.col("Close").first()).pow(
                    (
                        1
                        / (
                            (pl.col("Date").last() - pl.col("Date").first())
                            / pl.duration(hours=8766)  # 1 year in hours
                        )
                    )
                )
                - 1
            ).alias("Return"),
        )
        .filter(pl.col("_upper_boundary") <= end_date)
        .filter(pl.col("Date") == pl.col("_lower_boundary"))
        .group_by("Index Name")
        .agg(
            pl.col("Return").min().alias(f"{period} Min Return"),
            pl.col("Return").quantile(0.25).alias(f"{period} 25% Quantile"),
            pl.col("Return").median().alias(f"{period} Median Return"),
            pl.col("Return").quantile(0.75).alias(f"{period} 75% Quantile"),
            pl.col("Return").max().alias(f"{period} Max Return"),
        )
        .sort("Index Name")
    )


if __name__ == "__main__":
    inv_amount = 10_000 if len(sys.argv) < 2 else float(sys.argv[1])
    step_up = 0.10 if len(sys.argv) < 3 else float(sys.argv[2])

    start_date: date | None = None
    end_date: date | None = None

    sip_data: list[pl.DataFrame] = []
    raw_data: list[pl.DataFrame] = []

    for path in pathlib.Path().glob(PATH):
        print(f"Processing {path}")

        df = pl.read_csv(path, null_values="-").with_columns(
            Date=pl.col("Date").str.to_date("%d %b %Y")
        )

        raw_data.append(df)

        start_date = (
            df.select(pl.col("Date").min()).item() if start_date is None else start_date
        )
        end_date = (
            df.select(pl.col("Date").max()).item() if end_date is None else end_date
        )

        sip_data.append(
            build_sip(df, inv_amount, step_up, start_date, end_date).with_columns(
                pl.col("Index Name"),
            )
        )

    if start_date is None or end_date is None:
        raise ValueError("No data found in the specified path.")

    df_sip = pl.concat(sip_data).sort("Date")
    df_sip_total = (
        df_sip.group_by("Index Name")
        .agg(
            pl.col("Investment Amount").sum().alias("Total Investment"),
            pl.col("units").sum().alias("Total Units"),
            (
                (pl.col("NAV").last() / pl.col("NAV").first())
                .cast(pl.Float64())
                .pow(
                    1
                    / (
                        (pl.col("Date").last() - pl.col("Date").first())
                        / pl.duration(days=365)
                    )
                )
                - 1
            ).alias("CAGR"),
            pl.col("Date").first().alias("Start Date"),
            pl.col("Date").last().alias("End Date"),
            (pl.col("NAV").last() * pl.col("units").sum())
            .cast(pl.Decimal(None, 4))
            .alias("Final Value"),
        )
        .with_columns(gains=pl.col("Final Value") - pl.col("Total Investment"))
    )

    # print(df_sip_total)

    df_returns = (
        pl.concat(
            [
                df_sip.select(
                    pl.col("Index Name"),
                    pl.col("Date").alias("date"),
                    pl.col("Investment Amount").neg().alias("amount"),
                ),
                df_sip_total.select(
                    pl.col("Index Name"),
                    pl.lit(df_sip_total["End Date"]).alias("date"),
                    pl.col("Final Value").alias("amount"),
                ),
            ]
        )
        .group_by("Index Name")
        .agg(
            pl.struct(["date", "amount"])
            .map_batches(
                xirr,
                returns_scalar=True,
            )
            .alias("xirr")
        )
    )

    # print(df_returns)

    df_sip_total = (
        df_sip_total.join(df_returns, on="Index Name", how="left")
        .select(
            pl.col("Index Name"),
            pl.col("Start Date"),
            pl.col("End Date"),
            pl.col("Total Investment"),
            pl.col("Total Units"),
            pl.col("Final Value"),
            pl.col("gains").alias("Absolute Gains"),
            pl.col("CAGR"),
            pl.col("xirr").alias("XIRR"),
        )
        .sort("XIRR", descending=True)
    )

    df_rollings = pl.concat(raw_data).sort("Date")

    df_rolling_1y = get_rolling_returns(
        df_rollings, start_date, end_date, period="1y", group_by="Index Name"
    )

    df_rolling_2y = get_rolling_returns(
        df_rollings, start_date, end_date, period="2y", group_by="Index Name"
    )

    df_rolling_5y = get_rolling_returns(
        df_rollings, start_date, end_date, period="5y", group_by="Index Name"
    )

    print("SIP Summary:")
    with pl.Config(
        tbl_cell_numeric_alignment="RIGHT",
        thousands_separator=True,
        float_precision=4,
        tbl_cols=-1,  # Show all columns
        tbl_rows=100,  # Show up to 100 rows
        tbl_hide_column_data_types=True,  # Hide data types in the output
        tbl_hide_dataframe_shape=True,  # Hide the shape of the DataFrame
    ):
        print(df_sip_total)

        print("\n1 Year Rolling Returns:")
        print(df_rolling_1y)

        print("\n2 Year Rolling Returns:")
        print(df_rolling_2y)

        print("\n5 Year Rolling Returns:")
        print(df_rolling_5y)
