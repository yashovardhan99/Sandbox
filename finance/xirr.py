import polars as pl
from scipy import optimize

df = pl.DataFrame(
    {
        "scheme_code": [
            "ABC",
            "ABC",
            "ABC",
            "ABC",
            "ABC",
            "ABC",
            "ABC",
            "ABC",
            "ABC",
            "ABC",
            "ABC",
            "ABC",
            "ABC",
            "DEF",
            "DEF",
            "DEF",
            "DEF",
            "DEF",
            "DEF",
            "DEF",
            "DEF",
            "DEF",
            "DEF",
            "DEF",
            "DEF",
            "DEF",
        ],
        "date": [
            "2023-01-01",
            "2023-02-02",
            "2023-03-01",
            "2023-04-01",
            "2023-05-01",
            "2023-06-01",
            "2023-07-01",
            "2023-08-01",
            "2023-09-01",
            "2023-10-01",
            "2023-11-01",
            "2023-12-01",
            "2023-12-01",
            "2024-12-01",
            "2023-01-01",
            "2023-02-02",
            "2023-03-01",
            "2023-04-01",
            "2023-05-01",
            "2023-06-01",
            "2023-07-01",
            "2023-08-01",
            "2023-09-01",
            "2023-10-01",
            "2023-11-01",
            "2023-12-01",
        ],
        "amount": [
            1000,
            1500,
            1200,
            1300,
            1400,
            1600,
            1100,
            1700,
            1300,
            1800,
            1900,
            2000,
            -19000,
            -20000,
            1000,
            1500,
            1200,
            1300,
            1400,
            1600,
            1100,
            1700,
            1300,
            1800,
            1900,
            2000,
        ],
    }
)
df = df.with_columns(pl.col("date").cast(pl.Date()))


def xnpv(rate: float, df: pl.DataFrame) -> float:
    """
    Calculate the net present value (NPV) of a series of cash flows.

    The NPV is calculated using the formula:
    NPV = sum(CF / (1 + r)^(t))

    where:
    CF = cash flow at time t
    r = discount rate
    t = time period

    The time period is calculated as the difference between the cash flow date and the minimum date in the DataFrame,
    divided by the number of days in a year (365).

    This function is designed to be called by the xirr function to calculate the IRR.

    Args:
        rate (float): The discount rate.
        df (pl.DataFrame): A Polars DataFrame containing the cash flows. The DataFrame should have two columns:
                       'date' and 'amount'.

    Returns:
        float: The NPV of the cash flows.

    Example:
    >>> df = pl.DataFrame(
    ...     {"date": ["2023-01-01", "2023-02-01", "2023-03-01"], "amount": [1000, 1500, 2000]}
    ... )
    >>> df = df.with_columns(pl.col("date").cast(pl.Date()))
    >>> xnpv(0.1, df)
    4457.33029053319
    """
    return df.select(
        (
            pl.col("amount")
            / (pl.lit(1 + rate)).pow(
                (pl.col("date") - pl.col("date").min()) / pl.duration(days=365)
            )
        ).sum(),
    ).item()


def xirr(df: pl.Series | pl.DataFrame, guess=0.1) -> float:
    """
    Calculate the internal rate of return (IRR) for a series of cash flows.

    The IRR is calculated using the Newton-Raphson method to find the root of the NPV function.

    This function can also be used with the polars map_batches function to calculate the IRR for each group in a DataFrame. This allows,
    for instance, to calculate the IRR for different assets in a portfolio.

    Args:
        df (pl.Series | pl.DataFrame): A Polars Series or DataFrame containing the cash flows. The series should be a struct with
                   two fields: 'date' and 'amount' in this order. The DataFrame should have two columns:
                   'date' and 'amount'.
        guess (float): An initial guess for the IRR. Default is 0.1 (10%).

    Returns:
        float: The calculated IRR.

    Raises:
        ValueError: If the input is not a Polars Series or DataFrame.

    Example:
    >>> df = pl.DataFrame(
    ...     {"date": ["2023-01-01", "2023-02-01", "2023-03-01"], "amount": [1000, 1500, -3000]}
    ... )
    >>> df = df.with_columns(pl.col("date").cast(pl.Date()))
    >>> xirr(df)
    0.0.11369105297634706
    """
    if isinstance(df, pl.Series):
        df = df.struct.rename_fields(["date", "amount"]).struct.unnest()
    elif isinstance(df, pl.DataFrame):
        df = df.select(pl.col("date"), pl.col("amount"))
    else:
        raise ValueError("Input must be a Polars Series or DataFrame.")
    return optimize.newton(lambda r: xnpv(r, df), guess)

print(xirr(df))

df = df.group_by("scheme_code").agg(
    pl.struct(["date", "amount"]).map_batches(xirr, returns_scalar=True).alias("xirr"),
)
print(df)
