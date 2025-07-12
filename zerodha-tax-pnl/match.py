import polars as pl
from decimal import Decimal
from dotenv import load_dotenv
import os

load_dotenv()

FILE_PATH = os.environ.get("FILE_PATH")
PNL_PATH = os.environ.get("PNL_PATH")

if not FILE_PATH or not PNL_PATH:
    raise ValueError("FILE_PATH and PNL_PATH environment variables must be set.")

df = pl.read_csv(FILE_PATH)
# print(df)
# print(df.columns)
df = df.filter(pl.col("Trading Symbol").str.len_chars() > 0)

df = df.with_columns(
    intraday_charges=pl.sum_horizontal(
        pl.col("Brokerage").str.to_decimal(),
        pl.col("Exchange Txn Charges").str.to_decimal(),
        pl.col("Sebi").str.to_decimal(),
        pl.col("Stamp Duty").str.to_decimal(),
        pl.col("STT").str.to_decimal(),
        pl.col("IGST").str.to_decimal(),
        pl.col("CGST").str.to_decimal(),
        pl.col("SGST").str.to_decimal(),
    ),
    cg_charges=pl.sum_horizontal(
        pl.col("Brokerage").str.to_decimal(),
        pl.col("Exchange Txn Charges").str.to_decimal(),
        pl.col("Sebi").str.to_decimal(),
        pl.col("Stamp Duty").str.to_decimal(),
        pl.col("IGST").str.to_decimal(),
        pl.col("CGST").str.to_decimal(),
        pl.col("SGST").str.to_decimal(),
    ),
)

df = df.select(
    pl.col("Trading Symbol"),
    pl.col("Order Date").str.to_datetime().dt.date(),
    pl.col("Quantity").cast(pl.Int64),
    pl.col("Price").str.to_decimal(),
    pl.col("Brokerage").str.to_decimal(),
    pl.col("STT").str.to_decimal(),
    pl.col("intraday_charges"),
    pl.col("cg_charges"),
)

df_buys = df.filter(pl.col("Quantity") > 0)
df_sells = df.filter(pl.col("Quantity") < 0).with_columns(
    pl.col("Quantity").abs().alias("Quantity"),
)

DECIMAL_TYPE = pl.Decimal(20, 8)  # Adjust precision/scale as needed

df_buys = df_buys.with_columns(
    (
        pl.col("intraday_charges").cast(DECIMAL_TYPE)
        / pl.col("Quantity").cast(DECIMAL_TYPE)
    )
    .alias("per_unit_intraday_charge")
    .cast(DECIMAL_TYPE),
    (pl.col("cg_charges").cast(DECIMAL_TYPE) / pl.col("Quantity").cast(DECIMAL_TYPE))
    .alias("per_unit_cg_charge")
    .cast(DECIMAL_TYPE),
)

df_sells = df_sells.with_columns(
    (
        pl.col("intraday_charges").cast(DECIMAL_TYPE)
        / pl.col("Quantity").cast(DECIMAL_TYPE)
    )
    .alias("per_unit_intraday_charge")
    .cast(DECIMAL_TYPE),
    (pl.col("cg_charges").cast(DECIMAL_TYPE) / pl.col("Quantity").cast(DECIMAL_TYPE))
    .alias("per_unit_cg_charge")
    .cast(DECIMAL_TYPE),
)

df_pnl = pl.read_excel(
    PNL_PATH,
    sheet_name="Tradewise Exits from 2024-04-01",
    drop_empty_cols=True,
    drop_empty_rows=True,
)

col0 = df_pnl.columns[0]
df_pnl = df_pnl.with_row_index()

starting_idx_eq = (
    df_pnl.filter(pl.col(col0) == pl.lit("Equity")).head(1).select("index").item()
)
ending_idx_eq = (
    df_pnl.filter(pl.col(col0) == pl.lit("Equity - Buyback"))
    .head(1)
    .select("index")
    .item()
)
df_pnl = df_pnl.filter(
    pl.col("index").is_between(starting_idx_eq + 1, ending_idx_eq - 1)
).drop("index")

# print(df_pnl.head(1))

df_pnl.columns = list(df_pnl.head(1).to_dicts()[0].values())

df_pnl = df_pnl.filter(pl.col("Symbol") != pl.lit("Symbol"))

df_pnl = df_pnl.select(
    "Symbol",
    pl.col("Entry Date").str.to_date(),
    pl.col("Exit Date").str.to_date(),
    pl.col("Quantity").str.to_integer(),
    "Buy Value",
    "Sell Value",
    "Profit",
    "Turnover",
    (pl.col("Buy Value") / pl.col("Quantity")).alias("Buy Price"),
    (pl.col("Sell Value") / pl.col("Quantity")).alias("Sell Price"),
)

# print(df_pnl)


def allocate_buys_to_sells(df_buys, df_pnl, price_tolerance=0.01):
    df_buys = df_buys.sort(["Trading Symbol", "Order Date"])
    df_pnl = df_pnl.sort(["Symbol", "Entry Date"])

    buys = df_buys.to_dicts()
    sells = df_pnl.to_dicts()
    allocations = []

    symbols = set([b["Trading Symbol"] for b in buys]) | set(
        [s["Symbol"] for s in sells]
    )
    for symbol in symbols:
        buy_rows = [b.copy() for b in buys if b["Trading Symbol"] == symbol]
        sell_rows = [s for s in sells if s["Symbol"] == symbol]

        for sell in sell_rows:
            sell_qty_left = abs(sell["Quantity"])
            is_intraday = sell["Entry Date"] == sell["Exit Date"]

            # 1. Intraday: match buys from same date, ignore price
            if is_intraday:
                for buy in buy_rows:
                    if buy["Quantity"] <= 0:
                        continue
                    if buy["Order Date"] == sell["Entry Date"]:
                        qty_to_allocate = min(buy["Quantity"], sell_qty_left)
                        allocations.append(
                            {
                                "Symbol": symbol,
                                "Sell Entry Date": sell["Entry Date"],
                                "Sell Exit Date": sell["Exit Date"],
                                "Sell Quantity": sell["Quantity"],
                                "Buy Order Date": buy["Order Date"],
                                "Buy Price": buy["Price"],
                                "Sell Buy Price": sell["Buy Price"],
                                "Sell Price": sell["Sell Price"],
                                "Allocated Quantity": qty_to_allocate,
                                # For intraday
                                "Buy Charge": buy["per_unit_intraday_charge"]
                                * Decimal(qty_to_allocate),
                                "Charge Type": "intraday",
                            }
                        )
                        buy["Quantity"] -= qty_to_allocate
                        sell_qty_left -= qty_to_allocate
                        if sell_qty_left == 0:
                            break

            # 2. Delivery: match on date and price
            if not is_intraday and sell_qty_left > 0:
                for buy in buy_rows:
                    if buy["Quantity"] <= 0:
                        continue
                    if (
                        buy["Order Date"] == sell["Entry Date"]
                        and abs(float(buy["Price"]) - float(sell["Buy Price"]))
                        <= price_tolerance
                    ):
                        qty_to_allocate = min(buy["Quantity"], sell_qty_left)
                        allocations.append(
                            {
                                "Symbol": symbol,
                                "Sell Entry Date": sell["Entry Date"],
                                "Sell Exit Date": sell["Exit Date"],
                                "Sell Quantity": sell["Quantity"],
                                "Buy Order Date": buy["Order Date"],
                                "Buy Price": buy["Price"],
                                "Sell Buy Price": sell["Buy Price"],
                                "Sell Price": sell["Sell Price"],
                                "Allocated Quantity": qty_to_allocate,
                                # For delivery
                                "Buy Charge": buy["per_unit_cg_charge"]
                                * Decimal(qty_to_allocate),
                                "Charge Type": "cg",
                            }
                        )
                        buy["Quantity"] -= qty_to_allocate
                        sell_qty_left -= qty_to_allocate
                        if sell_qty_left == 0:
                            break

            # 3. If still left, treat as IPO/unmatched (charge = 0)
            if sell_qty_left > 0:
                allocations.append(
                    {
                        "Symbol": symbol,
                        "Sell Entry Date": sell["Entry Date"],
                        "Sell Exit Date": sell["Exit Date"],
                        "Sell Quantity": sell["Quantity"],
                        "Buy Order Date": sell["Entry Date"],
                        "Buy Price": sell["Buy Price"],
                        "Sell Buy Price": sell["Buy Price"],
                        "Sell Price": sell["Sell Price"],
                        "Allocated Quantity": sell_qty_left,
                        "Buy Charge": Decimal(0),
                        "Charge Type": "intraday" if is_intraday else "cg",
                    }
                )
    return pl.DataFrame(allocations)


# Example usage:
df_alloc = allocate_buys_to_sells(df_buys, df_pnl)
# print(df_alloc)


def add_sell_charges_to_allocations(df_alloc, df_sells, price_tolerance=0.05):
    df_sells = df_sells.with_columns(
        pl.col("Order Date").cast(pl.Utf8), pl.col("Price").cast(pl.Float64)
    )
    sell_rows = df_sells.to_dicts()
    for row in sell_rows:
        row["_qty_left"] = row["Quantity"]

    allocs = df_alloc.to_dicts()
    results = []
    for alloc in allocs:
        symbol = alloc["Symbol"]
        sell_date = str(alloc["Sell Exit Date"])
        alloc_qty = alloc["Allocated Quantity"]
        sell_price = float(alloc["Sell Price"])
        is_intraday = alloc["Charge Type"] == "intraday"
        matched = False

        qty_left_to_allocate = alloc_qty
        for sell in sell_rows:
            if (
                sell["Trading Symbol"] == symbol
                and str(sell["Order Date"]) == sell_date
                and abs(float(sell["Price"]) - sell_price) <= price_tolerance
                and sell["_qty_left"] > 0
            ):
                qty_from_this_sell = min(sell["_qty_left"], qty_left_to_allocate)
                if is_intraday:
                    alloc.setdefault("Sell Charge", Decimal(0))
                    alloc["Sell Charge"] += sell["per_unit_intraday_charge"] * Decimal(
                        qty_from_this_sell
                    )
                else:
                    alloc.setdefault("Sell Charge", Decimal(0))
                    alloc["Sell Charge"] += sell["per_unit_cg_charge"] * Decimal(
                        qty_from_this_sell
                    )
                sell["_qty_left"] -= qty_from_this_sell
                qty_left_to_allocate -= qty_from_this_sell
                if qty_left_to_allocate == 0:
                    matched = True
                    break
        if not matched:
            print(f"WARN: No match found for allocation: {alloc}")
            alloc["Sell Charge"] = Decimal(0)
        results.append(alloc)
    return pl.DataFrame(results)


# Usage after your allocation:
df_alloc = allocate_buys_to_sells(df_buys, df_pnl)
df_alloc = add_sell_charges_to_allocations(df_alloc, df_sells).sort(
    ["Sell Exit Date", "Buy Order Date", "Symbol"]
)

with pl.Config(tbl_cols=20, tbl_rows=20):
    print(
        df_alloc.group_by("Symbol", "Sell Entry Date", "Sell Exit Date")
        .agg(
            pl.sum("Buy Charge").alias("Total Buy Charge"),
            pl.sum("Sell Charge").alias("Total Sell Charge"),
            pl.sum("Allocated Quantity").alias("Total Allocated Quantity"),
            (pl.col("Buy Price") * pl.col("Allocated Quantity"))
            .sum()
            .alias("Total Buy Value"),
            (pl.col("Sell Price") * pl.col("Allocated Quantity"))
            .sum()
            .alias("Total Sell Value"),
            (pl.sum("Buy Charge") + pl.sum("Sell Charge")).alias("Total Charges"),
        )
        .sort("Sell Exit Date", "Sell Entry Date", "Symbol")
    )
