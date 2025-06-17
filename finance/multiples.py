import concurrent.futures
import datetime
import tempfile
import pathlib
import polars as pl
import requests
import time
from typing import List


def download_file(url, directory, filename):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad responses
        if "text/plain" not in response.headers["Content-Type"]:
            print(f"Skipping non-text file: {filename}")
            return None
        path = pathlib.Path(directory, filename)
        with path.open("wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        return file.name
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None


start_date = datetime.date(2025, 1, 1)
end_date = datetime.date.today()

date_range = pl.date_range(
    start=start_date,
    end=end_date,
    interval="1d",
    closed="both",
    eager=True,
)
df = (
    date_range.alias("date")
    .to_frame()
    .group_by_dynamic("date", every="1w")
    .agg(
        [
            pl.col("date").first().alias("start_date"),
            pl.col("date").last().alias("end_date"),
        ]
    )
)

queries = []
BASE_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?tp=1&frmdt={FRMDT}&todt={TODT}"

print("Downloading files")
start_time = time.perf_counter()
with tempfile.TemporaryDirectory() as temp_dir:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for row in df.iter_rows(named=True):
            start_date = row["start_date"]
            end_date = row["end_date"]
            url = BASE_URL.format(
                FRMDT=start_date.strftime("%d-%b-%Y"),
                TODT=end_date.strftime("%d-%b-%Y"),
            )
            filename = (
                f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.txt"
            )
            futures.append(executor.submit(download_file, url, temp_dir, filename))

        for idx, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()

            if result:
                print(f"Downloaded ({idx + 1} of {len(futures)}): {result}")
            else:
                print(
                    f"Failed to download or process the file for {start_date} to {end_date}"
                )
    end_time = time.perf_counter()
    print(f"Time taken to download files: {end_time - start_time:.2f} seconds")

    # Check if any files were downloaded
    if not any(pathlib.Path(temp_dir).glob("*.txt")):
        print("No files were downloaded.")
        exit(1)

    # Process the downloaded files
    print("Saving data")
    start = time.perf_counter()
    df = pl.scan_csv(
        f"{temp_dir}/*.txt",
        separator=";",
        null_values=["N.A.", "-"],
        infer_schema=False,
    )
    df = df.drop_nulls(subset=["Scheme Code", "Date"]).select(
        pl.col("Scheme Code").cast(pl.String()).alias("scheme_code"),
        pl.col("Net Asset Value").cast(pl.Decimal(None, 4)).alias("nav"),
        pl.col("Date").str.to_date("%d-%b-%Y").alias("date"),
    )

    # Check if navdata.parquet exists
    if pathlib.Path("navdata.parquet").exists():
        # If it exists, read the file and check for availability
        df_navdata = pl.read_parquet("navdata.parquet")
        df = df_navdata.update(df, on=["scheme_code", "date"], how="full")
        df.write_parquet("navdata.parquet")
    else:
        df.sink_parquet("navdata.parquet")
    end = time.perf_counter()
    print(f"Time taken to read and process files: {end - start:.2f} seconds")
    print(df)
