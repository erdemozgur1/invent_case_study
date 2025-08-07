import argparse
import pandas as pd
import logging
from utils import (
    load_csv,
    rename_columns,
    calculate_moving_average,
    calculate_lag,
    group_by_sum_agg,
    merge_dataframes,
    calculate_wmape,
    write_to_csv,
)

# Configure module-level logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run(args: argparse.Namespace) -> None:
    logger.info("Starting sales feature pipeline...")

    # Load datasets
    product_df = load_csv("product.csv")
    store_df = load_csv("store.csv")
    brand_df = load_csv("brand.csv")
    sales_df = load_csv("sales.csv", parse_dates=["date"])

    # Filter date range
    min_date = pd.to_datetime(args.min_date)
    max_date = pd.to_datetime(args.max_date)
    sales_df = sales_df[(sales_df["date"] >= min_date) & (sales_df["date"] <= max_date)]
    logger.info(
        f"Filtered sales data to date range {min_date.date()} - {max_date.date()} ({len(sales_df)} rows)"
    )

    # Prepare and clean sales data
    sales_df = rename_columns(
        sales_df,
        {"store": "store_id", "product": "product_id", "quantity": "sales_product"},
        "sales_df",
    )
    sales_df = sales_df.sort_values(by=["product_id", "store_id", "date"])
    sales_df = group_by_sum_agg(
        sales_df, ["product_id", "store_id", "date"], "sales_product"
    )

    # Product-level features
    sales_df = calculate_moving_average(
        sales_df, "MA7_P", ["product_id", "store_id"], "sales_product", 7
    )
    sales_df = calculate_lag(
        sales_df, "LAG7_P", ["product_id", "store_id"], "sales_product", 7
    )

    # Brand ID assignment
    product_df = rename_columns(
        product_df, {"id": "product_id", "brand": "brand_name"}, "product_df"
    )
    brand_df = rename_columns(
        brand_df, {"id": "brand_id", "name": "brand_name"}, "brand_df"
    )

    sales_df = merge_dataframes(
        sales_df,
        product_df[["product_id", "brand_name"]],
        ["product_id"],
        "sales_df",
        "product_df",
    )
    sales_df = merge_dataframes(
        sales_df,
        brand_df[["brand_id", "brand_name"]],
        ["brand_name"],
        "sales_df",
        "brand_df",
    )

    # Brand-level features
    brand_sales = group_by_sum_agg(
        sales_df, ["brand_id", "store_id", "date"], "sales_product"
    )
    brand_sales = rename_columns(
        brand_sales, {"sales_product": "sales_brand"}, "brand_sales"
    )
    brand_sales = brand_sales.sort_values(by=["brand_id", "store_id", "date"])
    brand_sales = calculate_moving_average(
        brand_sales, "MA7_B", ["brand_id", "store_id"], "sales_brand", 7
    )
    brand_sales = calculate_lag(
        brand_sales, "LAG7_B", ["brand_id", "store_id"], "sales_brand", 7
    )

    sales_df = merge_dataframes(
        sales_df,
        brand_sales,
        ["brand_id", "store_id", "date"],
        "sales_df",
        "brand_sales",
    )

    # Store-level features
    store_sales = group_by_sum_agg(sales_df, ["store_id", "date"], "sales_product")
    store_sales = rename_columns(
        store_sales, {"sales_product": "sales_store"}, "store_sales"
    )
    store_sales = store_sales.sort_values(by=["store_id", "date"])
    store_sales = calculate_moving_average(
        store_sales, "MA7_S", ["store_id"], "sales_store", 7
    )
    store_sales = calculate_lag(store_sales, "LAG7_S", ["store_id"], "sales_store", 7)

    sales_df = merge_dataframes(
        sales_df, store_sales, ["store_id", "date"], "sales_df", "store_sales"
    )

    # Output 1: features.csv
    sorted_df = sales_df.sort_values(by=["product_id", "brand_id", "store_id", "date"])
    write_to_csv(sorted_df, "features.csv")

    # Output 2: mapes.csv
    wmape_df = calculate_wmape(sorted_df)
    top_wmape_df = wmape_df.sort_values(by="WMAPE", ascending=False).head(args.top)
    write_to_csv(top_wmape_df, "mapes.csv")

    logger.info("Task completed successfully.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Invent AI sales case args")
    args_info = [
        {
            "name": "--min-date",
            "help": "Start of the date range (YYYY-MM-DD)",
            "type": str,
            "default": "2021-01-08",
        },
        {
            "name": "--max-date",
            "help": "End of the date range (YYYY-MM-DD)",
            "type": str,
            "default": "2021-05-30",
        },
        {
            "name": "--top",
            "help": "Number of rows in the WMAPE output",
            "type": int,
            "default": 5,
        },
    ]

    for arg in args_info:
        parser.add_argument(
            arg["name"], type=arg["type"], default=arg["default"], help=arg["help"]
        )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args)
