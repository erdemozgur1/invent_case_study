import logging
from pathlib import Path
import pandas as pd
from typing import Optional, List, Dict
import os
from dotenv import load_dotenv

# Set up basic logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment variables from .env
load_dotenv()


INPUT_DATA_DIR = Path(os.getenv("INPUT_DATA_DIR", "input_data/data")).resolve()
OUTPUT_DATA_DIR = Path(os.getenv("OUTPUT_DATA_DIR", "output_data/data")).resolve()


def load_csv(file_name: str, parse_dates: Optional[List[str]] = None) -> pd.DataFrame:
    """Load a CSV file from the data directory."""
    file_path = INPUT_DATA_DIR / file_name
    logger.info(f"Attempting to load file: {file_path}")

    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        df = pd.read_csv(file_path, parse_dates=parse_dates)
        logger.info(f"Loaded {file_path} with {len(df)} rows.")
        return df
    except pd.errors.ParserError as e:
        logger.exception(f"Parser error while reading {file_path}")
        raise


def write_to_csv(df: pd.DataFrame, file_name: str, index: bool = False) -> None:
    """
    Write DataFrame to CSV in the output directory, creating the path if it doesn't exist.
    """
    file_path = OUTPUT_DATA_DIR / file_name

    try:
        # Create the directory if it doesn't exist
        OUTPUT_DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Write the DataFrame to CSV
        df.to_csv(file_path, index=index)
        logger.info(f"Data written to: {file_path}")

    except Exception as e:
        logger.exception(f"Failed to write CSV to {file_path}")
        raise


def rename_columns(
    df: pd.DataFrame, column_mapping: Dict[str, str], df_name: str
) -> pd.DataFrame:
    """Rename DataFrame columns based on a mapping."""
    logger.info(f"Renaming columns for {df_name}: {column_mapping}")
    try:
        df = df.rename(columns=column_mapping)
        return df
    except Exception as e:
        logger.exception("Failed to rename columns.")
        raise


def calculate_moving_average(
    df: pd.DataFrame,
    column_name: str,
    group_by_columns: List[str],
    calculation_column: str,
    day: int,
) -> pd.DataFrame:
    """Calculate moving average for a column."""
    logger.info(
        f"Calculating {day}-day moving average for {calculation_column} grouped by {group_by_columns}"
    )
    try:
        df[column_name] = df.groupby(group_by_columns)[calculation_column].transform(
            lambda x: x.shift(1).rolling(window=day, min_periods=1).mean()
        )
        return df
    except KeyError as e:
        logger.exception("Missing column during moving average calculation.")
        raise


def calculate_lag(
    df: pd.DataFrame,
    column_name: str,
    group_by_columns: List[str],
    calculation_column: str,
    day: int,
) -> pd.DataFrame:
    """Calculate lag for a column."""
    logger.info(
        f"Calculating lag of {day} for {calculation_column} grouped by {group_by_columns}"
    )
    try:
        df[column_name] = df.groupby(group_by_columns)[calculation_column].shift(day)
        return df
    except KeyError as e:
        logger.exception("Missing column during lag calculation.")
        raise


def group_by_sum_agg(
    df: pd.DataFrame, group_by_columns: List[str], calculation_column: str
) -> pd.DataFrame:
    """Aggregate DataFrame by sum based on group_by_columns."""
    logger.info(f"Aggregating by {group_by_columns} summing {calculation_column}")
    try:
        return df.groupby(group_by_columns, as_index=False)[calculation_column].sum()
    except Exception as e:
        logger.exception("Aggregation by sum failed.")
        raise


def merge_dataframes(
    left_dataframe: pd.DataFrame,
    right_dataframe: pd.DataFrame,
    on_columns: List[str],
    left_df_name: str,
    right_df_name: str,
    how: str = "left",
) -> pd.DataFrame:
    """Merge two DataFrames on specified columns."""
    logger.info(
        f"Merging {left_df_name} and {right_df_name} on {on_columns} with method '{how}'"
    )
    try:
        return left_dataframe.merge(right_dataframe, on=on_columns, how=how)
    except KeyError as e:
        logger.exception("Merge failed due to missing key columns.")
        raise
    except Exception as e:
        logger.exception("Merge operation failed.")
        raise


def calculate_wmape(
    df: pd.DataFrame,
    forecast_column: str = "MA7_P",
    actual_column: str = "sales_product",
    group_by_columns: List[str] = ["product_id", "store_id", "brand_id"],
) -> pd.DataFrame:
    """Calculate WMAPE per group."""
    logger.info(
        f"Calculating WMAPE using forecast: {forecast_column}, actual: {actual_column}"
    )
    try:
        valid_df = df.dropna(subset=[actual_column, forecast_column]).copy()
        valid_df["abs_error"] = (
            valid_df[actual_column] - valid_df[forecast_column]
        ).abs()

        wmape_df = (
            valid_df.groupby(group_by_columns)
            .agg(abs_error_sum=("abs_error", "sum"), actual_sum=(actual_column, "sum"))
            .reset_index()
        )

        wmape_df["WMAPE"] = wmape_df.apply(
            lambda row: (
                (row["abs_error_sum"] / row["actual_sum"])
                if row["actual_sum"] != 0
                else None
            ),
            axis=1,
        )

        logger.info(f"WMAPE calculation complete with {len(wmape_df)} rows.")
        return wmape_df[group_by_columns + ["WMAPE"]]
    except KeyError as e:
        logger.exception("Missing column during WMAPE calculation.")
        raise
    except Exception as e:
        logger.exception("WMAPE calculation failed.")
        raise
