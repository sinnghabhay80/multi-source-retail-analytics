"""
Retail360 Synthetic Returns Data Generator
Generates realistic returns data with full control via CLI + config. - links to real sales order_ids
"""
from pydoc import replace

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from utils.logger import get_logger
from utils.config import load_config, get_project_root

logger = get_logger(__name__)

class ReturnsDataGenerator:
    """Generates realistic returns linked to existing sales."""

    SCHEMA = {
        "return_id": "string",
        "order_id": "string",
        "customer_id": "string",
        "product_id": "string",
        "return_date": "datetime64[ns]",
        "return_reason": "string",
        "return_value": "float64"
    }

    SALES_DIR = Path("data/raw/sales")
    OUTPUT_DIR = Path("data/raw/returns")
    CONFIG_PATH = "configs/data_generation/returns.yaml"

    def __init__(self, config=None) -> None:
        if config is None:
            config = dict()
        self.config = {**config}  # in-case user provides new configs
        default_config = load_config(self.CONFIG_PATH)
        self.config.update(default_config)  # We'll use default_config(what we have in configs)
        self._validate_config()
        np.random.seed(self.config["seed"])
        logger.info(f"Random seed set to: {self.config['seed']}")

    def _validate_config(self):
        """Validate configuration values."""
        cfg = self.config
        if cfg["max_days_after_purchase"] <= 0:
            raise ValueError("max_days_after_purchase must be <= 30")
        if cfg["output_format"] != "csv":
            raise ValueError("output_format must be 'csv'")

    def _load_sales(self) -> pd.DataFrame:
        """Load all sales files (CSV)."""
        project_root = get_project_root()
        full_sales_path = (project_root / self.SALES_DIR).resolve()
        sales_files = list(full_sales_path.glob("sales_*.csv"))
        if not sales_files:
            raise FileNotFoundError(f"No sales files found in {self.SALES_DIR}, Run sales generator first.")

        logger.info(f"Found {len(sales_files)} sales files found in {self.SALES_DIR}.")

        dfs = []
        for sales_file in sales_files:
            if sales_file.suffix == ".csv":
                df = pd.read_csv(sales_file)
            else:
                continue
            dfs.append(df)

        if not dfs:
            raise ValueError("No valid sales files loaded.")

        return pd.concat(dfs, ignore_index=True)

    def generate_returns(self) -> pd.DataFrame:
        """Generate realistic returns data based on sales data."""
        sales = self._load_sales()
        logger.info(f"Loaded {len(sales)} sales data.")
        logger.info(f"Using {len(sales):,} sales orders for return generation...")

        n_returns = max(1, int(len(sales) * self.config["return_rate"]))
        returned_orders = sales.sample(n_returns, replace=False)

        returns = []
        for _, order in returned_orders.iterrows():
            days_late = np.random.randint(1, self.config["max_days_after_purchase"])
            return_date = pd.to_datetime(order["order_date"]) + timedelta(days=days_late)

            returns.append({
                "return_id": f"RET_{np.random.randint(100000, 999999)}",
                "order_id": order["order_id"],
                "customer_id": order["customer_id"],
                "product_id": order["product_id"],
                "return_date": return_date,
                "return_reason": np.random.choice([
                    "Defective", "Wrong size", "Not as described",
                    "Changed mind", "Arrived late"
                ]),
                "return_value": round(order["price"] * order["quantity"] * np.random.uniform(0.9, 1.0), 2)
            })

        df_returns = pd.DataFrame(returns)
        df_returns = df_returns.astype(self.SCHEMA)
        logger.info(f"Generated {len(df_returns):,} returns.")
        return df_returns

    def save(self, df: pd.DataFrame) -> None:
        """Save DataFrame to disk."""
        project_root = get_project_root()
        full_path = (project_root / self.OUTPUT_DIR).resolve()
        full_path.mkdir(parents=True, exist_ok=True)
        if self.config["output_format"] == "csv":
            date_str = datetime.now().strftime("%Y%m%d%H%m%s")
            suffix = "csv"
            output_file = full_path/f"returns_{date_str}.{suffix}"
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(df):,} rows → {output_file}...")

    def run(self):
        df = self.generate_returns()
        self.save(df)


def main():
    returns_generator = ReturnsDataGenerator()
    returns_generator.run()


if __name__ == "__main__":
    main()