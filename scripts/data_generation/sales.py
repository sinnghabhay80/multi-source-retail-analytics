"""
Retail360 Synthetic Sales Data Generator
Generates realistic sales data with full control via CLI + config.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from utils.logger import get_logger
from utils.config import load_config
from utils.config import get_project_root

logger = get_logger(__name__)

class SalesDataGenerator:
    """Generates synthetic sales data with schema enforcement."""

    SCHEMA = {
        "order_id": "string",
        "customer_id": "string",
        "product_id": "string",
        "quantity": "int64",
        "price": "float64",
        "order_date": "datetime64[ns]"
    }

    OUTPUT_DIR = Path("data/raw/sales")
    CONFIG_PATH = "configs/data_generation/sales.yaml"

    def __init__(self, config=None) -> None:
        if config is None:
            config = dict()
        self.config = {**config} # in-case user provides new configs
        default_config = load_config(self.CONFIG_PATH)
        self.config.update(default_config) # We'll use default_config(what we have in configs)
        self._validate_config()
        np.random.seed(self.config["seed"])
        logger.info(f"Random seed set to: {self.config['seed']}")

    def _validate_config(self):
        """Validate configuration values."""
        cfg = self.config
        if cfg["n_records"] <= 0:
            raise ValueError("n_records must be > 0")
        if cfg["output_format"] != "csv":
            raise ValueError("output_format must be 'csv'")

    def generate_sales(self) -> pd.DataFrame:
        """Generate synthetic sales data."""
        cfg = self.config
        n = cfg["n_records"]

        logger.info(f"Generating {n} sales records...")

        data = {
            "order_id": [f"ORD_{i:06d}" for i in range(1, n+1)],
            "customer_id": np.random.choice([f"CUST_{i:05d}" for i in range(1, cfg["n_customers"]+1)], n),
            "product_id": np.random.choice([f"PROD_{i:04d}" for i in range(1, cfg["n_products"] + 1)], n),
            "quantity": np.random.randint(cfg["min_quantity"], cfg["max_quantity"] + 1, n),
            "price": np.random.uniform(cfg["min_price"], cfg["max_price"], n).round(2),
            "order_date": [
                datetime.now() - timedelta(days=np.random.randint(0, cfg["date_range_days"]))
                for _ in range(n)
            ]
        }

        df_sales = pd.DataFrame(data)
        df_sales = df_sales.astype(self.SCHEMA)
        logger.info(f"Generated {n} sales records...")
        return df_sales

    def save(self, df: pd.DataFrame) -> None:
        """Save DataFrame to disk."""
        project_root = get_project_root()
        full_path = (project_root / self.OUTPUT_DIR).resolve()
        full_path.mkdir(parents=True, exist_ok=True)
        if self.config["output_format"] == "csv":
            date_str = datetime.now().strftime("%Y%m%d%H%m%s")
            suffix = "csv"
            output_file = full_path/f"sales_{date_str}.{suffix}"
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(df):,} rows → {output_file}...")

    def run(self):
        df = self.generate_sales()
        self.save(df)


def main():
    sales_generator = SalesDataGenerator()
    sales_generator.run()


if __name__ == "__main__":
    main()