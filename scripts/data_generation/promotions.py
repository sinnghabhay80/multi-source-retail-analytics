"""
Retail360 Synthetic Promotions Data Generator
Generates realistic promotions data with full control via CLI + config.
"""
from pydoc import replace

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from utils.logger import get_logger
from utils.config import load_config, get_project_root

logger = get_logger(__name__)

class PromotionsDataGenerator:
    """Generates realistic promotions data."""
    SCHEMA = {
        "promo_id": "string",
        "promo_name": "string",
        "discount_pct": "float64",
        "start_date": "datetime64[ns]",
        "end_date": "datetime64[ns]",
        "target_segment": "string",
        "status": "string"
    }
    CONFIG_PATH = "configs/data_generation/promotions.yaml"
    OUTPUT_DIR = Path("data/raw/promotions")

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
        if cfg["min_discount"] < 5 or cfg["max_discount"] > 50:
            raise ValueError("Discount should be between 5-50%!")
        if cfg["output_format"] != "csv":
            raise ValueError("output_format must be 'csv'")

    def generate_promotions(self):
        """Generates realistic promotions data."""
        logger.info(f"Generating {self.config['n_campaigns']}...")

        promos = []
        for i in range(1, self.config["n_campaigns"] + 1):
            start_offset = np.random.randint(-180, 180)  # campaigns from past to future
            start_date = datetime.now() + timedelta(days=start_offset)
            duration = np.random.randint(self.config["min_duration_days"], self.config["max_duration_days"] + 1)
            end_date = start_date + timedelta(days=duration)

            promos.append({
                "promo_id": f"PROMO_{i:03d}",
                "promo_name": np.random.choice([
                    "Summer Sale", "Black Friday", "Flash Deal", "New User",
                    "Free Shipping", "Buy 2 Get 1", "Clearance", "VIP Exclusive"
                ]) + f" {start_date.year}",
                "discount_pct": round(np.random.uniform(self.config["min_discount"], self.config["max_discount"]), 1),
                "start_date": start_date.date(),
                "end_date": end_date.date(),
                "target_segment": np.random.choice(["All", "New", "VIP", "Lapsed", "HighSpender"]),
                "status": "Active" if end_date > datetime.now() else "Expired"
            })

        df_promos = pd.DataFrame(promos)
        df_promos = df_promos.astype(self.SCHEMA)
        logger.info(f"Generated {len(df_promos)} promotions")
        return df_promos

    def save(self, df: pd.DataFrame) -> None:
        """Save DataFrame to disk."""
        project_root = get_project_root()
        full_path = (project_root / self.OUTPUT_DIR).resolve()
        full_path.mkdir(parents=True, exist_ok=True)
        if self.config["output_format"] == "csv":
            date_str = datetime.now().strftime("%Y%m%d%H%m%s")
            suffix = "csv"
            output_file = full_path / f"promotions_{date_str}.{suffix}"
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(df):,} rows → {output_file}...")

    def run(self):
        df = self.generate_promotions()
        self.save(df)

def main():
    promos_generator = PromotionsDataGenerator()
    promos_generator.run()

if __name__ == "__main__":
    main()