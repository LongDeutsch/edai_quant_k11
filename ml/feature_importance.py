# Feature Importance Extraction
"""Compute feature importance for the trained Alpha model.

Assumes a trained model artifact (e.g., a scikit‑learn estimator) is stored at
``model_path`` and that a validation feature set is available as a parquet file.
The script writes ``feature_importance.json`` alongside the model artifacts.
"""

import json
import os
import pandas as pd
from sklearn.inspection import permutation_importance
from joblib import load as joblib_load
from quant_edai.common.dataset_paths import resolve_dataset_root

def load_validation_features(split_id: str = "OOS") -> pd.DataFrame:
    root = resolve_dataset_root(os.getenv("OUT_DIR", "/tmp/lakehouse_source"))
    feat_path = os.path.join(root, "gold_features/feat_ticker_technical_indicators.parquet")
    df = pd.read_parquet(feat_path)
    if "split_id" in df.columns:
        df = df[df["split_id"] == split_id]
    return df

def main():
    model_path = os.getenv("MODEL_PATH")
    if not model_path or not os.path.exists(model_path):
        print("MODEL_PATH env var not set or file missing; skipping feature importance.")
        return
    model = joblib_load(model_path)
    df = load_validation_features()
    if "target_label" not in df.columns:
        print("No target_label in validation set; cannot compute importance.")
        return
    X = df.drop(columns=["target_label"]).values
    y = df["target_label"].values
    result = permutation_importance(model, X, y, n_repeats=5, random_state=42, n_jobs=1)
    importance = {f"feature_{i}": float(score) for i, score in enumerate(result.importances_mean)}
    out_path = os.path.join(os.path.dirname(model_path), "feature_importance.json")
    with open(out_path, "w") as f:
        json.dump(importance, f, indent=2)
    print(f"Feature importance written to {out_path}")

if __name__ == "__main__":
    main()
