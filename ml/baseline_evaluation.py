# Baseline Evaluation Script
"""Train and evaluate simple baselines for the Quant EDAI Alpha models.

- Random classifier (uniform probabilities)
- Majority class classifier (always predicts the most frequent label)

Outputs a CSV ``baseline_metrics.csv`` with AUC, Accuracy for each baseline on the OOS split.
"""

import os
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.metrics import roc_auc_score, accuracy_score
from quant_edai.common.dataset_paths import resolve_dataset_root

def load_features(split_id: str = "OOS") -> pd.DataFrame:
    # Resolve the canonical dataset root and load the feature parquet for the given split.
    root = resolve_dataset_root(os.getenv("OUT_DIR", "/tmp/lakehouse_source"))
    feat_path = os.path.join(root, f"gold_features/feat_ticker_technical_indicators.parquet")
    df = pd.read_parquet(feat_path)
    # Assume ``split_id`` column exists; filter accordingly.
    if "split_id" in df.columns:
        df = df[df["split_id"] == split_id]
    return df

def train_and_evaluate(df: pd.DataFrame) -> pd.DataFrame:
    X = df.drop(columns=["target_label"]).values
    y = df["target_label"].values
    results = []
    for strategy in ["uniform", "most_frequent"]:
        clf = DummyClassifier(strategy=strategy, random_state=42)
        clf.fit(X, y)
        preds = clf.predict_proba(X)[:, 1]
        auc = roc_auc_score(y, preds)
        acc = accuracy_score(y, clf.predict(X))
        results.append({"baseline": strategy, "auc": auc, "accuracy": acc})
    return pd.DataFrame(results)

def main():
    df = load_features()
    # Expect a ``target_label`` column; if missing, skip.
    if "target_label" not in df.columns:
        print("No target label column found; baseline evaluation skipped.")
        return
    metrics = train_and_evaluate(df)
    out_path = os.path.join(os.getenv("OUT_DIR", "/tmp"), "baseline_metrics.csv")
    metrics.to_csv(out_path, index=False)
    print(f"Baseline metrics written to {out_path}")

if __name__ == "__main__":
    main()
