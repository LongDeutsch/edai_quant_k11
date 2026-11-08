"""
Great Expectations Quality Checkpoint Runner (Coursework Specification).

Validates datasets against declared JSON expectation suites:
- expect_column_to_exist
- expect_column_values_to_not_be_null
- expect_column_values_to_be_between
- expect_column_values_to_be_in_set
- expect_column_values_to_be_unique

Outputs structured validation artifacts and HTML/Markdown Data Docs reports.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


class GreatExpectationsRunner:
    def __init__(self, suites_dir: str = "configs/quality"):
        self.suites_dir = Path(suites_dir)

    def load_suite(self, suite_name: str) -> Dict[str, Any]:
        suite_path = self.suites_dir / f"{suite_name}.json"
        if not suite_path.exists():
            raise FileNotFoundError(f"Expectation suite not found at {suite_path}")
        with open(suite_path, "r") as f:
            return json.load(f)

    def validate_dataframe(self, df: pd.DataFrame, suite_name: str) -> Dict[str, Any]:
        suite = self.load_suite(suite_name)
        expectations = suite.get("expectations", [])
        results: List[Dict[str, Any]] = []
        passed_count = 0

        for exp in expectations:
            etype = exp["expectation_type"]
            kwargs = exp["kwargs"]
            col = kwargs.get("column")
            res_item = {
                "expectation_type": etype,
                "kwargs": kwargs,
                "success": False,
                "result": {},
            }

            if etype == "expect_column_to_exist":
                success = col in df.columns
                res_item["success"] = success
                res_item["result"] = {"observed_value": col in df.columns}

            elif etype == "expect_column_values_to_not_be_null":
                if col in df.columns:
                    null_count = int(df[col].isna().sum())
                    success = null_count == 0
                    res_item["success"] = success
                    res_item["result"] = {
                        "unexpected_count": null_count,
                        "unexpected_percent": round(null_count / max(1, len(df)) * 100, 2),
                    }
                else:
                    res_item["success"] = False
                    res_item["result"] = {"error": f"Column {col} missing"}

            elif etype == "expect_column_values_to_be_between":
                if col in df.columns:
                    s = pd.to_numeric(df[col], errors="coerce")
                    min_v = kwargs.get("min_value")
                    max_v = kwargs.get("max_value")
                    cond = pd.Series(True, index=s.index)
                    if min_v is not None:
                        cond &= s >= min_v
                    if max_v is not None:
                        cond &= s <= max_v
                    unexpected = int((~cond).sum())
                    success = unexpected == 0
                    res_item["success"] = success
                    res_item["result"] = {
                        "unexpected_count": unexpected,
                        "min_observed": float(s.min()) if len(s) else None,
                        "max_observed": float(s.max()) if len(s) else None,
                    }
                else:
                    res_item["success"] = False
                    res_item["result"] = {"error": f"Column {col} missing"}

            elif etype == "expect_column_values_to_be_in_set":
                if col in df.columns:
                    allowed = set(kwargs["value_set"])
                    observed = set(df[col].dropna().unique())
                    unexp = list(observed - allowed)
                    success = len(unexp) == 0
                    res_item["success"] = success
                    res_item["result"] = {"unexpected_values": unexp}
                else:
                    res_item["success"] = False
                    res_item["result"] = {"error": f"Column {col} missing"}

            elif etype == "expect_column_values_to_be_unique":
                if col in df.columns:
                    dups = int(df[col].duplicated().sum())
                    success = dups == 0
                    res_item["success"] = success
                    res_item["result"] = {"duplicate_count": dups}
                else:
                    res_item["success"] = False
                    res_item["result"] = {"error": f"Column {col} missing"}

            if res_item["success"]:
                passed_count += 1
            results.append(res_item)

        total_exp = len(expectations)
        success_ratio = passed_count / max(1, total_exp)
        is_all_success = (passed_count == total_exp)

        summary = {
            "suite_name": suite_name,
            "table_meta": suite.get("meta", {}),
            "evaluated_rows": int(len(df)),
            "total_expectations": total_exp,
            "successful_expectations": passed_count,
            "success_percent": round(success_ratio * 100, 2),
            "status": "PASSED" if is_all_success else "FAILED",
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "results": results,
        }
        return summary

    def run_checkpoint(
        self,
        data_path: str,
        suite_name: str,
        out_report: Optional[str] = None,
    ) -> Dict[str, Any]:
        p = Path(data_path)
        if not p.exists():
            raise FileNotFoundError(f"Data file/dir not found at {data_path}")
        df = pd.read_parquet(p)
        print(f"[GXRunner] Running Checkpoint for '{suite_name}' on {len(df)} records...")
        summary = self.validate_dataframe(df, suite_name)
        print(f"[GXRunner] Checkpoint result: {summary['status']} ({summary['success_percent']}% passed).")

        if out_report:
            out_p = Path(out_report)
            out_p.parent.mkdir(parents=True, exist_ok=True)
            with open(out_p, "w") as f:
                json.dump(summary, f, indent=2)
            print(f"[GXRunner] Report saved to {out_report}")

            # Also generate Markdown Data Docs
            md_path = out_p.with_suffix(".md")
            self._render_markdown_docs(summary, md_path)

        return summary

    def _render_markdown_docs(self, summary: Dict[str, Any], md_path: Path) -> None:
        lines = [
            f"# Great Expectations Data Docs: {summary['suite_name']}",
            f"> Evaluated at: `{summary['evaluated_at']}` | Status: **{summary['status']}**",
            "",
            f"- **Evaluated Records**: {summary['evaluated_rows']:,}",
            f"- **Scorecard**: {summary['successful_expectations']} / {summary['total_expectations']} expectations ({summary['success_percent']}%)",
            "",
            "## Detailed Results",
            "| Expectation Type | Column | Parameters | Status | Details |",
            "|---|---|---|---|---|",
        ]
        for r in summary["results"]:
            c = r["kwargs"].get("column", "N/A")
            status = "✅ PASSED" if r["success"] else "❌ FAILED"
            param_str = json.dumps({k: v for k, v in r["kwargs"].items() if k != "column"})
            details_str = json.dumps(r["result"])
            lines.append(f"| `{r['expectation_type']}` | `{c}` | {param_str} | {status} | {details_str} |")

        with open(md_path, "w") as f:
            f.write("\n".join(lines) + "\n")


def run_gx_checkpoint(
    data_path: str,
    suite_path: str,
    doc_out_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Top-level helper to execute a Great Expectations checkpoint."""
    s_p = Path(suite_path)
    suite_dir = str(s_p.parent)
    suite_name = s_p.stem

    runner = GreatExpectationsRunner(suites_dir=suite_dir)
    json_out = doc_out_path
    if doc_out_path and doc_out_path.endswith(".md"):
        json_out = str(Path(doc_out_path).with_suffix(".json"))

    summary = runner.run_checkpoint(
        data_path=data_path,
        suite_name=suite_name,
        out_report=json_out,
    )
    if doc_out_path and doc_out_path.endswith(".md"):
        runner._render_markdown_docs(summary, Path(doc_out_path))

    summary["success"] = summary["status"] == "PASSED"
    summary["evaluated_expectations"] = summary["total_expectations"]
    summary["unsuccessful_expectations"] = summary["total_expectations"] - summary["successful_expectations"]
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Great Expectations Quality Checkpoint Runner")
    parser.add_argument("--data", required=True, help="Path to Parquet dataset")
    parser.add_argument("--suite", required=True, help="Expectation suite name (e.g. suite_raw_market_data)")
    parser.add_argument("--out-report", default=None, help="Output JSON report path")
    args = parser.parse_args()

    runner = GreatExpectationsRunner()
    res = runner.run_checkpoint(data_path=args.data, suite_name=args.suite, out_report=args.out_report)
    print(json.dumps(res, indent=2))
