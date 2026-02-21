# ct/score/analyze_ct_scores.py
#
# Quick-and-dirty cyber-style analysis for CT scores.
# Run from repo root:
#   source .venv/bin/activate
#   python ct/score/analyze_ct_scores.py

import os
import glob

import numpy as np
import pandas as pd
import tldextract


# ---------- path helpers ----------

THIS_DIR = os.path.dirname(__file__)               # .../ct/score
CT_DIR = os.path.abspath(os.path.join(THIS_DIR, ".."))
DATA_DIR = os.path.join(CT_DIR, "data")
SCORED_DIR = os.path.join(DATA_DIR, "scored")


def latest_scored_file() -> str:
    """Pick the newest ct_scored_*.parquet from ct/data/scored."""
    pattern = os.path.join(SCORED_DIR, "ct_scored_*.parquet")
    paths = glob.glob(pattern)
    if not paths:
        raise SystemExit(f"[analyze] no scored files found under {pattern}")
    paths.sort()
    return paths[-1]


def get_domain_column(df: pd.DataFrame) -> str:
    """Prefer registered_domain; fall back to domain_sample."""
    if "registered_domain" in df.columns:
        return "registered_domain"
    if "domain_sample" in df.columns:
        return "domain_sample"
    raise SystemExit("[analyze] neither 'registered_domain' nor 'domain_sample' present")


def extract_tld(domain: str) -> str:
    if not isinstance(domain, str) or not domain:
        return ""
    ext = tldextract.extract(domain)
    return ext.suffix or ""


def ip_bucket(n):
    if pd.isna(n) or float(n) <= 0:
        return "ips:0"
    n = float(n)
    if n == 1:
        return "ips:1"
    if n <= 3:
        return "ips:2-3"
    if n <= 10:
        return "ips:4-10"
    return "ips:>10"


def main():
    scored_path = latest_scored_file()
    print(f"[analyze] using scored file: {scored_path}")

    df = pd.read_parquet(scored_path)
    print(f"[analyze] rows={len(df)}")
    print("[analyze] columns:", list(df.columns))

    if "week5_score" not in df.columns:
        raise SystemExit("[analyze] expected 'week5_score' column missing")

    dom_col = get_domain_column(df)

    # ---------- basic score stats ----------

    print("\n== score distribution ==")
    desc = df["week5_score"].describe(
        percentiles=[0.5, 0.9, 0.95, 0.99, 0.999]
    )
    print(desc)

    print("\n== score thresholds ==")
    for thr in [0.5, 0.9, 0.99]:
        n = int((df["week5_score"] >= thr).sum())
        pct = n / len(df) * 100.0
        print(f"score >= {thr:.2f}: {n}  ({pct:.2f}% of {len(df)})")

    # bucket-level view
    if "risk_bucket" in df.columns:
        print("\n== risk_bucket counts & score ranges ==")
        bucket_stats = (
            df.groupby("risk_bucket")["week5_score"]
            .agg(["count", "min", "max", "mean"])
            .sort_values("mean", ascending=False)
        )
        print(bucket_stats.to_string(float_format=lambda x: f"{x:.4f}"))

    # ---------- TLD risk ----------

    print("\n== TLD risk (min_count=20) ==")
    df["tld"] = df[dom_col].map(extract_tld)

    tld_stats = (
        df.groupby("tld")
        .agg(
            count=(dom_col, "size"),
            avg_score=("week5_score", "mean"),
            max_score=("week5_score", "max"),
            pct_high=("week5_score", lambda s: (s >= 0.9).mean() * 100.0),
        )
        .reset_index()
    )
    tld_stats = tld_stats[tld_stats["count"] >= 20].sort_values(
        ["pct_high", "avg_score"], ascending=[False, False]
    )

    if not tld_stats.empty:
        print(
            tld_stats.head(30).to_string(
                index=False, float_format=lambda x: f"{x:.3f}"
            )
        )
    else:
        print("(no TLDs with >=20 domains)")

    # ---------- hosting country risk ----------

    if "sample_country" in df.columns:
        print("\n== hosting country risk (min_count=20) ==")
        cstats = (
            df.groupby("sample_country")
            .agg(
                count=(dom_col, "size"),
                avg_score=("week5_score", "mean"),
                max_score=("week5_score", "max"),
                pct_high=("week5_score", lambda s: (s >= 0.9).mean() * 100.0),
            )
            .reset_index()
        )
        cstats = cstats[cstats["count"] >= 20].sort_values(
            ["pct_high", "avg_score"], ascending=[False, False]
        )
        if not cstats.empty:
            print(
                cstats.head(30).to_string(
                    index=False, float_format=lambda x: f"{x:.3f}"
                )
            )
        else:
            print("(no countries with >=20 domains)")
    else:
        print("\n== hosting country risk ==\n(sample_country column not present)")

    # ---------- IP diversity risk ----------

    if "num_unique_ips" in df.columns:
        print("\n== num_unique_ips buckets ==")
        df["ip_bucket"] = df["num_unique_ips"].map(ip_bucket)
        ip_stats = (
            df.groupby("ip_bucket")
            .agg(
                count=(dom_col, "size"),
                avg_score=("week5_score", "mean"),
                pct_high=("week5_score", lambda s: (s >= 0.9).mean() * 100.0),
            )
            .reset_index()
        )
        print(
            ip_stats.sort_values("avg_score", ascending=False).to_string(
                index=False, float_format=lambda x: f"{x:.3f}"
            )
        )
    else:
        print("\n== num_unique_ips buckets ==\n(num_unique_ips column not present)")

    # ---------- keyword hunting (phishy patterns) ----------

    keywords = [
        "login",
        "secure",
        "update",
        "verify",
        "account",
        "password",
        "signin",
        "pay",
        "paypal",
        "bank",
        "office",
        "microsoft",
        "apple",
        "support",
        "delivery",
        "package",
        "post",
        "dhl",
        "ups",
    ]
    kw_regex = "|".join(keywords)

    df["__dom_lower"] = df[dom_col].fillna("").str.lower()
    mask_kw = df["__dom_lower"].str.contains(kw_regex, regex=True)

    df_kw = df[mask_kw].copy()
    print(f"\n== domains containing suspicious keywords ({len(df_kw)}) ==")
    if not df_kw.empty:
        cols_show = [dom_col, "week5_score"]
        if "risk_bucket" in df_kw.columns:
            cols_show.append("risk_bucket")
        if "sample_country" in df_kw.columns:
            cols_show.append("sample_country")

        print(
            df_kw.sort_values("week5_score", ascending=False)[cols_show]
            .head(50)
            .to_string(index=False)
        )

    # ---------- optional histogram plot ----------

    try:
        import matplotlib.pyplot as plt  # type: ignore

        FIG_DIR = os.path.join(CT_DIR, "figures")
        os.makedirs(FIG_DIR, exist_ok=True)

        plt.figure()
        plt.hist(df["week5_score"], bins=50)
        plt.xlabel("week5_score")
        plt.ylabel("count")
        plt.title("CT domain risk score distribution (week5 model)")
        out_png = os.path.join(FIG_DIR, "week5_score_hist.png")
        plt.savefig(out_png, bbox_inches="tight")
        plt.close()
        print(f"\n[analyze] saved histogram to {out_png}")
    except Exception as e:
        print(f"\n[analyze] histogram skipped (matplotlib missing or failed: {e})")


if __name__ == "__main__":
    main()
