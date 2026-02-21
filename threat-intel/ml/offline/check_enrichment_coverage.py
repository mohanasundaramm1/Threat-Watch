# check_enrichment_coverage.py
import os, pandas as pd
import tldextract

PHISH_DAYS  = [f"2025-11-{d:02d}" for d in range(17, 27)]
BENIGN_DAYS = ["2025-10-28","2025-10-29","2025-10-30","2025-10-31"]

def reg_domain(d):
    if not isinstance(d, str) or not d:
        return ""
    ext = tldextract.extract(d)
    reg = getattr(ext, "registered_domain", None) or getattr(
        ext, "top_domain_under_public_suffix", None
    ) or ""
    return reg.lower().strip()

def main():
    all_days = PHISH_DAYS + BENIGN_DAYS

    def exists(path):
        return os.path.exists(path)

    print("=== PER-DAY LABEL / DNS / WHOIS ===")
    for ds in all_days:
        labels_p = f"../silver/labels_union/ingest_date={ds}/labels_union.parquet"
        dns_p    = f"../lookups/dns_geo/ingest_date={ds}/dns_geo.parquet"
        whois_p  = f"../lookups/whois/ingest_date={ds}/whois.parquet"
        print(f"\n== {ds} ==")
        print("  labels:", labels_p, "exists?", exists(labels_p))
        print("  dns   :", dns_p,    "exists?", exists(dns_p))
        print("  whois :", whois_p,  "exists?", exists(whois_p))

    # union coverage for benign only
    labels = []
    for ds in BENIGN_DAYS:
        p = f"../silver/labels_union/ingest_date={ds}/labels_union.parquet"
        if exists(p):
            labels.append(pd.read_parquet(p))
    if not labels:
        print("\n[err] no benign labels at all")
        return
    labels = pd.concat(labels, ignore_index=True)
    labels["registered_domain"] = labels["domain"].map(reg_domain)
    labels = labels[labels["registered_domain"].astype(bool)].copy()
    benign_domains = set(labels["registered_domain"])
    print(f"\nTotal benign registered domains: {len(benign_domains)}")

    dns_frames = []
    for ds in BENIGN_DAYS:
        p = f"../lookups/dns_geo/ingest_date={ds}/dns_geo.parquet"
        if exists(p):
            dns_frames.append(pd.read_parquet(p))
    if dns_frames:
        dns = pd.concat(dns_frames, ignore_index=True)
        dns["registered_domain"] = (
            dns["puny_domain"].astype(str).str.lower().str.strip()
        )
        dns_cov = set(dns["registered_domain"]) & benign_domains
        print(f"DNS-Geo covers {len(dns_cov) / len(benign_domains) * 100:.2f}% of benign domains")

    whois_frames = []
    for ds in BENIGN_DAYS:
        p = f"../lookups/whois/ingest_date={ds}/whois.parquet"
        if exists(p):
            whois_frames.append(pd.read_parquet(p))
    if whois_frames:
        w = pd.concat(whois_frames, ignore_index=True)
        w["registered_domain"] = (
            w["domain"].astype(str).str.lower().str.strip()
        )
        whois_cov = set(w["registered_domain"]) & benign_domains
        print(f"WHOIS covers {len(whois_cov) / len(benign_domains) * 100:.2f}% of benign domains")

if __name__ == "__main__":
    main()
