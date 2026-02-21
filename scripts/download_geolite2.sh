#!/usr/bin/env bash
# scripts/download_geolite2.sh
# ---------------------------------------------------------------------------
# Downloads the free MaxMind GeoLite2 City + ASN databases for offline
# geolocation (no API rate limits, HTTPS safe, works inside Docker).
#
# Prerequisites:
#   1. Create a free MaxMind account: https://www.maxmind.com/en/geolite2/signup
#   2. Generate a LICENSE_KEY in your account portal
#   3. Run:  MAXMIND_LICENSE_KEY=your_key ./scripts/download_geolite2.sh
#
# The databases are placed in threat-intel/geoip/ and must be added to
# .gitignore (they are binary/large). Mount geoip/ into the Docker container
# at /opt/airflow/geoip/ and set GEO_PROVIDER=maxmind.
# ---------------------------------------------------------------------------
set -euo pipefail

LICENSE_KEY="${MAXMIND_LICENSE_KEY:-}"
if [ -z "$LICENSE_KEY" ]; then
  echo "Error: MAXMIND_LICENSE_KEY environment variable is required."
  echo "  Get a free key at: https://www.maxmind.com/en/geolite2/signup"
  exit 1
fi

DEST_DIR="$(dirname "$0")/../threat-intel/geoip"
mkdir -p "$DEST_DIR"

BASE_URL="https://download.maxmind.com/app/geoip_download"

for EDITION in GeoLite2-City GeoLite2-ASN; do
  echo "Downloading $EDITION..."
  ARCHIVE="$DEST_DIR/${EDITION}.tar.gz"
  curl -fsSL \
    "${BASE_URL}?edition_id=${EDITION}&license_key=${LICENSE_KEY}&suffix=tar.gz" \
    -o "$ARCHIVE"
  tar -xzf "$ARCHIVE" -C "$DEST_DIR" --strip-components=1 \
      --wildcards "*.mmdb" 2>/dev/null || \
    tar -xzf "$ARCHIVE" -C "$DEST_DIR" --strip-components=1 || true
  rm -f "$ARCHIVE"
  echo "  -> $(find "$DEST_DIR" -name "${EDITION}.mmdb" | head -1)"
done

echo ""
echo "Done! Set GEO_PROVIDER=maxmind and mount $DEST_DIR to /opt/airflow/geoip/ in Docker."
echo "The DAG will auto-detect .mmdb files and use them instead of ip-api.com."
