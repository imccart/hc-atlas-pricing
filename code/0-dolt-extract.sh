#!/usr/bin/env bash
# Meta --------------------------------------------------------------------
# Title:         Dolt Data Extraction
# Author:        Ian McCarthy
# Date Created:  2026-02-16
# Description:   Exports filtered hospital and rate data from the local Dolt
#                clone of dolthub/transparency-in-pricing. Run once manually
#                after cloning the Dolt repo.
#
# Prerequisites:
#   1. Install Dolt:  winget install DoltHub.Dolt
#   2. Shallow clone:
#        cd "C:/Users/immccar/SynologyDrive/work/research-data/Hospital Price Transparency"
#        dolt clone --depth 1 dolthub/transparency-in-pricing dolthub
#
# Usage:
#   bash code/0-dolt-extract.sh
# -------------------------------------------------------------------------

set -euo pipefail

DOLT_DIR="C:/Users/immccar/SynologyDrive/work/research-data/Hospital Price Transparency/dolthub"
OUT_DIR="data/input/dolthub"

if [ ! -d "$DOLT_DIR" ]; then
  echo "ERROR: Dolt clone not found at $DOLT_DIR"
  echo "Run:  cd 'C:/Users/immccar/SynologyDrive/work/research-data/Hospital Price Transparency'"
  echo "      dolt clone --depth 1 dolthub/transparency-in-pricing dolthub"
  exit 1
fi

mkdir -p "$OUT_DIR"

# -- Hospital table (full export, ~12K rows) --------------------------------
echo "Exporting hospital table..."
cd "$DOLT_DIR"
dolt table export hospital "$OLDPWD/$OUT_DIR/hospital.csv"
cd "$OLDPWD"
echo "  -> $(wc -l < "$OUT_DIR/hospital.csv") lines in hospital.csv"

# -- Target MS-DRGs ---------------------------------------------------------
# 10 common inpatient DRGs (joint/spine, OB, cardiac, GI)
DRGS="470,473,743,766,767,291,292,329,330,871"

echo "Exporting rates for MS-DRGs: $DRGS"
echo "  (This may take 30-90 minutes on a full table scan...)"
cd "$DOLT_DIR"
dolt sql --result-format csv -q "
  SELECT *
  FROM rate
  WHERE ms_drg IN ($DRGS)
" > "$OLDPWD/$OUT_DIR/rates-drg.csv"
cd "$OLDPWD"
echo "  -> $(wc -l < "$OUT_DIR/rates-drg.csv") lines in rates-drg.csv"

# -- Target CPT/HCPCS codes ------------------------------------------------
# 23 common outpatient codes (ED, office visits, imaging, cardiac, GI, ortho, labs)
CPTS="'99213','99214','99215','99281','99282','99283','99284','99285','70553','74177','27447','29881','43239','93000','93306','93452','36415','80053','85025','71046','72148','G0105','G0121'"

echo "Exporting rates for CPT/HCPCS codes..."
echo "  (This may take 30-90 minutes on a full table scan...)"
cd "$DOLT_DIR"
dolt sql --result-format csv -q "
  SELECT *
  FROM rate
  WHERE hcpcs_cpt IN ($CPTS)
" > "$OLDPWD/$OUT_DIR/rates-cpt.csv"
cd "$OLDPWD"
echo "  -> $(wc -l < "$OUT_DIR/rates-cpt.csv") lines in rates-cpt.csv"

echo ""
echo "Done. Exported files:"
ls -lh "$OUT_DIR"/*.csv
