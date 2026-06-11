#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Download the Olist Brazilian E-Commerce dataset from Kaggle into data/raw/.
# Requires the Kaggle CLI and a token at ~/.kaggle/kaggle.json
# (or KAGGLE_USERNAME / KAGGLE_KEY env vars).
# ---------------------------------------------------------------------------
set -euo pipefail

RAW_DIR="${RAW_PATH:-./data/raw}"
DATASET="olistbr/brazilian-ecommerce"

echo ">> Ensuring raw dir: ${RAW_DIR}"
mkdir -p "${RAW_DIR}"

if ! command -v kaggle >/dev/null 2>&1; then
  echo ">> Installing kaggle CLI..."
  pip install --quiet kaggle
fi

echo ">> Downloading ${DATASET} ..."
kaggle datasets download -d "${DATASET}" -p "${RAW_DIR}" --unzip

echo ">> Files in ${RAW_DIR}:"
ls -1 "${RAW_DIR}"

echo ">> Done. Expected 9 CSV files."
