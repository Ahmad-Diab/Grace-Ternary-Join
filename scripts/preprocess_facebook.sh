#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m';    GREEN='\033[0;32m'
YELLOW='\033[1;33m'; BLUE='\033[0;34m'
CYAN='\033[0;36m';   NC='\033[0m'

URL="https://snap.stanford.edu/data/facebook_combined.txt.gz"
ARCHIVE="facebook_combined.txt.gz"
RAW="facebook_combined.txt"
BIN="parse_tuples"

echo -e "${CYAN}Downloading${NC} ${YELLOW}$URL${NC} …"
wget -q --show-progress -O "$ARCHIVE" "$URL"

echo -e "${CYAN}Uncompressing${NC} ${YELLOW}$ARCHIVE${NC} …"
gunzip -f "$ARCHIVE"

echo -e "${CYAN}Normalizing whitespace → commas in${NC} ${YELLOW}$RAW${NC} …"
tr -s ' \t' ',' < "$RAW" > "${RAW}.tmp"
mv "${RAW}.tmp" "$RAW"

echo -e "${BLUE}Running${NC} ./${BIN} on ${YELLOW}$RAW${NC} …"
./"$BIN" "$RAW" ../benchmark/resources/facebook/facebook_R.bin

mv "$RAW" ../benchmark/resources/facebook/facebook.csv
echo -e "${GREEN}Processed data is in${NC} ${YELLOW}/benchmark/resources/facebook/${NC}"