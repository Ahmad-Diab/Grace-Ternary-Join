#!/usr/bin/env bash
set -euo pipefail

# ─── Color Palette ─────────────────────────────────────────────────────────────
RED='\033[0;31m';    GREEN='\033[0;32m'
YELLOW='\033[1;33m'; BLUE='\033[0;34m'
CYAN='\033[0;36m';   NC='\033[0m'

URL="https://snap.stanford.edu/data/wiki-Talk.txt.gz"
ARCHIVE="$(basename "$URL")"
RAW="${ARCHIVE%.gz}"
TMP_HEADER="${RAW}.nohead"
TMP_COMMA="${RAW}.comma"
BIN="parse_tuples"

echo -e "${CYAN}Downloading${NC} ${YELLOW}$URL${NC} …"
wget -q --show-progress -O "$ARCHIVE" "$URL"

echo -e "${CYAN}Uncompressing${NC} ${YELLOW}$ARCHIVE${NC} …"
gunzip -f "$ARCHIVE"

echo -e "${CYAN}Removing first 4 lines from${NC} ${YELLOW}$RAW${NC} …"
tail -n +5 "$RAW" > "$TMP_HEADER"

echo -e "${CYAN}Converting whitespace to commas in${NC} ${YELLOW}$TMP_HEADER${NC} …"
tr -s ' \t' ',' < "$TMP_HEADER" > "$TMP_COMMA"

mv "$TMP_COMMA" "$RAW"
rm -f "$TMP_HEADER"

echo -e "${BLUE}Running${NC} ./${BIN} on ${YELLOW}$RAW${NC} …"
./"$BIN" "$RAW" ../benchmark/resources/wiki/wiki_R.bin

mv "$RAW" ../benchmark/resources/wiki/wiki.csv
echo -e "${GREEN}Processed data is in${NC} ${YELLOW}/benchmark/resources/wiki/${NC}"
