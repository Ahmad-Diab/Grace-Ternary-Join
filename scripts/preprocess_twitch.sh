#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m';    GREEN='\033[0;32m'
YELLOW='\033[1;33m'; BLUE='\033[0;34m'
CYAN='\033[0;36m';   NC='\033[0m'

URL="https://snap.stanford.edu/data/twitch_gamers.zip"
ARCHIVE="$(basename "$URL")"
CSV="large_twitch_edges.csv"
TMP="${CSV}.tmp"
BIN="parse_tuples"

echo -e "${CYAN}Downloading${NC} ${YELLOW}$URL${NC} …"
wget -q --show-progress -O "$ARCHIVE" "$URL"

echo -e "${CYAN}Extracting${NC} ${YELLOW}$CSV${NC} from $ARCHIVE …"
unzip -p "$ARCHIVE" "$CSV" > "$CSV"

echo -e "${CYAN}Removing header line from${NC} ${YELLOW}$CSV${NC} …"
tail -n +2 "$CSV" > "$TMP"

mv "$TMP" "$CSV"

echo -e "${BLUE}Running${NC} ./${BIN} on ${YELLOW}$CSV${NC} …"
./"$BIN" "$CSV" ../benchmark/resources/twitch/twitch_R.bin

mv "$CSV" ../benchmark/resources/twitch/twitch.csv
rm -r "$ARCHIVE"
echo -e "${GREEN}Processed data is in${NC} ${YELLOW}/benchmark/resources/twitch/${NC}"