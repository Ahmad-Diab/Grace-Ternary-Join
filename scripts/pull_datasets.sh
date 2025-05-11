#!/usr/bin/env bash
set -euo pipefail

CPP="parse_tuples.cpp"
BIN="parse_tuples"
MAGENTA='\033[0;35m'
YELLOW='\033[1;33m'
NC='\033[0m'
GREEN='\033[0;32m'

echo -e "${MAGENTA}Compiling${NC} ${YELLOW}$CPP${NC} → ${YELLOW}$BIN${NC} …"
g++ -std=c++20 -O3 "$CPP" -o "$BIN"

bash preprocess_facebook.sh
printf "\n"
bash preprocess_twitter.sh
printf "\n"
bash preprocess_wiki.sh
printf "\n"
bash preprocess_twitch.sh
printf "\n"
bash preprocess_livejournal.sh
printf "\n"

rm parse_tuples

echo -e "${GREEN}All done!${NC}"
