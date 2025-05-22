#!/bin/bash

# Usage: ./get_working_day.sh <WD number>
# Example: ./get_working_day.sh 7 → prints 7th working day's date

WD_NUM="$1"
if [[ -z "$WD_NUM" || "$WD_NUM" -le 0 ]]; then
  echo "Please provide a valid Working Day number (e.g., 5, 7)"
  exit 1
fi

HOLIDAY_FILE="/home/x01545807/uk_holidays.txt"
YEAR=$(date +%Y)
MONTH=$(date +%m)
WORKING_DAYS=()

# Step 1: Build list of working days (Mon–Fri, excluding holidays)
for DAY in {1..31}; do
  DATE=$(date -d "$YEAR-$MONTH-$(printf '%02d' $DAY)" +%Y-%m-%d 2>/dev/null)
  [ -z "$DATE" ] && continue

  DOW=$(date -d "$DATE" +%u)  # 1 = Monday, 7 = Sunday
  if [[ "$DOW" -lt 6 ]] && ! grep -q "^$DATE$" "$HOLIDAY_FILE"; then
    WORKING_DAYS+=("$DATE")
  fi
done

# Step 2: Print the requested working day
INDEX=$((WD_NUM - 1))
if [[ "$INDEX" -ge ${#WORKING_DAYS[@]} ]]; then
  echo "This month only has ${#WORKING_DAYS[@]} working days. WD${WD_NUM} does not exist."
  exit 1
fi

echo "WD${WD_NUM} for $MONTH/$YEAR is: ${WORKING_DAYS[$INDEX]}"
