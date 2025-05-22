#!/bin/bash

# Config
HOLIDAY_FILE="/home/x01545807/uk_holidays.txt"
TODAY=$(date +%Y-%m-%d)
YEAR=$(date +%Y)
MONTH=$(date +%m)

# Step 1: Generate all dates in the current month
ALL_DAYS=()
for DAY in {1..31}; do
  DATE=$(date -d "$YEAR-$MONTH-$(printf '%02d' $DAY)" +%Y-%m-%d 2>/dev/null)
  [ -z "$DATE" ] && continue  # Skip invalid dates
  DOW=$(date -d "$DATE" +%u)  # Day of week: 1 (Mon) to 7 (Sun)
  
  # Step 2: Skip weekends
  if [[ "$DOW" -lt 6 ]]; then
    # Step 3: Skip holidays
    if ! grep -q "^$DATE$" "$HOLIDAY_FILE"; then
      ALL_DAYS+=("$DATE")
    fi
  fi
done

# Step 4: Is today the 5th working day?
if [[ "$TODAY" == "${ALL_DAYS[4]}" ]]; then
  echo "Today is the 5th working day: $TODAY. Running job..."

  /sas/Prod/SAS94M4/SASFoundation/9.4/sas \
    -sysin /home/x01545807/your_code.sas \
    -log /home/x01545807/logs/working_day5_$(date +%Y%m%d).log

else
  echo "Today is NOT the 5th working day. No action taken."
fi
