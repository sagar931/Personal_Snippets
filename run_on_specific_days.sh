#!/bin/bash

# File that contains allowed dates
DATE_LIST_FILE="allowed_dates.txt"

# Today's date in YYYY-MM-DD format
TODAY=$(date +%F)

# Check if the date list file exists
if [[ ! -f "$DATE_LIST_FILE" ]]; then
  echo "❌ Error: $DATE_LIST_FILE not found."
  exit 1
fi

# Check if today is in the list of allowed dates
if grep -q "$TODAY" "$DATE_LIST_FILE"; then
  echo "✅ Today ($TODAY) is in the allowed list. Running the job..."
  
  # Replace the below command with your actual code
  /path/to/your/code_or_command.sh

else
  echo "⏭️  Today ($TODAY) is not in the list. Skipping execution."
fi
