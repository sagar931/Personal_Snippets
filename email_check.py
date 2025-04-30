import win32com.client
import pandas as pd
from datetime import datetime, timedelta

# Function to fetch emails based on the date (today or yesterday)
def fetch_emails(date_str):
    # Access Outlook application
    outlook = win32com.client.Dispatch("Outlook.Application")
    namespace = outlook.GetNamespace("MAPI")
    
    # Access the inbox
    folder = namespace.GetDefaultFolder(6)  # 6 refers to the inbox
    messages = folder.Items
    messages.Sort("[ReceivedTime]", True)  # Sort messages by received time
    
    # Convert the string date to datetime
    date_to_check = datetime.strptime(date_str, "%Y-%m-%d")
    
    # Filter messages by date
    filtered_messages = []
    for msg in messages:
        if msg.ReceivedTime.date() == date_to_check.date():
            filtered_messages.append(msg)
    
    return filtered_messages

# Function to check for emails and write to CSV
def check_process_emails(subjects, date_str):
    # Fetch the emails for the given date
    messages = fetch_emails(date_str)
    
    # Prepare a list to store results
    results = []
    
    # Iterate through the subjects (processes)
    for subject in subjects:
        found = False
        for msg in messages:
            if subject.lower() in msg.Subject.lower():
                # Record the details if email is found
                results.append([subject, date_str, msg.ReceivedTime, "Found"])
                found = True
                break
        
        if not found:
            # If no email is found for the subject, log the status as "Not Found"
            results.append([subject, date_str, "N/A", "Not Found"])
    
    # Create a DataFrame and save to CSV
    df = pd.DataFrame(results, columns=["Subject", "Execution Date", "Mail Found Time", "Status"])
    df.to_csv("email_check_results.csv", index=False)
    print("Results have been saved to email_check_results.csv")

# Example usage:
# Define the list of process subjects you're looking for
process_subjects = [
    "Process 1 Subject",
    "Process 2 Subject",
    "Process 3 Subject"
    # Add all your process subjects here
]

# Define the date to check (e.g., '2025-04-29' for today, '2025-04-28' for yesterday)
date_to_check = "2025-04-29"  # Change this to "yesterday" or a specific date as required

check_process_emails(process_subjects, date_to_check)