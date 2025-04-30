import win32com.client
import pandas as pd
from datetime import datetime, timedelta

# === CONFIG ===
# Subjects of the processes you want to track
process_subjects = [
    "Daily MI Report",
    "FML Upload",
    "Process A Subject",
    "Process B Subject",
    # Add more expected subjects here
]

# Choose date to check: "today" or "yesterday"
date_option = "today"  # or "yesterday"

# Name of the shared mailbox
shared_mailbox_name = "UKRB FCU DM And MI"


# === UTILITY FUNCTIONS ===

def get_target_date(option):
    """Return the target date based on option"""
    if option.lower() == "yesterday":
        return (datetime.now() - timedelta(days=1)).date()
    return datetime.now().date()


def fetch_messages_from_shared_mailbox(mailbox_name, target_date):
    """Fetch messages from shared mailbox Inbox filtered by date"""
    outlook = win32com.client.Dispatch("Outlook.Application")
    namespace = outlook.GetNamespace("MAPI")

    try:
        inbox = namespace.Folders(mailbox_name).Folders("Inbox")
        messages = inbox.Items
        messages.Sort("[ReceivedTime]", True)
    except Exception as e:
        print(f"Error accessing shared mailbox '{mailbox_name}': {e}")
        return []

    filtered = []
    for msg in messages:
        try:
            if msg.ReceivedTime.date() == target_date:
                filtered.append(msg)
        except AttributeError:
            continue  # Skip items without ReceivedTime
    return filtered


def check_subjects_in_messages(subjects, messages, target_date, mailbox_name):
    """Check if given subjects are found in the message list"""
    results = []

    for subject in subjects:
        found = False
        for msg in messages:
            if subject.lower() in msg.Subject.lower():
                results.append([subject, target_date, msg.ReceivedTime, f"Found ({mailbox_name})"])
                found = True
                break
        if not found:
            results.append([subject, target_date, "N/A", f"Not Found in {mailbox_name}"])
    
    return results


# === MAIN SCRIPT ===

def main():
    target_date = get_target_date(date_option)
    messages = fetch_messages_from_shared_mailbox(shared_mailbox_name, target_date)
    results = check_subjects_in_messages(process_subjects, messages, target_date, shared_mailbox_name)

    # Export results to CSV
    df = pd.DataFrame(results, columns=["Subject", "Execution Date", "Mail Received Time", "Status"])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"email_check_{shared_mailbox_name.replace(' ', '_')}_{timestamp}.csv"
    df.to_csv(filename, index=False)
    print(f"\nResults saved to '{filename}'")


# === RUN ===
if __name__ == "__main__":
    main()