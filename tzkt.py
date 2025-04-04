import requests
import time
import datetime
import json
import pandas as pd
import os # Used for environment variables if storing API keys securely

# --- Configuration ---
TEZOS_ADDRESS = "tz1cY5tTfFb5c4Q9VyJ895y6eLk1ohXXqwVD"
TZKT_API_URL = "https://api.tzkt.io/v1"
REQUESTS_PER_SECOND = 10
DELAY_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND

# Specify the time range (inclusive start, exclusive end)
# Use ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ) or just YYYY-MM-DD
# Example: All transactions from Jan 1, 2024 up to (but not including) April 4, 2025
START_DATE_STR = "2024-01-01T00:00:00Z"
END_DATE_STR = "2025-04-04T00:00:00Z" # Fetch up to this date

# --- Output Configuration ---
OUTPUT_METHOD = "csv" # Options: "csv", "google_sheets", "airtable"
CSV_FILENAME = f"{TEZOS_ADDRESS}_transactions_{START_DATE_STR.split('T')[0]}_to_{END_DATE_STR.split('T')[0]}.csv"
DAILY_CSV_FILENAME = f"{TEZOS_ADDRESS}_daily_summary_{START_DATE_STR.split('T')[0]}_to_{END_DATE_STR.split('T')[0]}.csv"


# --- Google Sheets Configuration (Requires gspread and google-auth) ---
# pip install gspread google-auth-oauthlib google-auth-httplib2
# GOOGLE_SHEET_NAME = "Your Google Sheet Name"
# GOOGLE_WORKSHEET_NAME = "Tezos Transactions"
# GOOGLE_DAILY_WORKSHEET_NAME = "Tezos Daily Summary"
# GOOGLE_CREDENTIALS_FILE = "path/to/your/credentials.json" # Download from Google Cloud Console

# --- Airtable Configuration (Requires pyairtable) ---
# pip install pyairtable
# AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY") # Store API key securely
# AIRTABLE_BASE_ID = "YOUR_BASE_ID"
# AIRTABLE_TABLE_NAME = "Tezos Transactions"
# AIRTABLE_DAILY_TABLE_NAME = "Tezos Daily Summary"
# if not AIRTABLE_API_KEY:
#     print("Warning: AIRTABLE_API_KEY environment variable not set.")

# --- Helper Functions ---

def fetch_transactions(address, start_date, end_date):
    """Fetches all transaction operations for a given address within a date range from TzKT."""
    all_transactions = []
    last_id = None
    limit = 1000 # Max limit per request for operations endpoint

    print(f"Fetching transactions for {address} from {start_date} to {end_date}...")

    while True:
        try:
            # Construct the API request URL
            # We filter by type 'transaction', timestamp range, and sort by ID descending
            # The /accounts/{address}/operations endpoint gets operations where the address is sender OR target
            url = f"{TZKT_API_URL}/accounts/{address}/operations"
            params = {
                "type": "transaction",
                "timestamp.ge": start_date, # Greater than or equal to start_date
                "timestamp.lt": end_date,    # Less than end_date
                "status": "applied",         # Only successful transactions
                "limit": limit,
                "sort.desc": "id",           # Process newest first, useful for stopping early
            }
            # Use lastId for pagination after the first request
            if last_id:
                params["lastId"] = last_id

            # Make the request
            response = requests.get(url, params=params)
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

            transactions = response.json()

            if not transactions:
                print("No more transactions found in the range.")
                break # Exit loop if no more transactions are returned

            # Filter again just in case (though API params should handle it)
            # and add to our list
            count_in_batch = 0
            for tx in transactions:
                tx_timestamp = tx.get('timestamp')
                # Double check timestamp is within range (API should handle this but good practice)
                if tx_timestamp >= start_date and tx_timestamp < end_date:
                    all_transactions.append(tx)
                    count_in_batch += 1
                # Since we sort descending, if we hit a tx before our start date,
                # we can potentially stop if NOT using lastId pagination, but with lastId, we must continue
                # until the API returns nothing, as older items might appear later in rare cases.

            print(f"Fetched {count_in_batch} transactions in this batch (Total: {len(all_transactions)}). Last timestamp: {transactions[-1].get('timestamp')}")

            # Get the ID of the last transaction in the batch for the next request's pagination
            last_id = transactions[-1]['id']

            # Rate Limiting
            time.sleep(DELAY_BETWEEN_REQUESTS)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from TzKT: {e}")
            # Implement retry logic here if desired
            break
        except json.JSONDecodeError:
            print("Error decoding JSON response from TzKT.")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    print(f"Finished fetching. Total transactions retrieved: {len(all_transactions)}")
    return all_transactions

def process_data(transactions, target_address):
    """Processes raw transaction data into a pandas DataFrame and calculates metrics."""
    if not transactions:
        return pd.DataFrame(), {"trades": 0, "volume_xtz": 0.0, "earned_xtz": 0.0}

    processed_data = []
    total_earned_mutez = 0
    total_volume_mutez = 0 # Volume includes both sent and received
    trade_count = 0

    for tx in transactions:
        amount_mutez = tx.get('amount', 0)
        sender = tx.get('sender', {}).get('address')
        target = tx.get('target', {}).get('address')
        timestamp = pd.to_datetime(tx.get('timestamp'))
        tx_hash = tx.get('hash')

        is_outgoing = sender == target_address
        is_incoming = target == target_address

        # Define a "trade" - here assuming any transfer involving the address
        # Adjust this definition if needed (e.g., only interactions with DEX contracts)
        trade_count += 1
        total_volume_mutez += amount_mutez

        if is_incoming:
            total_earned_mutez += amount_mutez
            direction = "IN"
        elif is_outgoing:
            direction = "OUT"
        else:
            # Should not happen with /accounts/{address}/operations, but good to check
            direction = "UNKNOWN"

        processed_data.append({
            "Timestamp": timestamp,
            "Hash": tx_hash,
            "Direction": direction,
            "From": sender,
            "To": target,
            "Amount (XTZ)": amount_mutez / 1_000_000 if amount_mutez else 0.0,
            "Amount (mutez)": amount_mutez,
            # Add more fields if needed (e.g., parameters for contract calls)
            # "Parameters": tx.get('parameter')
        })

    df = pd.DataFrame(processed_data)
    # Sort by timestamp ascending
    if not df.empty:
        df = df.sort_values(by="Timestamp").reset_index(drop=True)

    metrics = {
        "trades": trade_count,
        "volume_xtz": total_volume_mutez / 1_000_000,
        "earned_xtz": total_earned_mutez / 1_000_000,
    }

    return df, metrics

def calculate_daily_summary(df):
    """Calculates daily summaries from the transaction DataFrame."""
    if df.empty:
        return pd.DataFrame()

    df_copy = df.copy()
    df_copy['Date'] = df_copy['Timestamp'].dt.date

    # Ensure 'Amount (XTZ)' is numeric
    df_copy['Amount (XTZ)'] = pd.to_numeric(df_copy['Amount (XTZ)'], errors='coerce')

    daily_summary = df_copy.groupby('Date').agg(
        Transactions=('Hash', 'count'),
        XTZ_Received=('Amount (XTZ)', lambda x: x[df_copy.loc[x.index, 'Direction'] == 'IN'].sum()),
        XTZ_Sent=('Amount (XTZ)', lambda x: x[df_copy.loc[x.index, 'Direction'] == 'OUT'].sum()),
    ).reset_index()

    daily_summary['Net XTZ Change'] = daily_summary['XTZ_Received'] - daily_summary['XTZ_Sent']

    return daily_summary

# --- Output Functions ---

def save_to_csv(df, filename):
    """Saves a DataFrame to a CSV file."""
    try:
        df.to_csv(filename, index=False)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving data to CSV {filename}: {e}")

# --- Google Sheets Functions (Placeholder) ---
# def save_to_google_sheets(df, sheet_name, worksheet_name, credentials_file):
#     """Saves a DataFrame to a Google Sheet."""
#     print(f"Attempting to save data to Google Sheet: {sheet_name}/{worksheet_name}...")
#     print("NOTE: Google Sheets integration requires authentication setup.")
#     try:
#         import gspread
#         from google.oauth2.service_account import Credentials
#         # You might need gspread-dataframe for easier df writing
#         # from gspread_dataframe import set_with_dataframe
#
#         scopes = ["https://www.googleapis.com/auth/spreadsheets"]
#         creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
#         gc = gspread.authorize(creds)
#
#         sh = gc.open(sheet_name)
#         try:
#             worksheet = sh.worksheet(worksheet_name)
#         except gspread.WorksheetNotFound:
#             worksheet = sh.add_worksheet(title=worksheet_name, rows="1", cols="1") # Start small
#
#         # Clear existing data (optional)
#         # worksheet.clear()
#
#         # Write header + data
#         # For large data, consider batch updates or gspread-dataframe
#         worksheet.update([df.columns.values.tolist()] + df.values.tolist())
#         # Or using gspread-dataframe:
#         # set_with_dataframe(worksheet, df)
#
#         print(f"Data successfully saved to Google Sheet: {sheet_name}/{worksheet_name}")
#
#     except ImportError:
#         print("Error: 'gspread' or 'google-auth' libraries not found.")
#         print("Please install them: pip install gspread google-auth-oauthlib google-auth-httplib2")
#     except Exception as e:
#         print(f"Error saving to Google Sheets: {e}")
#         print("Ensure credentials file is correct and the service account has permissions.")

# --- Airtable Functions (Placeholder) ---
# def save_to_airtable(df, api_key, base_id, table_name):
#     """Saves a DataFrame to an Airtable table."""
#     print(f"Attempting to save data to Airtable Base: {base_id}, Table: {table_name}...")
#     print("NOTE: Airtable integration requires API key and correct Base/Table IDs.")
#     try:
#         from pyairtable import Table
#
#         if not api_key:
#             print("Error: Airtable API key not configured.")
#             return
#
#         # Convert DataFrame to list of dictionaries suitable for Airtable
#         # Ensure column names in DataFrame match Airtable field names EXACTLY
#         # Airtable API might have limits on batch create size (e.g., 10 records per request batch)
#         # Handle date/datetime conversion to string format Airtable expects (ISO 8601)
#         records_to_create = []
#         for index, row in df.iterrows():
#             record = row.to_dict()
#             # Convert Timestamp to ISO 8601 string if it's a datetime object
#             if 'Timestamp' in record and isinstance(record['Timestamp'], (datetime.datetime, pd.Timestamp)):
#                  record['Timestamp'] = record['Timestamp'].isoformat()
#             # Ensure field names match Airtable exactly
#             records_to_create.append({'fields': record})
#
#         table = Table(api_key, base_id, table_name)
#
#         # Airtable's batch_create usually takes a list of dictionaries like {'fields': {...}}
#         # Consider batching if df is large (e.g., 10 records at a time)
#         # table.batch_create(records_to_create)
#
#         # Simple loop for demonstration (less efficient for large data)
#         created_count = 0
#         for record in records_to_create:
#              try:
#                  table.create(record['fields'])
#                  created_count += 1
#              except Exception as e:
#                  print(f"Error creating record in Airtable: {e} - Data: {record['fields']}")
#                  # Optional: add failed record to a list for retry/logging
#
#         print(f"Successfully created {created_count} records in Airtable: {table_name}")
#
#     except ImportError:
#         print("Error: 'pyairtable' library not found.")
#         print("Please install it: pip install pyairtable")
#     except Exception as e:
#         print(f"Error saving to Airtable: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    print("Starting Tezos transaction data fetcher...")
    print(f"Account: {TEZOS_ADDRESS}")
    print(f"Time Range: {START_DATE_STR} to {END_DATE_STR}")
    print(f"Rate Limit: {REQUESTS_PER_SECOND} req/sec (Delay: {DELAY_BETWEEN_REQUESTS:.2f}s)")
    print(f"Output Method: {OUTPUT_METHOD}")

    # Fetch raw transaction data
    raw_transactions = fetch_transactions(TEZOS_ADDRESS, START_DATE_STR, END_DATE_STR)

    if raw_transactions:
        # Process data and calculate overall metrics
        transactions_df, metrics = process_data(raw_transactions, TEZOS_ADDRESS)

        # Calculate daily summary
        daily_summary_df = calculate_daily_summary(transactions_df)

        print("\n--- Metrics ---")
        print(f"Total Transactions Processed: {metrics['trades']}")
        print(f"Total Volume (Sent + Received): {metrics['volume_xtz']:.6f} XTZ")
        print(f"Total Tezos Earned (Incoming): {metrics['earned_xtz']:.6f} XTZ")

        # --- Output Data ---
        print("\n--- Saving Data ---")
        if not transactions_df.empty:
            if OUTPUT_METHOD == "csv":
                save_to_csv(transactions_df, CSV_FILENAME)
                if not daily_summary_df.empty:
                    save_to_csv(daily_summary_df, DAILY_CSV_FILENAME)
            elif OUTPUT_METHOD == "google_sheets":
                # save_to_google_sheets(transactions_df, GOOGLE_SHEET_NAME, GOOGLE_WORKSHEET_NAME, GOOGLE_CREDENTIALS_FILE)
                # if not daily_summary_df.empty:
                #     save_to_google_sheets(daily_summary_df, GOOGLE_SHEET_NAME, GOOGLE_DAILY_WORKSHEET_NAME, GOOGLE_CREDENTIALS_FILE)
                print("Google Sheets output selected - uncomment and configure the relevant code section.")
            elif OUTPUT_METHOD == "airtable":
                # if AIRTABLE_API_KEY:
                #     save_to_airtable(transactions_df, AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
                #     if not daily_summary_df.empty:
                #         save_to_airtable(daily_summary_df, AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_DAILY_TABLE_NAME)
                # else:
                #      print("Airtable output selected, but API Key is missing.")
                print("Airtable output selected - uncomment and configure the relevant code section.")

            else:
                print(f"Unknown output method: {OUTPUT_METHOD}")
        else:
            print("No transaction data processed, skipping output.")

    else:
        print("No transactions found for the specified criteria.")

    print("\nScript finished.")