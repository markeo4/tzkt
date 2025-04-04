import requests
import time
import datetime
import json
import pandas as pd
import io # For creating CSV in memory
from flask import Flask, request, render_template, send_file, url_for, redirect

# --- Configuration ---
TZKT_API_URL = "https://api.tzkt.io/v1"
REQUESTS_PER_SECOND = 10 # TzKT free tier limit
DELAY_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND

# --- Flask App Setup ---
app = Flask(__name__)
# In a real app, use a proper secret key stored securely
app.secret_key = 'your_very_secret_key_here'

# --- Helper Functions (Adapted from previous script) ---

def fetch_transactions(address, start_date_iso, end_date_iso):
    """Fetches transaction operations from TzKT API."""
    all_transactions = []
    last_id = None
    limit = 1000

    print(f"Fetching transactions for {address} from {start_date_iso} to {end_date_iso}...") # Log for server console

    while True:
        try:
            url = f"{TZKT_API_URL}/accounts/{address}/operations"
            params = {
                "type": "transaction",
                "timestamp.ge": start_date_iso,
                "timestamp.lt": end_date_iso,
                "status": "applied",
                "limit": limit,
                "sort.desc": "id",
            }
            if last_id:
                params["lastId"] = last_id

            response = requests.get(url, params=params)
            # Don't raise for status here, handle potential errors below
            if response.status_code == 429: # Rate limited
                 print("Rate limit hit. Waiting 5 seconds...")
                 time.sleep(5)
                 continue # Retry the same request
            elif response.status_code >= 400:
                 print(f"API Error: {response.status_code} - {response.text}")
                 # Return what we have, or an empty list, or raise an exception
                 # For this web app, let's return what we have and potentially show partial results
                 break # Exit loop on other errors for now
            # response.raise_for_status() # Optional: Raise for other errors if preferred

            transactions = response.json()

            if not transactions:
                break

            count_in_batch = 0
            for tx in transactions:
                tx_timestamp = tx.get('timestamp')
                # Double check timestamp range (should be handled by API)
                if tx_timestamp >= start_date_iso and tx_timestamp < end_date_iso:
                    all_transactions.append(tx)
                    count_in_batch += 1

            print(f"Fetched {count_in_batch} transactions in batch (Total: {len(all_transactions)})")

            last_id = transactions[-1]['id']
            time.sleep(DELAY_BETWEEN_REQUESTS)

        except requests.exceptions.RequestException as e:
            print(f"Network error fetching data from TzKT: {e}")
            # Decide how to handle: break, retry, raise...
            break # Exit loop on network errors for now
        except json.JSONDecodeError:
            print("Error decoding JSON response from TzKT.")
            break
        except Exception as e:
            print(f"An unexpected error occurred during fetch: {e}")
            break # Prevent potential infinite loops on unexpected errors

    print(f"Finished fetching. Total transactions: {len(all_transactions)}")
    return all_transactions


def process_data(transactions, target_address):
    """Processes raw transactions into a DataFrame and calculates metrics."""
    if not transactions:
        return pd.DataFrame(), {"trades": 0, "volume_xtz": 0.0, "earned_xtz": 0.0}

    processed_data = []
    total_earned_mutez = 0
    total_volume_mutez = 0
    trade_count = 0

    for tx in transactions:
        # Ensure required fields exist and handle potential missing data gracefully
        amount_mutez = tx.get('amount', 0)
        sender = tx.get('sender', {}).get('address')
        target = tx.get('target', {}).get('address')
        timestamp_str = tx.get('timestamp')
        tx_hash = tx.get('hash', 'N/A') # Default if hash is missing

        if timestamp_str is None:
            print(f"Warning: Transaction missing timestamp (Hash: {tx_hash}). Skipping.")
            continue # Skip transactions without a timestamp

        try:
            timestamp = pd.to_datetime(timestamp_str)
        except Exception as e:
            print(f"Warning: Could not parse timestamp '{timestamp_str}' (Hash: {tx_hash}). Skipping. Error: {e}")
            continue

        is_outgoing = sender == target_address
        is_incoming = target == target_address

        trade_count += 1
        total_volume_mutez += amount_mutez

        if is_incoming:
            total_earned_mutez += amount_mutez
            direction = "IN"
        elif is_outgoing:
            direction = "OUT"
        else:
            direction = "UNKNOWN"

        processed_data.append({
            "Timestamp": timestamp,
            "Hash": tx_hash,
            "Direction": direction,
            "From": sender or "N/A", # Handle missing sender/target
            "To": target or "N/A",
            "Amount (XTZ)": amount_mutez / 1_000_000 if amount_mutez else 0.0,
            "Amount (mutez)": amount_mutez,
        })

    df = pd.DataFrame(processed_data)
    if not df.empty:
        df = df.sort_values(by="Timestamp").reset_index(drop=True)

    metrics = {
        "trades": trade_count,
        "volume_xtz": total_volume_mutez / 1_000_000,
        "earned_xtz": total_earned_mutez / 1_000_000,
    }
    return df, metrics

def calculate_daily_summary(df):
    """Calculates daily summaries."""
    if df.empty or 'Timestamp' not in df.columns:
        return pd.DataFrame()

    df_copy = df.copy()
    # Ensure Timestamp is datetime type
    df_copy['Timestamp'] = pd.to_datetime(df_copy['Timestamp'], errors='coerce')
    df_copy = df_copy.dropna(subset=['Timestamp']) # Remove rows where conversion failed

    if df_copy.empty:
        return pd.DataFrame()

    df_copy['Date'] = df_copy['Timestamp'].dt.date
    df_copy['Amount (XTZ)'] = pd.to_numeric(df_copy['Amount (XTZ)'], errors='coerce').fillna(0)

    # Define aggregation functions safely
    def sum_incoming(x):
        return x[df_copy.loc[x.index, 'Direction'] == 'IN'].sum()

    def sum_outgoing(x):
         return x[df_copy.loc[x.index, 'Direction'] == 'OUT'].sum()

    daily_summary = df_copy.groupby('Date').agg(
        Transactions=('Hash', 'count'),
        XTZ_Received=('Amount (XTZ)', sum_incoming),
        XTZ_Sent=('Amount (XTZ)', sum_outgoing),
    ).reset_index()

    daily_summary['Net XTZ Change'] = daily_summary['XTZ_Received'] - daily_summary['XTZ_Sent']
    # Format columns for better display if needed
    daily_summary['XTZ_Received'] = daily_summary['XTZ_Received'].round(6)
    daily_summary['XTZ_Sent'] = daily_summary['XTZ_Sent'].round(6)
    daily_summary['Net XTZ Change'] = daily_summary['Net XTZ Change'].round(6)


    return daily_summary

# --- Flask Routes ---

@app.route('/')
def index():
    """Display the input form."""
    return render_template('index.html')

@app.route('/results', methods=['POST'])
def handle_results():
    """Process form data, fetch data, and display results."""
    try:
        address = request.form['tezos_address']
        start_dt_str = request.form['start_datetime']
        end_dt_str = request.form['end_datetime']

        # Validate inputs
        if not address or not address.startswith('tz'):
            return render_template('index.html', error="Invalid Tezos address.")

        # Convert HTML datetime-local format to ISO 8601 Z format for API
        try:
            start_dt = datetime.datetime.fromisoformat(start_dt_str)
            end_dt = datetime.datetime.fromisoformat(end_dt_str)
            start_date_iso = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_date_iso = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
             return render_template('index.html', error="Invalid date/time format.")

        if start_dt >= end_dt:
            return render_template('index.html', error="End date/time must be after start date/time.")

        # Fetch and Process Data
        raw_transactions = fetch_transactions(address, start_date_iso, end_date_iso)
        transactions_df, metrics = process_data(raw_transactions, address)
        daily_summary_df = calculate_daily_summary(transactions_df)

        # Generate HTML table for preview (optional, limit rows for performance if needed)
        daily_summary_html = None
        has_data = not transactions_df.empty
        if not daily_summary_df.empty:
             daily_summary_html = daily_summary_df.head(50).to_html(index=False, classes='table table-striped') # Show first 50 rows

        return render_template(
            'results.html',
            address=address,
            start_date_str=start_dt.strftime('%Y-%m-%d %H:%M'),
            end_date_str=end_dt.strftime('%Y-%m-%d %H:%M'),
            start_date_iso=start_date_iso, # Pass ISO strings for download links
            end_date_iso=end_date_iso,
            metrics=metrics,
            daily_summary_html=daily_summary_html,
            has_data=has_data
        )

    except Exception as e:
        print(f"Error in /results route: {e}") # Log the error
        # Redirect back to index with a generic error, or show a dedicated error page
        return render_template('index.html', error=f"An unexpected error occurred: {e}")


@app.route('/download/<type>')
def download_csv(type):
    """Generates and serves the requested CSV file."""
    try:
        address = request.args.get('address')
        start_date_iso = request.args.get('start')
        end_date_iso = request.args.get('end')

        if not all([address, start_date_iso, end_date_iso]):
            return "Error: Missing required parameters for download.", 400

        # Regenerate the data for the download request
        # This avoids storing large data in sessions or globals but hits the API again.
        raw_transactions = fetch_transactions(address, start_date_iso, end_date_iso)
        transactions_df, _ = process_data(raw_transactions, address) # We only need the DF here

        if type == 'transactions':
            if transactions_df.empty:
                return "No transaction data found to download.", 404
            df_to_download = transactions_df
            filename = f"{address}_transactions_{start_date_iso[:10]}_to_{end_date_iso[:10]}.csv"

        elif type == 'daily_summary':
            daily_summary_df = calculate_daily_summary(transactions_df)
            if daily_summary_df.empty:
                 return "No daily summary data found to download.", 404
            df_to_download = daily_summary_df
            filename = f"{address}_daily_summary_{start_date_iso[:10]}_to_{end_date_iso[:10]}.csv"

        else:
            return "Invalid download type.", 400

        # Create CSV in memory
        output = io.StringIO()
        df_to_download.to_csv(output, index=False)
        output.seek(0) # Rewind the buffer

        # Send the file
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')), # Send as BytesIO
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename # Use download_name (formerly attachment_filename)
        )

    except Exception as e:
         print(f"Error in /download route: {e}") # Log the error
         return f"An error occurred generating the download: {e}", 500


# --- Run the App ---
if __name__ == '__main__':
    # Use host='0.0.0.0' to make it accessible on your network
    # Debug=True is helpful during development but SHOULD BE FALSE for production
    app.run(debug=True, host='0.0.0.0', port=5000)