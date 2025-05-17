import requests
import time
import datetime
import json
import pandas as pd
import io # For creating CSV in memory
import matplotlib
matplotlib.use('Agg') #Use non-interactive backend
import matplotlib.pyplot as plt
import base64
from flask import Flask, request, render_template, send_file, url_for, redirect

# --- Configuration ---
TZKT_API_URL = "https://api.tzkt.io/v1"
REQUESTS_PER_SECOND = 5 # Reduced to be more conservative with rate limits
DELAY_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND
MAX_RETRIES = 3 # Maximum number of retries for failed requests
RETRY_DELAY = 5 # Seconds to wait between retries

# --- Flask App Setup ---
app = Flask(__name__)
# In a real app, use a proper secret key stored securely
app.secret_key = 'your_very_secret_key_here'

# --- Helper Functions (Adapted from previous script) ---

def fetch_transactions(address, start_date_iso, end_date_iso):
    """Fetches transaction operations from TzKT API with robust error handling and retries."""
    all_transactions = []
    last_id = None
    limit = 1000
    page_count = 0
    total_retries = 0

    print(f"Fetching transactions for {address} from {start_date_iso} to {end_date_iso}...") # Log for server console

    while True:
        retry_count = 0
        success = False
        
        while not success and retry_count < MAX_RETRIES:
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

                print(f"Requesting page {page_count + 1}...")
                response = requests.get(url, params=params)
                
                # Handle rate limiting and other errors
                if response.status_code == 429: # Rate limited
                    retry_count += 1
                    total_retries += 1
                    wait_time = RETRY_DELAY * retry_count # Exponential backoff
                    print(f"Rate limit hit. Waiting {wait_time} seconds... (Retry {retry_count}/{MAX_RETRIES})")
                    time.sleep(wait_time)
                    continue # Retry the same request
                elif response.status_code >= 400:
                    print(f"API Error: {response.status_code} - {response.text}")
                    if retry_count < MAX_RETRIES - 1:
                        retry_count += 1
                        total_retries += 1
                        wait_time = RETRY_DELAY * retry_count
                        print(f"Retrying in {wait_time} seconds... (Retry {retry_count}/{MAX_RETRIES})")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"Max retries reached for this request. Moving on with partial data.")
                        break # Exit retry loop but continue with what we have
                
                # Parse the response
                transactions = response.json()
                success = True
                
                if not transactions:
                    print("No more transactions found.")
                    break

                # Process the transactions
                count_in_batch = 0
                for tx in transactions:
                    tx_timestamp = tx.get('timestamp')
                    # Double check timestamp range (should be handled by API)
                    if tx_timestamp >= start_date_iso and tx_timestamp < end_date_iso:
                        all_transactions.append(tx)
                        count_in_batch += 1

                page_count += 1
                print(f"Fetched {count_in_batch} transactions in batch {page_count} (Total: {len(all_transactions)})")

                # Update last_id for pagination
                if transactions:
                    last_id = transactions[-1]['id']
                
                # Respect rate limits
                time.sleep(DELAY_BETWEEN_REQUESTS)
                
            except requests.exceptions.RequestException as e:
                print(f"Network error fetching data from TzKT: {e}")
                if retry_count < MAX_RETRIES - 1:
                    retry_count += 1
                    total_retries += 1
                    wait_time = RETRY_DELAY * retry_count
                    print(f"Retrying in {wait_time} seconds... (Retry {retry_count}/{MAX_RETRIES})")
                    time.sleep(wait_time)
                else:
                    print("Max retries reached for network error. Moving on with partial data.")
                    break
            except json.JSONDecodeError:
                print("Error decoding JSON response from TzKT.")
                if retry_count < MAX_RETRIES - 1:
                    retry_count += 1
                    total_retries += 1
                    wait_time = RETRY_DELAY * retry_count
                    print(f"Retrying in {wait_time} seconds... (Retry {retry_count}/{MAX_RETRIES})")
                    time.sleep(wait_time)
                else:
                    print("Max retries reached for JSON decode error. Moving on with partial data.")
                    break
            except Exception as e:
                print(f"An unexpected error occurred during fetch: {e}")
                if retry_count < MAX_RETRIES - 1:
                    retry_count += 1
                    total_retries += 1
                    wait_time = RETRY_DELAY * retry_count
                    print(f"Retrying in {wait_time} seconds... (Retry {retry_count}/{MAX_RETRIES})")
                    time.sleep(wait_time)
                else:
                    print("Max retries reached for unexpected error. Moving on with partial data.")
                    break
        
        # If we didn't get any transactions in this batch or reached max retries without success, exit the main loop
        if not success or not transactions:
            break

    print(f"Finished fetching. Total transactions: {len(all_transactions)}, Pages: {page_count}, Total retries: {total_retries}")
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
            "Address": target_address, # Add the target address
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

def calculate_daily_summary(df, addresses=None):
    """Calculates daily summaries with totals, potentially grouped by address."""
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
        subset = x[df_copy.loc[x.index, 'Direction'] == 'IN']
        return subset.sum() if not subset.empty else 0.0

    def sum_outgoing(x):
        subset = x[df_copy.loc[x.index, 'Direction'] == 'OUT']
        return subset.sum() if not subset.empty else 0.0

    mp_fees_address = 'tz1cY5tTfFb5c4Q9VyJ895y6eLk1ohXXqwVD'
    is_mp_fees_only_selected = addresses and len(addresses) == 1 and addresses[0] == mp_fees_address

    if is_mp_fees_only_selected:
        # Special handling for MP Fees address when it's the only one selected
        daily_summary = df_copy.groupby('Date').agg(
            Transactions=('Hash', 'count'),
            Skurpy_Cut=('Amount (XTZ)', sum_incoming),
        ).reset_index()

        if daily_summary.empty: # Added robustness: return empty if no data after grouping
            return pd.DataFrame()

        # Calculate Total Volume (Skurpy Cut is 3% of total)
        daily_summary['Total_Volume'] = (daily_summary['Skurpy_Cut'] / 0.03).round(6)

        # Format columns for better display
        daily_summary['Skurpy_Cut'] = daily_summary['Skurpy_Cut'].round(6)

        # Calculate totals
        totals = pd.DataFrame({
            'Date': ['TOTAL'],
            'Transactions': [daily_summary['Transactions'].sum()],
            'Skurpy_Cut': [daily_summary['Skurpy_Cut'].sum().round(6)],
            'Total_Volume': [daily_summary['Total_Volume'].sum().round(6)]
        })
        daily_summary = pd.concat([daily_summary, totals], ignore_index=True)

    elif addresses and len(addresses) > 1:
        # Handling for multiple addresses: group by Date and Address
        daily_summary = df_copy.groupby(['Date', 'Address']).agg(
            Transactions=('Hash', 'count'),
            XTZ_Received=('Amount (XTZ)', sum_incoming),
            XTZ_Sent=('Amount (XTZ)', sum_outgoing),
        ).reset_index()

        if daily_summary.empty: # Added robustness: return empty if no data after grouping
            return pd.DataFrame()

        daily_summary['Net XTZ Change'] = daily_summary['XTZ_Received'] - daily_summary['XTZ_Sent']
        daily_summary['XTZ_Received'] = daily_summary['XTZ_Received'].round(6)
        daily_summary['XTZ_Sent'] = daily_summary['XTZ_Sent'].round(6)
        daily_summary['Net XTZ Change'] = daily_summary['Net XTZ Change'].round(6)

        # Calculate totals per address
        address_totals = df_copy.groupby('Address').agg(
            Transactions=('Hash', 'count'),
            XTZ_Received=('Amount (XTZ)', sum_incoming),
            XTZ_Sent=('Amount (XTZ)', sum_outgoing),
        ).reset_index()
        address_totals['Net XTZ Change'] = address_totals['XTZ_Received'] - address_totals['XTZ_Sent']
        address_totals['Date'] = 'TOTAL' # Add a 'TOTAL' date for address totals
        address_totals['XTZ_Received'] = address_totals['XTZ_Received'].round(6)
        address_totals['XTZ_Sent'] = address_totals['XTZ_Sent'].round(6)
        address_totals['Net XTZ Change'] = address_totals['Net XTZ Change'].round(6)

        # Calculate grand total across all addresses and dates
        grand_total_metrics = df_copy.agg(
            Transactions=('Hash', 'count'),
            XTZ_Received=('Amount (XTZ)', sum_incoming),
            XTZ_Sent=('Amount (XTZ)', sum_outgoing),
        )

        # Safely get values using .get() with a default Series for robustness
        total_transactions = grand_total_metrics.get('Transactions', pd.Series([0])).iloc[0]
        total_received = grand_total_metrics.get('XTZ_Received', pd.Series([0.0])).iloc[0]
        total_sent = grand_total_metrics.get('XTZ_Sent', pd.Series([0.0])).iloc[0]

        grand_total_row = {
            'Date': 'GRAND TOTAL',
            'Address': '', # No address for grand total
            'Transactions': total_transactions,
            'XTZ_Received': total_received.round(6),
            'XTZ_Sent': total_sent.round(6),
            'Net XTZ Change': (total_received - total_sent).round(6)
        }

        grand_total_df = pd.DataFrame([grand_total_row])

        # Ensure columns are consistent before concatenating
        cols = ['Date', 'Address', 'Transactions', 'XTZ_Received', 'XTZ_Sent', 'Net XTZ Change']
        daily_summary = daily_summary[cols]
        address_totals = address_totals[cols]
        grand_total_df = grand_total_df[cols]

        # Concatenate daily summary by address, address totals, and grand total
        daily_summary = pd.concat([daily_summary, address_totals, grand_total_df], ignore_index=True)

    else:
        # Standard handling for a single address (not MP Fees)
        daily_summary = df_copy.groupby('Date').agg(
            Transactions=('Hash', 'count'),
            XTZ_Received=('Amount (XTZ)', sum_incoming),
            XTZ_Sent=('Amount (XTZ)', sum_outgoing),
        ).reset_index()

        if daily_summary.empty: # Added robustness: return empty if no data after grouping
            return pd.DataFrame()

        daily_summary['Net XTZ Change'] = daily_summary['XTZ_Received'] - daily_summary['XTZ_Sent']
        daily_summary['XTZ_Received'] = daily_summary['XTZ_Received'].round(6)
        daily_summary['XTZ_Sent'] = daily_summary['XTZ_Sent'].round(6)
        daily_summary['Net XTZ Change'] = daily_summary['Net XTZ Change'].round(6)

        # Calculate totals
        totals = pd.DataFrame({
            'Date': ['TOTAL'],
            'Transactions': [daily_summary['Transactions'].sum()],
            'XTZ_Received': [daily_summary['XTZ_Received'].sum().round(6)],
            'XTZ_Sent': [daily_summary['XTZ_Sent'].sum().round(6)],
            'Net XTZ Change': [(daily_summary['XTZ_Received'].sum() - daily_summary['XTZ_Sent'].sum()).round(6)]
        })
        daily_summary = pd.concat([daily_summary, totals], ignore_index=True)

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
        # Get selected address types and custom address
        selected_address_types = request.form.getlist('address_type')
        custom_address = request.form.get('tezos_address', '').strip()

        # Define the address mapping
        address_map = {
            'bank': 'KT1NkX98gNeFb3QVcpMs5r7pKUut1twg9DQd',
            'factory': 'KT1S6WCZrJdXFgT1zbVN9MmiPF1C9UMjeFzK',
            'marketplace': 'KT1J8ydKTxBL7ioUqSosrYNsZNq6XcoXkP9C',
            'auction': 'KT1CT8AjgBhzzUP1CwhW7EvNcsYStipGt5B4',
            'editions': 'KT19Mb31GqSumA3YhfdCkv1p1uAaFNR7w7gT',
            'mp_owner': 'tz1cY5tTfFb5c4Q9VyJ895y6eLk1ohXXqwVD',
            'factory_owner': 'tz1L6kFTx9N9TKGzUMCLJ7ZBgqFs6biRHQEd',
            'factory_v2': 'KT1Qf5nMU9KhS6eRCfrQznD72MuhiHQse5Cd',
            'contract_factory_v2': 'KT1Qf5nMU9KhS6eRCfrQznD72MuhiHQse5Cd' # Added Contract Factory V2
        }

        # Build the list of addresses to fetch data for
        addresses_to_fetch = []
        for addr_type in selected_address_types:
            if addr_type == 'custom' and custom_address:
                addresses_to_fetch.append(custom_address)
            elif addr_type in address_map:
                addresses_to_fetch.append(address_map[addr_type])

        # Remove duplicates and ensure at least one address is selected
        addresses_to_fetch = list(set(addresses_to_fetch))

        if not addresses_to_fetch:
            return render_template('index.html', error="Please select at least one Tezos address.")

        # Get date and time inputs
        start_dt_str = request.form['start_datetime']
        end_dt_str = request.form['end_datetime']

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

        # Fetch and Process Data for multiple addresses
        all_raw_transactions = []
        all_processed_dfs = []
        all_metrics = {}

        for address in addresses_to_fetch:
            raw_transactions = fetch_transactions(address, start_date_iso, end_date_iso)
            all_raw_transactions.extend(raw_transactions) # Concatenate raw transactions

            # Process data for each address separately to get individual metrics
            processed_df, metrics = process_data(raw_transactions, address)
            all_processed_dfs.append(processed_df)
            all_metrics[address] = metrics # Store metrics per address

        # Combine all processed dataframes
        combined_transactions_df = pd.concat(all_processed_dfs, ignore_index=True)

        # Calculate overall metrics from the combined dataframe
        overall_metrics = {
            "trades": len(combined_transactions_df),
            "volume_xtz": combined_transactions_df['Amount (XTZ)'].sum() if not combined_transactions_df.empty else 0.0,
            # Note: 'earned_xtz' is tricky for multiple addresses.
            # For now, we'll sum up the 'earned_xtz' from individual address metrics.
            # This assumes 'earned_xtz' in process_data correctly identifies incoming transactions *to that specific address*.
            "earned_xtz": sum(m['earned_xtz'] for m in all_metrics.values())
        }

        # Calculate daily summary from the combined dataframe, potentially grouped by address
        daily_summary_df = calculate_daily_summary(combined_transactions_df, addresses_to_fetch)

        # Check if any of the selected addresses is the MP Fees address for graph generation (only generate if MP Fees is the *only* address selected)
        mp_fees_address = address_map.get('mp_owner')
        is_mp_fees_only_selected = len(addresses_to_fetch) == 1 and addresses_to_fetch[0] == mp_fees_address

        # Generate line graph if data exists
        graph_data = None
        if not combined_transactions_df.empty:
            # Calculate daily summary for the graph, grouped by address if multiple are selected
            graph_daily_summary_df = calculate_daily_summary(combined_transactions_df, addresses_to_fetch)

            # Filter out total rows for plotting
            plot_data = graph_daily_summary_df[~graph_daily_summary_df['Date'].isin(['TOTAL', 'GRAND TOTAL'])]

            if not plot_data.empty and len(plot_data) > 1:
                plt.figure(figsize=(12, 6)) # Slightly larger figure for multiple lines

                # Convert Date to datetime for proper plotting
                plot_data['Date'] = pd.to_datetime(plot_data['Date'])

                # Sort by date
                plot_data = plot_data.sort_values('Date')

                # Determine the column to plot based on whether MP Fees is the only address
                plot_column = 'Skurpy_Cut' if is_mp_fees_only_selected else 'XTZ_Received' # Default to XTZ_Received for multiple/other addresses
                y_label = 'Skurpy Cut (XTZ)' if is_mp_fees_only_selected else 'XTZ Received (XTZ)'
                title = 'Skurpy Cut (3%) Over Time (MP Fees Address)' if is_mp_fees_only_selected else 'XTZ Received Over Time by Address'

                if 'Address' in plot_data.columns and len(addresses_to_fetch) > 1:
                    # Plot data for each address
                    for address in addresses_to_fetch:
                        address_plot_data = plot_data[plot_data['Address'] == address]
                        if not address_plot_data.empty:
                            plt.plot(address_plot_data['Date'], address_plot_data[plot_column], marker='o', linestyle='-', label=address)
                    plt.legend(title="Addresses", bbox_to_anchor=(1.05, 1), loc='upper left') # Add legend for multiple addresses
                    plt.subplots_adjust(right=0.75) # Adjust layout to make space for legend
                else:
                    # Plot for a single address (either MP Fees or another single address)
                    plt.plot(plot_data['Date'], plot_data[plot_column], marker='o', linestyle='-', color='#3498db')

                # Add labels and title
                plt.xlabel('Date')
                plt.ylabel(y_label)
                plt.title(title)
                plt.grid(True, linestyle='--', alpha=0.7)

                # Format the y-axis to show XTZ values clearly
                plt.ticklabel_format(style='plain', axis='y')

                # Rotate date labels for better readability
                plt.xticks(rotation=45)

                # Tight layout to ensure everything fits (adjusting for legend if present)
                plt.tight_layout()

                # Save the plot to a bytes buffer
                buf = io.BytesIO()
                plt.savefig(buf, format='png')
                buf.seek(0)

                # Convert the image to base64 string for embedding in HTML
                graph_data = base64.b64encode(buf.read()).decode('utf-8')

                # Close the plot to free memory
                plt.close()

        # Generate HTML table for preview (optional, limit rows for performance if needed)
        daily_summary_html = None
        has_data = not combined_transactions_df.empty
        if not daily_summary_df.empty:
            # We need to handle the case where the daily_summary_df includes address grouping
            if 'Address' in daily_summary_df.columns:
                # Separate regular rows, address total rows, and grand total row
                regular_rows = daily_summary_df[~daily_summary_df['Date'].isin(['TOTAL', 'GRAND TOTAL'])].head(50)
                address_total_rows = daily_summary_df[daily_summary_df['Date'] == 'TOTAL']
                grand_total_row = daily_summary_df[daily_summary_df['Date'] == 'GRAND TOTAL']

                # Generate HTML for each part
                regular_html = regular_rows.to_html(index=False, classes='table table-striped')
                address_total_html = address_total_rows.to_html(index=False, classes='table table-striped total-row') # Style address total rows
                grand_total_html = grand_total_row.to_html(index=False, classes='table total-row') # Style grand total row

                # Combine the HTML parts, removing redundant table tags and re-adding final closing tag
                daily_summary_html = regular_html.replace('</table>', '')
                if not address_total_rows.empty:
                    daily_summary_html += address_total_html.replace('<table border="1" class="table table-striped total-row">', '').replace('</table>', '')
                if not grand_total_row.empty:
                    daily_summary_html += grand_total_html.replace('<table border="1" class="table total-row">', '').replace('</table>', '')
                daily_summary_html += '</table>' # Re-add the closing table tag

            else:
                # If not grouped by address (only one address selected), use the previous logic
                if len(daily_summary_df) > 1:
                    regular_rows = daily_summary_df.iloc[:-1]
                    total_row = daily_summary_df.iloc[-1:]

                    regular_html = regular_rows.head(50).to_html(index=False, classes='table table-striped')
                    total_html = total_row.to_html(index=False, classes='table')

                    total_html = total_html.replace('<tr>', '<tr class="total-row">', 1)
                    total_html = total_html.split('</thead>')[1]

                    daily_summary_html = regular_html.replace('</table>', '') + total_html.replace('<table border="1" class="table">', '')
                    daily_summary_html += '</table>' # Re-add the closing table tag
                else:
                    daily_summary_html = daily_summary_df.to_html(index=False, classes='table table-striped')


        # Pass the list of addresses to the results template for display and download links
        return render_template(
            'results.html',
            addresses=addresses_to_fetch, # Pass list of addresses
            start_date_str=start_dt.strftime('%Y-%m-%d %H:%M'),
            end_date_str=end_dt.strftime('%Y-%m-%d %H:%M'),
            start_date_iso=start_date_iso, # Pass ISO strings for download links
            end_date_iso=end_date_iso,
            metrics=all_metrics, # Pass metrics per address
            overall_metrics=overall_metrics, # Pass overall metrics
            daily_summary_html=daily_summary_html,
            has_data=has_data,
            is_mp_fees=is_mp_fees_only_selected, # Indicate if MP Fees was *only* selected
            graph_data=graph_data
        )

    except Exception as e:
        print(f"Error in /results route: {e}") # Log the error
        # Redirect back to index with a generic error, or show a dedicated error page
        return render_template('index.html', error=f"An unexpected error occurred: {e}")


@app.route('/download/<type>')
def download_csv(type):
    """Generates and serves the requested CSV file."""
    try:
        # Get addresses from query parameters (they will be comma-separated)
        addresses_str = request.args.get('addresses')
        start_date_iso = request.args.get('start')
        end_date_iso = request.args.get('end')

        if not all([addresses_str, start_date_iso, end_date_iso]):
            return "Error: Missing required parameters for download.", 400

        addresses_to_fetch = addresses_str.split(',')

        # Regenerate the data for the download request for multiple addresses
        all_raw_transactions = []
        all_processed_dfs = []

        for address in addresses_to_fetch:
            raw_transactions = fetch_transactions(address, start_date_iso, end_date_iso)
            all_raw_transactions.extend(raw_transactions) # Concatenate raw transactions

            # Process data for each address separately (needed for 'Direction' logic in process_data)
            processed_df, _ = process_data(raw_transactions, address)
            all_processed_dfs.append(processed_df)

        # Combine all processed dataframes
        combined_transactions_df = pd.concat(all_processed_dfs, ignore_index=True)
        transactions_df = combined_transactions_df # Rename for clarity for the rest of this function

        if type == 'transactions':
            if transactions_df.empty:
                return "No transaction data found to download.", 404
            df_to_download = transactions_df
            
            # Create a shortened string of addresses for the filename
            address_list_str = '_'.join([addr[:8] for addr in addresses_to_fetch]) # Shorten addresses for filename
            filename = f"transactions_{address_list_str}_{start_date_iso[:10]}_to_{end_date_iso[:10]}.csv"

        elif type == 'daily_summary':
            # Calculate daily summary from the combined dataframe, passing addresses_to_fetch
            daily_summary_df = calculate_daily_summary(transactions_df, addresses_to_fetch)

            if daily_summary_df.empty:
                return "No daily summary data found to download.", 404
            
            df_to_download = daily_summary_df

            address_list_str = '_'.join([addr[:8] for addr in addresses_to_fetch])
            filename = f"combined_{address_list_str}_daily_summary_with_totals_{start_date_iso[:10]}_to_{end_date_iso[:10]}.csv"

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
    app.run(host='0.0.0.0', port=5000)