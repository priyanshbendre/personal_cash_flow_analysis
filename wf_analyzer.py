import pandas as pd
import json
import os
import sys

# --- Configuration ---
# Define file paths used by the script
csv_new_raw_file = 'WF_march_april_25.csv'  # Path to the *new* raw CSV file containing transactions to be processed.
processed_csv_file = 'processed_transactions.csv' # Path to the file where processed data is stored/appended.
json_config_file = 'config.json'          # Path to the JSON configuration file for vendor patterns and investment categories.

# --- Function to process raw transactions ---
# This function encapsulates the core logic for transforming raw transaction data.
def process_raw_transactions(raw_df, patterns_wf, cash_investments):
    """
    Applies processing steps to a raw transactions DataFrame:
    - Attempts to drop specified extra columns based on expected raw file structure.
    - Renames the relevant columns to a standard format ('date', 'amount', 'description').
    - Identifies the vendor for each transaction based on description patterns from config.
    - Determines the cash flow category ('cash_in', 'cash_out', 'cash_investments') based on amount and vendor.
    - Ensures 'amount' is numeric and handles errors.

    Args:
        raw_df (pd.DataFrame): DataFrame loaded directly from the raw CSV file.
                                Expected to have at least 3 columns.
        patterns_wf (dict): Dictionary mapping vendors to description patterns.
        cash_investments (list): List of vendors considered as cash investments.

    Returns:
        pd.DataFrame: Processed DataFrame with 'date', 'amount', 'description',
                      'vendors', and 'cash_flow' columns. Returns an empty DataFrame
                      if processing fails critically (e.g., insufficient columns).
    """
    print("Processing raw transaction data...")

    # --- Column Handling ---
    # Based on the original code's header=None and column dropping logic,
    # it assumes the raw CSV has extra columns after description (indices 2 and 3).
    # We check if these columns exist before attempting to drop them.
    cols_to_drop_indices = [2, 3] # Indices of columns to attempt to drop (0-indexed)
    existing_cols_to_drop = [col for col in cols_to_drop_indices if col < raw_df.shape[1]]

    if existing_cols_to_drop:
        print(f"Dropping columns at indices: {existing_cols_to_drop}")
        raw_df = raw_df.drop(columns=existing_cols_to_drop)

    # Rename the remaining columns. Assuming the first three remaining columns are
    # date, amount, and description in that order.
    expected_renamed_cols = ['date', 'amount', 'description']
    if raw_df.shape[1] >= len(expected_renamed_cols):
        raw_df.columns = expected_renamed_cols[:raw_df.shape[1]]
        print(f"Renamed columns to: {raw_df.columns.tolist()}")
    else:
         print(f"Error: Raw data does not have at least {len(expected_renamed_cols)} columns after dropping. Cannot rename.")
         return pd.DataFrame() # Return empty DataFrame if columns are insufficient


    # --- Data Type Conversion and Cleaning ---
    # Ensure the 'amount' column is numeric. Coerce errors will turn invalid parsing into NaN.
    raw_df['amount'] = pd.to_numeric(raw_df['amount'], errors='coerce')
    # Drop rows where the 'amount' could not be converted to numeric, as they are likely invalid transactions.
    initial_rows = len(raw_df)
    raw_df.dropna(subset=['amount'], inplace=True)
    if len(raw_df) < initial_rows:
        print(f"Warning: Removed {initial_rows - len(raw_df)} row(s) due to non-numeric amount values.")


    # --- Vendor Identification Function ---
    def find_vendor(description):
        # Ensure description is a string and handle potential NaN values gracefully.
        # Convert to lower case for case-insensitive pattern matching.
        description_str = str(description).lower() if pd.notna(description) else ""

        # Iterate through defined vendors and their patterns.
        for vendor, patterns in patterns_wf.items():
             # Ensure the patterns value is a list before iterating through patterns.
            if isinstance(patterns, list):
                # Check if any of the vendor's patterns are in the transaction description.
                # Convert each pattern to string and lower case for matching.
                if any(str(pattern).lower() in description_str for pattern in patterns):
                    return vendor # Return the first matching vendor

        return 'Other' # Return 'Other' if no pattern matches


    # --- Apply Vendor Identification ---
    print("Identifying vendors...")
    raw_df['vendors'] = raw_df['description'].apply(find_vendor)


    # --- Cash Flow Classification Function ---
    def determine_cash_flow(amount, vendor):
        # Use the already cleaned and numeric amount
        amount_float = float(amount)

        if amount_float >= 0.0:
            return 'cash_in' # Positive amount indicates cash coming in
        elif vendor in cash_investments:
            return 'cash_investments' # Negative amount for a specified investment vendor
        else:
            return 'cash_out' # Negative amount for other vendors


    # --- Apply Cash Flow Classification ---
    print("Classifying cash flow...")
    raw_df['cash_flow'] = raw_df.apply(lambda row: determine_cash_flow(row['amount'], row['vendors']), axis=1)

    print("Raw data processing complete.")
    # Return the DataFrame with the final, expected columns.
    return raw_df[['date', 'amount', 'description', 'vendors', 'cash_flow']]

# --- Main Script Execution ---

print("Starting transaction processing script...")

# Load configuration file (patterns and investment list)
try:
    print(f"Loading configuration from '{json_config_file}'...")
    with open(json_config_file, 'r') as f:
        patterns_data = json.load(f)
    # Use .get() with default empty values for safety if keys are missing in config
    patterns_wf = patterns_data.get('patterns_wf', {})
    cash_investments = patterns_data.get('cash_investments', [])
    if not patterns_wf:
         print("Warning: 'patterns_wf' is missing or empty in config.json. Vendor identification may not work as expected.")
    if not cash_investments:
         print("Warning: 'cash_investments' is missing or empty in config.json. Cash investment classification may not work as expected.")

except FileNotFoundError:
    print(f"Error: Configuration file '{json_config_file}' not found.")
    sys.exit(1) # Exit if config is missing
except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from '{json_config_file}'. Please check the file format for syntax errors.")
    sys.exit(1) # Exit if config is invalid JSON
except Exception as e:
    print(f"An unexpected error occurred while loading the config file: {e}")
    sys.exit(1) # Exit for any other config loading error


# Check if the processed data file already exists
if os.path.exists(processed_csv_file):
    print(f"\nProcessed data file '{processed_csv_file}' found. Reading existing data...")
    try:
        # Read the existing processed data
        df_existing = pd.read_csv(processed_csv_file)
        print(f"Successfully read {len(df_existing)} existing records.")

        # Define columns to use for identifying duplicate transactions
        key_cols = ['date', 'amount', 'description']
        # Check if the existing DataFrame has the necessary columns for duplicate detection
        if not all(col in df_existing.columns for col in key_cols):
             print(f"Error: Existing file '{processed_csv_file}' is missing one or more required columns for duplicate checking ({key_cols}). Please ensure the file has the correct structure.")
             sys.exit(1)

        # Create a unique key string for each row in the existing data for easy comparison
        # Convert columns to string first to avoid type/format issues during concatenation
        df_existing['_key'] = df_existing[key_cols].astype(str).agg('_'.join, axis=1)
        # print(f"Created unique keys for existing data. Example key: {df_existing['_key'].iloc[0] if not df_existing.empty else 'N/A'}") # Debug print


    except Exception as e:
        print(f"An error occurred while reading or preparing the existing file '{processed_csv_file}': {e}")
        sys.exit(1) # Exit if unable to read or process the existing file

    # Read the new raw CSV file containing new transactions
    print(f"\nReading new raw data from '{csv_new_raw_file}'...")
    try:
        df_new_raw = pd.read_csv(csv_new_raw_file, header=None)
        print(f"Successfully read {len(df_new_raw)} raw records from new file.")
    except FileNotFoundError:
        print(f"Error: New CSV file '{csv_new_raw_file}' not found.")
        print("No new data to process. Exiting.")
        sys.exit(0) # Exit gracefully if the new file is missing
    except Exception as e:
        print(f"An error occurred while reading the new raw file '{csv_new_raw_file}': {e}")
        sys.exit(1) # Exit for any other error reading the new file


    # Process the new raw data using the defined function
    df_new_processed = process_raw_transactions(df_new_raw, patterns_wf, cash_investments)

    # Check if processing the new data was successful and resulted in a non-empty DataFrame
    if df_new_processed.empty:
         if not df_new_raw.empty:
              # If the raw file was not empty but processing resulted in an empty df, indicate failure
              print("Error: Processing of the new raw data resulted in an empty DataFrame.")
              print("This could be due to incorrect column dropping/renaming or data issues.")
              sys.exit(1)
         else:
              # If the raw file was empty, processing will also result in an empty df - this is expected
              print("New raw data file was empty or contained no valid transactions after processing. No new records to add.")
              sys.exit(0) # Exit gracefully as there's nothing new to append


    # Create a unique key string for each row in the new processed data for comparison
    # Ensure the processed new data has the necessary columns before creating the key
    if all(col in df_new_processed.columns for col in key_cols):
        df_new_processed['_key'] = df_new_processed[key_cols].astype(str).agg('_'.join, axis=1)
        # print(f"Created unique keys for new processed data. Example key: {df_new_processed['_key'].iloc[0] if not df_new_processed.empty else 'N/A'}") # Debug print
    else:
        print(f"Error: Processed new data is missing one or more required columns for duplicate checking ({key_cols}). Cannot proceed.")
        # You might want to save df_new_processed here for debugging its structure
        # df_new_processed.to_csv('debug_new_processed_structure_error.csv', index=False)
        sys.exit(1)


    # Identify duplicates: Find keys in df_new_processed that already exist in df_existing
    # This identifies rows in the *new* data that are duplicates of rows in the *existing* data.
    duplicate_keys_in_new = df_new_processed[df_new_processed['_key'].isin(df_existing['_key'])]['_key']

    if not duplicate_keys_in_new.empty:
        # If duplicates are found, report them to the user
        print("\n--- Duplicate Transactions Found ---")
        print(f"{len(duplicate_keys_in_new)} transaction(s) in the new data match existing records:")
        # Display the actual duplicate rows from the new data
        duplicates_to_show = df_new_processed[df_new_processed['_key'].isin(duplicate_keys_in_new)][key_cols]
        print(duplicates_to_show.to_string(index=False))
        print("----------------------------------")

        # Ask the user for confirmation to proceed and append unique records
        user_input = input("\nProceed and append only the unique new records? (y/n): ").lower()

        if user_input != 'y':
            # If user does not confirm, cancel the operation and exit
            print("Operation cancelled by user. No changes made to processed file.")
            sys.exit(0) # Exit cleanly

        # Filter out the identified duplicates from the new processed data
        # Use ~ for negation (keep rows where key is NOT in duplicate_keys_in_new)
        # Use .copy() to prevent SettingWithCopyWarning when modifying this filtered view later if needed
        df_new_unique = df_new_processed[~df_new_processed['_key'].isin(duplicate_keys_in_new)].copy()
        print(f"\nProceeding. Removed {len(duplicate_keys_in_new)} duplicate(s).")
        print(f"Appending {len(df_new_unique)} unique new record(s) to '{processed_csv_file}'.")

    else:
        # If no duplicates are found, all new records are unique relative to existing
        print("\nNo duplicate transactions found in the new data matching existing records.")
        df_new_unique = df_new_processed.copy() # All new records are unique, use .copy()
        print(f"Appending all {len(df_new_unique)} new record(s) to '{processed_csv_file}'.")


    # --- Finalize and Save ---
    # Remove the temporary '_key' column from both DataFrames before combining/saving
    df_existing = df_existing.drop(columns=['_key'])
    # Check if '_key' exists in df_new_unique before dropping (it might not if df_new_unique was empty)
    if '_key' in df_new_unique.columns:
       df_new_unique = df_new_unique.drop(columns=['_key'])

    # Combine the existing data with the unique new data
    # ignore_index=True resets the index of the combined DataFrame
    df_combined = pd.concat([df_existing, df_new_unique], ignore_index=True)

    # Save the updated combined DataFrame back to the processed file
    try:
        df_combined.to_csv(processed_csv_file, index=False)
        print(f"Successfully updated '{processed_csv_file}' with {len(df_new_unique)} new record(s). Total records: {len(df_combined)}.")
    except Exception as e:
         print(f"Error: Could not save the combined data to '{processed_csv_file}': {e}")
         sys.exit(1)


else:
    # --- Scenario: Processed file does NOT exist ---
    # Process the new raw file and create the initial processed file.
    print(f"'{processed_csv_file}' not found.")
    print(f"Processing '{csv_new_raw_file}' to create the initial processed data file.")

    # Read the new raw CSV file
    try:
        df_new_raw = pd.read_csv(csv_new_raw_file, header=None)
        print(f"Successfully read {len(df_new_raw)} raw records.")
    except FileNotFoundError:
        print(f"Error: New CSV file '{csv_new_raw_file}' not found.")
        print("Cannot create the processed file without the raw data. Exiting.")
        sys.exit(1) # Exit if the raw file is missing when processed file doesn't exist
    except Exception as e:
        print(f"An error occurred while reading the new raw file '{csv_new_raw_file}': {e}")
        sys.exit(1) # Exit for any other error reading the raw file


    # Process the raw data using the defined function
    df_processed = process_raw_transactions(df_new_raw, patterns_wf, cash_investments)

    # Check if processing was successful
    if df_processed.empty:
        if not df_new_raw.empty:
             print("Error: Processing of the raw data resulted in an empty DataFrame.")
             print("Please check the raw file format and the processing logic.")
             sys.exit(1)
        else:
             print("Raw data file was empty or contained no valid transactions after processing. No processed file created.")
             sys.exit(0) # Exit gracefully if raw file was empty


    # Save the processed DataFrame to create the new processed file
    try:
        df_processed.to_csv(processed_csv_file, index=False)
        print(f"Successfully created and saved initial processed data to '{processed_csv_file}' with {len(df_processed)} records.")
    except Exception as e:
         print(f"Error: Could not save the initial processed data to '{processed_csv_file}': {e}")
         sys.exit(1)

print("\nScript finished.")

# Optional: Print the final DataFrame head (uncomment the lines below to enable)
# try:
#     print("\nFirst 5 rows of the final processed DataFrame:")
#     print(pd.read_csv(processed_csv_file).head().to_string(index=False))
# except FileNotFoundError:
#     print(f"\nProcessed file '{processed_csv_file}' not found after script execution.")
