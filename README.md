# Wells Fargo Transaction Processing Script
Note: Generated using LLM

## Overview

This Python script processes raw transaction data from a specified CSV file, categorizes transactions based on configured patterns, identifies cash flow types (cash in, cash out, cash investments), and manages this processed data by appending new, unique transactions to an existing processed file. It checks for duplicate transactions based on date, amount, and description and requires user confirmation before appending new data if duplicates are found.

## Features

* Reads raw transaction data from a specified CSV file (assumes no header row).
* Uses a JSON configuration file to identify vendors and categorize transactions (e.g., investments).
* Determines transaction cash flow type ('cash_in', 'cash_out', 'cash_investments').
* Reads an existing processed data CSV file (`processed_transactions.csv`) if available.
* Compares new processed transactions against existing records to find duplicates based on 'date', 'amount', and 'description'.
* Reports duplicate transactions found in the new data to the user.
* Prompts the user for confirmation to append only the unique new records.
* Saves the combined, de-duplicated data back to `processed_transactions.csv`.
* Includes basic error handling for file operations and data processing.

## Prerequisites

* Python 3.6 or higher
* pandas library (`pip install pandas`)

## Files

Ensure the following files are in the same directory as the script:

* `process_transactions.py`: The main script file containing the Python code.
* `WF_march_april_25.csv`: The raw input CSV file containing new transactions to be processed. **This file is expected to have no header row.**
* `processed_transactions.csv`: The output file where processed and combined transaction data is stored. This file will be created if it doesn't exist.
* `config.json`: A JSON file containing configuration for vendor patterns and cash investment categories.

## Setup

1.  **Save the script:** Save the provided Python code as `process_transactions.py`.
2.  **Place files:** Ensure `process_transactions.py`, your raw CSV file (`WF_march_april_25.csv`), and `config.json` are all in the same directory.
3.  **Create `config.json`:** Create or update the `config.json` file. It must contain two top-level keys:
    * `patterns_wf`: A dictionary where keys are vendor names (strings) and values are lists of strings. If any string in the list is found in a transaction's description (case-insensitive), that vendor will be assigned.
    * `cash_investments`: A list of vendor names (strings) that should be classified as 'cash_investments' when the transaction amount is negative.

## Usage

1.  Open your terminal or command prompt.
2.  Navigate to the directory where you saved the script and data files.
3.  Run the script using the command:

    ```bash
    python process_transactions.py
    ```

## How it Works

1.  **Initialization:** The script defines the paths for the input raw data (`WF_march_april_25.csv`), the processed data (`processed_transactions.csv`), and the configuration file (`config.json`).
2.  **Load Config:** It reads vendor patterns and investment categories from `config.json`.
3.  **Check for Processed File:** It checks if `processed_transactions.csv` exists using `os.path.exists()`.
    * **If `processed_transactions.csv` exists:**
        * Reads the existing `processed_transactions.csv`.
        * Reads the new raw data from `WF_march_april_25.csv`.
        * Processes the new raw data (drops columns, renames, identifies vendor/cash flow).
        * Creates a unique key for each transaction in both the existing and new processed data based on 'date', 'amount', and 'description'.
        * Identifies transactions in the ***new*** data whose keys match transactions in the ***existing*** data (duplicates).
        * If duplicates are found, it prints them to the console and asks for user confirmation (`y` to proceed).
        * If the user confirms, it filters the new processed data to remove these duplicates.
        * It combines the existing data with the remaining unique new data.
        * Saves the combined data back to `processed_transactions.csv`, overwriting the old file.
    * **If `processed_transactions.csv` does not exist:**
        * Reads the new raw data from `WF_march_april_25.csv`.
        * Processes the new raw data completely.
        * Saves this processed data as the new `processed_transactions.csv`.

## Input CSV Format Assumption

The script assumes the raw input CSV file (`WF_march_april_25.csv`) has **no header row**. It expects the relevant data (date, amount, description) to be in the first three columns, and will attempt to drop the 3rd and 4th columns (indices 2 and 3) based on the original code's logic. Ensure your raw CSV matches this structure or adjust the `cols_to_drop_indices` and column renaming logic in the `process_raw_transactions` function if necessary.

## Output

* **Terminal Output:** The script prints messages indicating its progress, file loading status, duplicate findings, and the result of the operation (appending or creating the file). It will prompt for user input if duplicates are found.
* **`processed_transactions.csv`:** This file will contain the processed transaction data with columns: `date`, `amount`, `description`, `vendors`, and `cash_flow`. If the file existed, it will be updated with unique new records; otherwise, it will be created.

## Troubleshooting

* **`FileNotFoundError`:** Check that `process_transactions.py`, the specified new raw CSV file (`WF_march_april_25.csv`), and `config.json` are in the same directory where you are running the script.
* **`json.JSONDecodeError`:** Your `config.json` file has a syntax error. Use a JSON validator online to check its format. Ensure all keys and string values are enclosed in double quotes, and commas are correctly placed.
* **"Raw data does not have enough columns..." or "Existing file ... is missing one or more key columns..." Errors:** The structure of your input CSV (`WF_march_april_25.csv`) or the existing `processed_transactions.csv` does not match the structure the script expects (at least 3 columns after potential dropping for raw, and 'date', 'amount', 'description' for both). Inspect your CSV files and adjust the column handling logic in the `process_raw_transactions` function if necessary.
* **"Non-numeric amount found..." Warnings:** Some rows in your raw data have values in the amount column that cannot be converted to numbers. These rows will be dropped. Inspect your raw CSV for formatting issues in the amount column.
* **Processing Results in Empty DataFrame:** If `process_raw_transactions` returns an empty DataFrame, it means either the raw file was empty, or the column handling or numeric conversion removed all rows. Check your raw data and the column dropping/renaming logic.
