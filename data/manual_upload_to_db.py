import pandas as pd
import sqlite3
import os
import sys
from datetime import datetime

def upload_csv_to_db(csv_path, db_path="team_model_db", table_name="prem_data"):
    """
    Upload data from a CSV file to a SQLite database table.
    
    Args:
        csv_path (str): Path to the CSV file
        db_path (str): Path to the SQLite database
        table_name (str): Name of the table to insert data into
    
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"Reading CSV file: {csv_path}")
    
    # Check if CSV file exists
    if not os.path.exists(csv_path):
        print(f"ERROR: CSV file '{csv_path}' does not exist!")
        return False
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        print(f"Successfully read {len(df)} rows from CSV")
        
        # Display sample of data
        print("\nSample data (first 3 rows):")
        print(df.head(3))
        
        # Check for season column, add if missing
        if 'season' not in df.columns:
            print("\nAdding 'season' column based on match_date...")
            # Function to determine season based on match date
            def get_season(date_str):
                if pd.isna(date_str):
                    return None
                try:
                    date = datetime.strptime(date_str, '%Y-%m-%d')
                    # If month is August or later, use current year, otherwise previous year
                    if date.month >= 8:
                        return date.year
                    else:
                        return date.year - 1
                except:
                    return None
            
            # Apply function to create season column
            df['season'] = df['match_date'].apply(get_season)
            print("Season column added")
        
        # Connect to database
        print(f"\nConnecting to database: {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None
        
        if table_exists:
            # Get current row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            before_count = cursor.fetchone()[0]
            print(f"Current rows in {table_name}: {before_count}")
            
            # Check for duplicate prevention
            should_check_duplicates = input("Check for duplicates before inserting? (y/n): ").lower() == 'y'
            
            if should_check_duplicates and 'match_url' in df.columns and 'Minute' in df.columns and 'Player' in df.columns:
                print("Checking for potential duplicates...")
                # Get a list of existing match URLs
                cursor.execute(f"SELECT DISTINCT match_url FROM {table_name}")
                existing_urls = [row[0] for row in cursor.fetchall()]
                
                # Filter out rows that might be duplicates
                original_len = len(df)
                df = df[~df['match_url'].isin(existing_urls)]
                filtered_count = original_len - len(df)
                
                if filtered_count > 0:
                    print(f"Filtered out {filtered_count} potential duplicate rows")
                    print(f"Proceeding with {len(df)} new rows")
                else:
                    print("No duplicates found")
        else:
            print(f"Table '{table_name}' does not exist, it will be created")
            before_count = 0
        
        # Insert data
        print(f"\nInserting data into {table_name}...")
        df.to_sql(table_name, conn, if_exists='append', index=False)
        
        # Verify insertion
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        after_count = cursor.fetchone()[0]
        rows_added = after_count - before_count
        
        # Commit changes
        conn.commit()
        print("Changes committed to database")
        
        # Report results
        print(f"\nOperation completed:")
        print(f"- Rows in CSV: {len(df)}")
        print(f"- Rows added to database: {rows_added}")
        print(f"- Total rows in table now: {after_count}")
        
        if rows_added != len(df):
            print(f"\nWARNING: The number of rows added ({rows_added}) doesn't match the CSV row count ({len(df)})")
            print("This could be due to constraints or data type issues.")
        
        # Close connection
        cursor.close()
        conn.close()
        
        return True
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python csv_to_db.py path_to_csv [database_path] [table_name]")
        print("\nExample: python csv_to_db.py data/recent_matches.csv team_model_db prem_data")
        
        # Ask for CSV path if not provided
        csv_path = input("\nEnter path to CSV file: ")
        if not csv_path:
            print("No CSV path provided. Exiting.")
            sys.exit(1)
    else:
        csv_path = sys.argv[1]
    
    # Get optional database path and table name
    db_path = sys.argv[2] if len(sys.argv) > 2 else "data/team_model_db.db"
    table_name = sys.argv[3] if len(sys.argv) > 3 else "prem_data"
    
    # Run the upload
    upload_csv_to_db(csv_path, db_path, table_name)