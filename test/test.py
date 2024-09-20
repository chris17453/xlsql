import sys
import os

# Add the directory above to sys.path
lib_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, lib_dir)


from excelsql import excelsql
import pandas as pd

# Path to the Excel file
xls_file = './data/file_example_XLS_5000.xls'  # Or .xlsx file
db_file = './data/excel_db.sqlite'

# Initialize the driver
driver = excelsql(xls_file, db_path=db_file)

# ==========================
# ORIGINAL FUNCTIONALITY
# ==========================

# Show available worksheets
worksheets = driver.show_worksheets()

# Check if 'Sheet1' exists before proceeding
if 'Sheet1' in worksheets:
    # Show columns for 'Sheet1'
    print(f"Columns in 'Sheet1':")
    columns = driver.show_columns('Sheet1')
    print(columns)

    # Step 1: Copy data from 'Sheet1' to a new worksheet called 'CopiedSheet'
    df = pd.read_sql('SELECT * FROM Sheet1', driver.engine)
    df.to_sql('CopiedSheet', driver.engine, if_exists='replace', index=False)
    print("Copied data to 'CopiedSheet'.")

    # Step 2: Create another worksheet 'NewSheet' and insert 3 new rows
    new_rows = [
        {'First Name': 'John', 'Last Name': 'Doe', 'Gender': 'Male', 'Country': 'USA', 'Age': 28, 'Date': '2024-09-20', 'Id': 5001},
        {'First Name': 'Jane', 'Last Name': 'Smith', 'Gender': 'Female', 'Country': 'UK', 'Age': 34, 'Date': '2024-09-21', 'Id': 5002},
        {'First Name': 'Alice', 'Last Name': 'Johnson', 'Gender': 'Female', 'Country': 'Canada', 'Age': 29, 'Date': '2024-09-22', 'Id': 5003}
    ]
    new_rows_df = pd.DataFrame(new_rows)
    new_rows_df.to_sql('NewSheet', driver.engine, if_exists='replace', index=False)
    print("Inserted 3 new rows into 'NewSheet'.")

    # Save back to an Excel file in the correct format
    driver.save_to_file('./data/newfile.xls')
    print("Saved the file as './data/newfile.xls'.")


    # Step 3: Execute SQL query and return as DataFrame
    print("Data from SQL query:")
    df_query = driver.execute_query_to_dataframe('SELECT * FROM Sheet1 LIMIT 10')
    print(df_query)

    # Step 4: Add and validate a new column
    print("Adding a new column and validating the schema:")
    driver.add_column('Sheet1', 'NewColumn', datatype='TEXT')
    driver.validate_schema('Sheet1', validations={'Age': 'numeric', 'Date': 'date'})

    # Step 5: Clean data (drop missing values)
    # Clean the data by dropping rows only where 'Age' or 'Gender' is missing
    print("Cleaning data by dropping rows where 'Age' or 'Gender' is missing:")
    driver.clean_data('Sheet1', strategy='dropna', subset=['Age', 'Gender'])

    # Alternative: Fill missing values in the 'Age' column with 0 and 'Gender' with 'Unknown'
    # print("Filling missing values in 'Age' and 'Gender':")
    # driver.clean_data('Sheet1', strategy='fillna', fill_value={'Age': 0, 'Gender': 'Unknown'})

    # Check the data after cleaning
    df_cleaned = pd.read_sql('SELECT * FROM Sheet1', driver.engine)
    print("Data after cleaning:")
    print(df_cleaned.head())
    # Step 6: Normalize the 'Age' column
    print("Normalizing the 'Age' column:")
    driver.normalize('Sheet1', columns=['Age'])

    # Step 7: Load multiple workbooks (assuming multiple files)
    # Uncomment the line below if you have multiple Excel files
    # driver.load_multiple_workbooks(['./data/file_example_XLS_5000.xls', './data/another_example.xlsx'])

    # Step 8: Join two sheets (assuming you have two valid sheets)
    # Uncomment the line below if you have multiple sheets to join
    # driver.join_sheets('Sheet1', 'Sheet2', on='Id', how='inner')

    # Step 9: Generate a report with descriptive statistics
    print("Generating a report for 'Sheet1':")
    driver.generate_report('Sheet1', output='./data/summary_report.xlsx')

    # Step 10: Export a bar chart visualization (e.g., Gender vs Age)
    print("Exporting a bar chart visualization:")
    driver.export_visualization('Sheet1', x_col='Gender', y_col='Age', plot_type='bar' , output_path='./data')

    # Example: Export a line chart for Age vs Id
    print("Exporting a line chart visualization:")
    driver.export_visualization('Sheet1', x_col='Age', y_col='Id', plot_type='line', output_path='./data')

    # Example: Export a bar chart showing counts of Gender (no y_col, so counts)
    print("Exporting a bar chart showing counts of Gender:")
    driver.export_visualization('Sheet1', x_col='Gender', plot_type='bar',  output_path='./data')
        # Step 11: Enable query caching
    print("Enabling query caching:")
    driver.enable_query_cache()

    # Step 12: Export data to CSV
    print("Exporting 'Sheet1' to CSV:")
    driver.export_to_csv('Sheet1', './data/sheet1_export.csv')

    # Step 13: Export data to JSON
    print("Exporting 'Sheet1' to JSON:")
    driver.export_to_json('Sheet1', './data/sheet1_export.json')

    # Step 14: Display data in Jupyter notebook (only works inside a notebook environment)
    # Uncomment the line below if running in a Jupyter Notebook
    driver.display_in_notebook('Sheet1')

else:
    print("Sheet 'Sheet1' not found. Aborting process.")

# Close the driver connection
driver.close()
