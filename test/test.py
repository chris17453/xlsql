from excelsql import excelsql
import pandas as pd

xls_file = './data/file_example_XLS_5000.xls'  # Or .xlsx file
db_file = './data/excel_db.sqlite'

# Create the driver
driver = excelsql(xls_file, db_path=db_file)

# Show available worksheets
worksheets = driver.show_worksheets()

# Show columns for a specific worksheet (e.g., 'Sheet1')
if 'Sheet1' in worksheets:
    columns = driver.show_columns('Sheet1')

# Step 1: Copy data from 'Sheet1' to a new worksheet called 'CopiedSheet'
if 'Sheet1' in worksheets:
    # Fetch the data from 'Sheet1'
    df = pd.read_sql('SELECT * FROM Sheet1', driver.engine)
    
    # Create a new table for the copied data
    df.to_sql('CopiedSheet', driver.engine, if_exists='replace', index=False)
    print("Copied data to 'CopiedSheet'.")

# Step 2: Create another worksheet 'NewSheet' and insert 3 new rows
# Define new rows to insert
new_rows = [
    {'First Name': 'John', 'Last Name': 'Doe', 'Gender': 'Male', 'Country': 'USA', 'Age': 28, 'Date': '2024-09-20', 'Id': 5001},
    {'First Name': 'Jane', 'Last Name': 'Smith', 'Gender': 'Female', 'Country': 'UK', 'Age': 34, 'Date': '2024-09-21', 'Id': 5002},
    {'First Name': 'Alice', 'Last Name': 'Johnson', 'Gender': 'Female', 'Country': 'Canada', 'Age': 29, 'Date': '2024-09-22', 'Id': 5003}
]

# Convert the new rows to a DataFrame
new_rows_df = pd.DataFrame(new_rows)

# Insert the new rows into a new worksheet 'NewSheet'
new_rows_df.to_sql('NewSheet', driver.engine, if_exists='replace', index=False)
print("Inserted 3 new rows into 'NewSheet'.")

# Step 3: Save back to an Excel file in the correct format
driver.save_to_file('./data/newfile.xls')  # Or .xlsx

# Close the driver connection
driver.close()
