# xlsql

`xlsql` is a Python package that allows you to load Excel workbooks (`.xls` and `.xlsx` formats) into an SQLite database, perform SQL queries on the data, and modify or extend the data within the workbook. It also supports saving the modified data back into the original Excel format.

## Features

- Supports both `.xls` and `.xlsx` file formats.
- Loads Excel worksheets into an SQLite database for easy SQL operations.
- Allows you to create new worksheets, insert new rows, and modify existing data.
- Saves the workbook with all modifications in the original format (`.xls` or `.xlsx`).
- Execute SQL queries on the loaded data using SQLAlchemy and SQLite.

## Installation

To install the package, run:

```bash
pip install xlsql
```

### Additional Requirements

You may need to install some system dependencies depending on your platform:

```bash
# On Fedora-based systems (e.g., Fedora, CentOS):
dnf install python-devel sqlite-devel
```

## Quick Start

Here’s how to get started with `xlsql`:

### 1. Load an Excel file into SQLite and query data

```python
from xlsql import excel_db
import pandas as pd

# Path to the Excel file
xls_file = './data/file_example_XLS_5000.xls'  # Or .xlsx file
db_file = './data/excel_db.sqlite'  # SQLite database (in-memory or on disk)

# Initialize the driver
driver = excel_db(xls_file, db_path=db_file)

# Show available worksheets in the Excel file
worksheets = driver.show_worksheets()

# Show the columns of a specific worksheet (e.g., 'Sheet1')
if 'Sheet1' in worksheets:
    columns = driver.show_columns('Sheet1')

# Query some data from 'Sheet1'
result = driver.execute_query('SELECT * FROM Sheet1 LIMIT 10')
print(result)
```

### 2. Copy Data to a New Worksheet and Insert Rows

```python
# Copy data from 'Sheet1' to a new worksheet 'CopiedSheet'
df = pd.read_sql('SELECT * FROM Sheet1', driver.engine)
df.to_sql('CopiedSheet', driver.engine, if_exists='replace', index=False)
print("Copied data to 'CopiedSheet'.")

# Insert new rows into a new worksheet 'NewSheet'
new_rows = [
    {'First Name': 'John', 'Last Name': 'Doe', 'Gender': 'Male', 'Country': 'USA', 'Age': 28, 'Date': '2024-09-20', 'Id': 5001},
    {'First Name': 'Jane', 'Last Name': 'Smith', 'Gender': 'Female', 'Country': 'UK', 'Age': 34, 'Date': '2024-09-21', 'Id': 5002},
    {'First Name': 'Alice', 'Last Name': 'Johnson', 'Gender': 'Female', 'Country': 'Canada', 'Age': 29, 'Date': '2024-09-22', 'Id': 5003}
]

new_rows_df = pd.DataFrame(new_rows)
new_rows_df.to_sql('NewSheet', driver.engine, if_exists='replace', index=False)
print("Inserted 3 new rows into 'NewSheet'.")
```

### 3. Save the Modified Workbook

Once you've copied data or inserted new rows, you can save the Excel workbook, and it will preserve the original format (`.xls` or `.xlsx`):

```python
# Save the Excel file with all changes
driver.save_to_file('./data/modified_file.xls')  # Or .xlsx depending on original format

# Close the driver connection
driver.close()
```

## API Reference

### `excel_db`

The `excel_db` class provides methods for loading Excel files into SQLite, performing SQL queries, and modifying the workbook.

#### Methods:

- **`show_worksheets()`**: Displays the names of all worksheets in the Excel file.
  ```python
  worksheets = driver.show_worksheets()
  ```

- **`show_columns(sheet_name)`**: Displays the column names of the specified worksheet.
  ```python
  columns = driver.show_columns('Sheet1')
  ```

- **`execute_query(query)`**: Executes a raw SQL query on the data stored in the SQLite database.
  ```python
  result = driver.execute_query('SELECT * FROM Sheet1 LIMIT 10')
  ```

- **`save_to_file(output_path)`**: Saves the modified workbook (including new worksheets) back into an Excel file in the original format.
  ```python
  driver.save_to_file('./data/modified_file.xls')  # Or .xlsx
  ```

## License

This project is licensed under the BSD 3 License.
