# excelsql

`excelsql` is a lightweight Python package that allows you to load Excel workbooks (`.xls` and `.xlsx` formats) into an SQLite database, perform SQL operations, and save changes back into the original format. It's designed to combine the ease of Excel with the power of SQL, streamlining data manipulation and analysis.

## Table of Contents

1. [Features](#features)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Features in Detail](#features-in-detail)
   - [SQL Query Interface with Pandas Integration](#sql-query-interface-with-pandas-integration)
   - [Schema Validation and Modification](#schema-validation-and-modification)
   - [Batch Processing and Multi-file Support](#batch-processing-and-multi-file-support)
   - [Data Cleaning Functions](#data-cleaning-functions)
   - [Joins and Merging Data Across Sheets](#joins-and-merging-data-across-sheets)
   - [Advanced Reporting & Visualization Support](#advanced-reporting--visualization-support)
   - [Caching Queries for Speed Optimization](#caching-queries-for-speed-optimization)
   - [Export to Multiple Formats](#export-to-multiple-formats)
   - [Jupyter Notebook Integration](#jupyter-notebook-integration)
5. [Contributing](#contributing)
6. [License](#license)

---

## Features

- Load Excel files into an SQLite database for SQL querying.
- Perform SQL operations on the data (SELECT, INSERT, JOIN, etc.).
- Return SQL query results as Pandas DataFrames.
- Clean and normalize data in Excel sheets.
- Validate and modify the schema (add/remove columns).
- Support for batch processing and merging multiple Excel workbooks.
- Generate reports and visualizations.
- Caching for repeated queries.
- Export data to multiple formats (CSV, JSON, etc.).
- Jupyter Notebook integration for displaying data.

---

## Installation

To install `excelsql`, run the following command:

```bash
pip install excelsql
```

---

## Usage

### Loading an Excel File

```python
import excelsql

# Path to the Excel file
xls_file = './data/file_example_XLS_5000.xls'
db_file = './data/excel_db.sqlite'

# Initialize the driver
driver = excelsql(xls_file, db_path=db_file)

# Show worksheets
worksheets = driver.show_worksheets()

# Perform SQL query and return results as a DataFrame
df = driver.execute_query_to_dataframe('SELECT * FROM Sheet1')

# Save changes back to an Excel file
driver.save_to_file('./data/modified_file.xlsx')

# Close the connection
driver.close()
```

---

## Features in Detail

### SQL Query Interface with Pandas Integration

Run SQL queries and return the results as a Pandas DataFrame.

```python
df = driver.execute_query_to_dataframe('SELECT * FROM Sheet1 WHERE Age > 30')
```

### Schema Validation and Modification

- **`validate_schema`**: Validate the schema for a worksheet.
- **`add_column`**: Add new columns to a worksheet.

```python
driver.add_column('Sheet1', 'NewColumn', datatype='TEXT')
driver.validate_schema('Sheet1', validations={'Age': 'numeric', 'Date': 'date'})
```

### Batch Processing and Multi-file Support

Load and merge multiple Excel workbooks.

```python
driver.load_multiple_workbooks(['file1.xlsx', 'file2.xlsx'])
driver.merge_workbooks('Sheet1', 'Sheet2', on='Id', how='inner')
```

### Data Cleaning Functions

- **`clean_data`**: Remove or fill missing values.
- **`normalize`**: Normalize the values of numeric columns.

```python
driver.clean_data('Sheet1', strategy='dropna')
driver.normalize('Sheet1', columns=['Age'])
```

### Joins and Merging Data Across Sheets

Join multiple worksheets using SQL-style operations.

```python
driver.join_sheets('Sheet1', 'Sheet2', on='Id', how='inner')
```

### Advanced Reporting & Visualization Support

- **`generate_report`**: Generate descriptive statistics and save as an Excel file.
- **`export_visualization`**: Create visualizations (bar, line, area charts) from data.

```python
driver.generate_report('Sheet1', output='./data/summary.xlsx')
driver.export_visualization('Sheet1', x_col='Age', y_col='Salary', plot_type='bar', output_path='./data/visuals')
```

Below is an example of a bar chart visualization generated from the `Sheet1` dataset:

![Bar Chart Visualization](./data/visuals/Sheet1_bar_Age_Salary.png)

### Caching Queries for Speed Optimization

Enable caching of frequent SQL queries to speed up repeated requests.

```python
driver.enable_query_cache()
```

### Export to Multiple Formats

- **`export_to_csv`**: Export worksheet data to CSV.
- **`export_to_json`**: Export worksheet data to JSON.

```python
driver.export_to_csv('Sheet1', './data/output.csv')
driver.export_to_json('Sheet1', './data/output.json')
```

You can find example exported data formats below:

- [CSV Export Example](./data/output.csv)
- [JSON Export Example](./data/output.json)

### Jupyter Notebook Integration

Display data directly in Jupyter notebooks for quick data inspection.

```python
driver.display_in_notebook('Sheet1')
```

---

## Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request on [GitHub](https://github.com/chris17453/xlsql).

---

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](./LICENSE) file for details.
