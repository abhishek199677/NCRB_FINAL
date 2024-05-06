import camelot
import re
import pandas as pd

file_paths = [
   "/Users/apple/Desktop/FACTLY/Incidence and Rate of Suicides during 2004 _State_ UT _ City-wise_.pdf",
   "/Users/apple/Desktop/FACTLY/2_2 Incidence and Rate of Suicides during 2006 _State_ UT _ City-wise_.pdf",
   "/Users/apple/Desktop/FACTLY/2_2 Incidence and Rate of Suicides during 2007 _State_ UT _ City-wise_.pdf",
   "/Users/apple/Desktop/FACTLY/2_2 -- Incidence and Rate of Suicides during 2008 _State_ UT _ City-wise_.pdf",
]

all_tables = []

# Iterate through each file path
for file_path in file_paths:
    # Use Camelot to read tables from the PDF
    tables = camelot.read_pdf(file_path, flavor='stream', row_tol=10, split_text=True)

    # Iterate through each table in the current PDF
    for table in tables:
        # Remove null values
        table_df = table.df.dropna()

        # Drop the first column
        table_df = table_df.drop(columns=[0])

        # Insert the year column
        table_df.insert(0, "year", "")

        # Extract the year from the file name
        year_match = re.search(r"\b\d{4}\b", file_path)
        if year_match:
            year = int(year_match.group())  # Extract the matched year
            table_df["year"] = year

        # Mapping dictionary for column renaming
        column_mapping = {
            table_df.columns[1]: 'state',
            table_df.columns[2]: 'Number of Suicides',
            table_df.columns[3]: 'Percentage Share in Total',
            table_df.columns[4]: 'Projected Mid Year Population',
            table_df.columns[5]: 'Rate of Suicides'
        }

        # Rename columns
        table_df = table_df.rename(columns=column_mapping)

        # Find index where "ANDHRA PRADESH" occurs
        index_ap = table_df[table_df['state'] == 'ANDHRA PRADESH'].index

        # If "ANDHRA PRADESH" found, set the index to 0
        if not index_ap.empty:
            table_df = pd.concat([table_df.loc[index_ap[0]:], table_df.loc[:index_ap[0] - 1]], ignore_index=True)

        # Find index where "UNION TERRITORIES:" or "UNION TERRITORIES" occurs
        index_ut = table_df[table_df['state'].str.contains('UNION TERRITORIES:|UNION TERRITORIES')].index

        # If "UNION TERRITORIES" found, remove rows from there till the end
        if not index_ut.empty:
            table_df = table_df.loc[:index_ut[0] - 1]

        # Append the table to the list of tables
        all_tables.append(table_df)

# Check if tables are extracted successfully
if all_tables:
   # Combine all the tables into one DataFrame
   combined_df2 = pd.concat(all_tables, ignore_index=True)
  
   # Drop the last column
   combined_df2 = combined_df2.iloc[:, :-1]

else:
   print("No tables were extracted from the PDF files.")

# Clean column names
combined_df2.columns = combined_df2.columns.str.strip().str.replace('&', 'and')

# Melt DataFrame
melted_df = pd.melt(combined_df2, id_vars=['year', 'state'],
                     value_vars=['Number of Suicides', 'Percentage Share in Total',
                                 'Projected Mid Year Population', 'Rate of Suicides'],
                     var_name='category', value_name='value')

# Define mapping of category to unit
unit_mapping = {
    'Number of Suicides': 'value in Absolute number',
    'Percentage Share in Total': 'value in Percentage',
    'Projected Mid Year Population': 'value in Lakh',
    'Rate of Suicides': 'value in Ratio'
}

# Map category to unit
melted_df['unit'] = melted_df['category'].map(unit_mapping)
melted_df['note'] = ''
melted_df['category'] = melted_df['category'].str.replace('&', 'and')

# Save to CSV
melted_df.to_csv('combined_df2.csv', index=False)
