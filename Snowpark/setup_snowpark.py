import pandas as pd
from tabulate import tabulate
from utils import snowpark_utils

# Get the Snowpark session
session = snowpark_utils.get_snowpark_session()

# SQL query
query = "select * from SNOWFLAKE_SAMPLE_DATA.INFORMATION_SCHEMA.ENABLED_ROLES;"

# Execute the query and fetch results
df = session.sql(query).toPandas()

# Set the display options to show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Formatting the table
formatted_table = tabulate(df, headers='keys', tablefmt='psql')

# Print the formatted table
print(formatted_table)

# Close the Snowpark session
session.close()




