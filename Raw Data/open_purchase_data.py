import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# Step 1: Database Connection Setup
# -----------------------------
DB_TYPE = 'mssql+pyodbc'
DB_HOST = 'IPGP-OX-AGP02'  # Replace with your server/host if needed
DB_NAME = 'IPG-DW-PROTOTYPE'
DB_DRIVER = 'ODBC Driver 17 for SQL Server'
connection_string = f"{DB_TYPE}://@{DB_HOST}/{DB_NAME}?driver={DB_DRIVER}&trusted_connection=yes"
engine = create_engine(connection_string)

# -----------------------------
# Step 2: Helper Function
# -----------------------------
def load_and_process_table(query, engine, rename_cols=None, additional_processing=None, **kwargs):
    """
    General-purpose function to run a SQL query and return a pandas DataFrame
    with optional renaming or post-processing.
    """
    try:
        df = pd.read_sql_query(query, con=engine)
        if rename_cols:
            df = df.rename(columns=rename_cols)
        if additional_processing:
            df = additional_processing(df, **kwargs)
        return df
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        return None

# -----------------------------
# Step 3: The Grouped Open Purchase Line Query
# -----------------------------
grouped_query = """
WITH Source AS (
    SELECT 
        'OPEN' AS [Status],
        [Document Type],
        [Document No_],
        [Line No_],
        [Shortcut Dimension 1 Code],
        [Buy-from Vendor No_],
        [Type],
        [No_],
        [Location Code],
        [Expected Receipt Date],
        [Package Tracking No_],
        [Promised Receipt Date],
        [Planned Receipt Date],
        [Description],
        CASE 
            WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
            ELSE [Qty_ per Unit of Measure]
        END AS [Qty_ per Unit of Measure],
        [Quantity] * CASE 
                        WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                        ELSE [Qty_ per Unit of Measure] 
                     END AS [Quantity],
        [Outstanding Quantity] * CASE 
                                    WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                                    ELSE [Qty_ per Unit of Measure] 
                                 END AS [Outstanding Quantity],
        [Unit Cost (LCY)] / CASE 
                                WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                                ELSE [Qty_ per Unit of Measure] 
                             END AS [Unit Cost (LCY)],
        [Requested Receipt Date],
        [Quantity] * [Unit Cost (LCY)] AS [Total],
        ([Quantity] - [Outstanding Quantity]) * CASE 
                                                    WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                                                    ELSE [Qty_ per Unit of Measure] 
                                                 END AS [Quantity Delivered]
    FROM
        [dbo].[IPG Photonics Corporation$Purchase Line]
    WHERE
        [Order Date] > '2019-01-01'
        AND [Quantity] > 0
        AND [Unit Cost (LCY)] > 0
        AND [Document Type] = 1
        AND [Type] = 2
)
SELECT 
    [No_],
    CONVERT(date, [Expected Receipt Date]) AS [Expected Receipt Date],
    [Document No_],
    SUM([Outstanding Quantity]) AS QTY
FROM Source
GROUP BY [No_], [Expected Receipt Date], [Document No_]
HAVING SUM([Outstanding Quantity]) <> 0
ORDER BY [Expected Receipt Date] ASC;
"""

# -----------------------------
# Step 4: get_grouped_purchase_line_data() Function
# -----------------------------
def get_grouped_purchase_line_data():
    """
    Returns the DataFrame containing the grouped open purchase line data
    loaded from the SQL query above.
    """
    df = load_and_process_table(query=grouped_query, engine=engine)
    return df

# -----------------------------
# OPTIONAL: if run directly
# -----------------------------
if __name__ == "__main__":
    grouped_df = get_grouped_purchase_line_data()
    if grouped_df is not None:
        print(grouped_df.head())
        print("Total records:", len(grouped_df))
