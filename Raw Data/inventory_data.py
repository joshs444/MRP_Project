import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# Step 1: Database Connection Setup
# -----------------------------
DB_TYPE = 'mssql+pyodbc'
DB_HOST = 'IPGP-OX-AGP02'  # Replace with your correct server/host if needed
DB_NAME = 'IPG-DW-PROTOTYPE'
DB_DRIVER = 'ODBC Driver 17 for SQL Server'
connection_string = f"{DB_TYPE}://@{DB_HOST}/{DB_NAME}?driver={DB_DRIVER}&trusted_connection=yes"
engine = create_engine(connection_string)

# -----------------------------
# Step 2: Helper Function
# -----------------------------
def load_and_process_table(query, engine, rename_cols=None, additional_processing=None, **kwargs):
    """
    General-purpose function to run a SQL query
    and return a pandas DataFrame with optional renaming or post-processing.
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
# Step 3: The FULL Inventory Query
# -----------------------------
inventory_query = """
WITH SourceData AS (
    -- Get all columns and add a new column with the posting date cast to DATE
    SELECT
        *,
        CAST([Posting Date] AS DATE) AS [Posting_Date]
    FROM dbo.item_ledger_entry_all_v
),
RemovedColumns AS (
    -- Select only the columns needed for further processing
    SELECT
        [Posting_Date],
        [SUM_Cost_Amount_Actual],
        [SUM_Cost_Amount_Expected],
        [SUM_Root_Cost_Actual],
        [SUM_Root_Cost_Expected],
        [Quantity],
        [Subsidiary],
        [Item No_],
        [Entry Type],
        [Location Code],
        [Global Dimension 1 Code]
    FROM SourceData
),
AddedColumns AS (
    -- Compute Inventory_Cost_LC and Root_Cost_LC by adding respective cost columns
    SELECT
        *,
        [SUM_Cost_Amount_Actual] + [SUM_Cost_Amount_Expected] AS [Inventory_Cost_LC],
        [SUM_Root_Cost_Actual] + [SUM_Root_Cost_Expected] AS [Root_Cost_LC]
    FROM RemovedColumns
),
FilteredRows AS (
    -- Keep only rows where Quantity is not zero
    SELECT *
    FROM AddedColumns
    WHERE [Quantity] <> 0
),
CustomRootCost AS (
    -- Compute additional columns: RelativeDifference, InitialResult, and End_of_Month
    SELECT
        *,
        CASE
            WHEN [Inventory_Cost_LC] = 0 THEN 1
            ELSE ABS((COALESCE([Inventory_Cost_LC], 0) - COALESCE([Root_Cost_LC], 0)) / NULLIF([Inventory_Cost_LC], 0))
        END AS RelativeDifference,
        CASE
            WHEN [Root_Cost_LC] IS NULL OR [Root_Cost_LC] = 0 
                 OR ABS((COALESCE([Inventory_Cost_LC], 0) - COALESCE([Root_Cost_LC], 0)) / NULLIF([Inventory_Cost_LC], 0)) >= 1
            THEN COALESCE([Inventory_Cost_LC], 0)
            ELSE COALESCE([Root_Cost_LC], 0)
        END AS InitialResult,
        CASE
            WHEN EOMONTH([Posting_Date]) = EOMONTH(CURRENT_TIMESTAMP)
                THEN CAST(CURRENT_TIMESTAMP AS DATE)
            ELSE EOMONTH([Posting_Date])
        END AS [End_of_Month]
    FROM FilteredRows
),
FinalData AS (
    -- Apply one more calculation for the Custom_Root_Cost
    SELECT
        *,
        CASE
            WHEN InitialResult = 0 AND [Root_Cost_LC] IS NOT NULL THEN COALESCE([Root_Cost_LC], 0)
            ELSE InitialResult
        END AS [Custom_Root_Cost]
    FROM CustomRootCost
),
AggregatedData AS (
    -- Group by Subsidiary, Item No_ and Location Code,
    -- summing Inventory_Cost_LC and Quantity
    SELECT
        [Subsidiary],
        [Item No_],
        [Location Code],
        SUM([Inventory_Cost_LC]) AS [Inventory_Cost_LC],
        SUM([Quantity]) AS [Quantity]
    FROM FinalData
    GROUP BY
        [Subsidiary],
        [Item No_],
        [Location Code]
),
FilteredAggregated AS (
    -- Further filter the aggregated results:
    --   * Keep only rows with Quantity <> 0 (again)
    --   * Exclude rows where [Location Code] contains 'MRB'
    --   * Keep only rows where Subsidiary is 'US010'
    SELECT *
    FROM AggregatedData
    WHERE [Quantity] <> 0
      AND [Location Code] NOT LIKE '%MRB%'
      AND [Subsidiary] = 'US010'
)
-- Final grouping: remove the Location Code and Subsidiary details
-- and group by [Item No_] (renamed to No_), summing the Quantity as Inventory.
SELECT
    [Item No_] AS No_,
    SUM([Quantity]) AS Inventory
FROM FilteredAggregated
GROUP BY [Item No_]
ORDER BY [Item No_];
"""

# -----------------------------
# Step 4: get_inventory_data() function
# -----------------------------
def get_inventory_data():
    """
    Returns the DataFrame containing the inventory data
    loaded from the large SQL query above.
    """
    df = load_and_process_table(query=inventory_query, engine=engine)
    return df

# -----------------------------
# OPTIONAL: if run directly
# -----------------------------
if __name__ == "__main__":
    # If you run "python inventory_data.py" directly, it will fetch data and show a quick preview.
    inventory_df = get_inventory_data()
    if inventory_df is not None:
        print(inventory_df.head())
        print("Total records:", len(inventory_df))
