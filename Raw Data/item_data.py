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
# Step 3: The Item Query with Deterministic Indexing
# -----------------------------
item_query = """
WITH
-- 1. Process the Item table: filter out blank item numbers, add a deterministic index, and map the Replenishment System code to text
ItemsCTE AS (
    SELECT 
         ROW_NUMBER() OVER (ORDER BY [No_], [Revision No_]) AS ItemIndex,
         [No_],
         [Revision No_] AS [Rev#],
         [Replenishment System],
         CASE 
             WHEN [Replenishment System] = 0 THEN 'Purchase'
             WHEN [Replenishment System] = 1 THEN 'Output'
             WHEN [Replenishment System] = 2 THEN 'Assembly'
             ELSE 'Unknown'
         END AS ReplenishmentSystemText
    FROM [dbo].[IPG Photonics Corporation$Item]
    WHERE [No_] <> ''
),

-- 2. Aggregate Ledger Entry data for each item (only for entry types 0 and 6)
LedgerCTE AS (
    SELECT 
         [Item No_],
         SUM(CASE WHEN [Entry Type] = 0 THEN [Quantity] ELSE 0 END) AS Purchase,
         SUM(CASE WHEN [Entry Type] = 6 THEN [Quantity] ELSE 0 END) AS Output
    FROM [dbo].[IPG Photonics Corporation$Item Ledger Entry]
    WHERE [Posting Date] > '2019-01-01'
      AND [Posting Date] >= DATEADD(quarter, -8, GETDATE())
      AND [Entry Type] IN (0, 6)
    GROUP BY [Item No_]
),

-- 3. OpenPurchasesCTE: Use only the Purchase Line table (excluding purchase history) for open orders  
OpenPurchasesCTE AS (
    SELECT
         [No_] AS ItemNo,
         SUM([Quantity]) AS OpenQTY
    FROM [dbo].[IPG Photonics Corporation$Purchase Line]
    WHERE [Order Date] > '2019-01-01'
      AND [Quantity] > 0
      AND [Unit Cost (LCY)] > 0
      AND ([Document Type] = 1 OR [Document Type] = 5)
      AND ([Type] IN (1,2,4))
    GROUP BY [No_]
)

-- Final SELECT: join Items, Ledger, and Open Purchases data, and compute ReplenishmentAdjusted.
SELECT 
    i.ItemIndex,
    i.[No_],
    i.[Rev#],
    CASE 
         WHEN ISNULL(op.OpenQTY, 0) > 0 THEN 'Purchase'
         WHEN i.ReplenishmentSystemText = 'Purchase' AND ISNULL(l.Output, 0) > ISNULL(l.Purchase, 0) THEN 'Output'
         WHEN i.ReplenishmentSystemText = 'Output' AND ISNULL(l.Purchase, 0) > ISNULL(l.Output, 0) THEN 'Purchase'
         ELSE i.ReplenishmentSystemText
    END AS ReplenishmentAdjusted
FROM ItemsCTE i
LEFT JOIN LedgerCTE l
    ON i.[No_] = l.[Item No_]
LEFT JOIN OpenPurchasesCTE op
    ON i.[No_] = op.ItemNo
ORDER BY i.ItemIndex;
"""

# -----------------------------
# Step 4: get_item_data() Function
# -----------------------------
def get_item_data():
    """
    Returns the DataFrame containing the Item data
    loaded from the SQL query above.
    """
    df = load_and_process_table(query=item_query, engine=engine)
    return df

# -----------------------------
# OPTIONAL: if run directly
# -----------------------------
if __name__ == "__main__":
    # Running "python item_data.py" directly will fetch data and display a quick preview.
    item_df = get_item_data()
    if item_df is not None:
        print(item_df.head())
        print("Total records:", len(item_df))
