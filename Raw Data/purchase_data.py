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
# Step 3: The FULL Purchase Query
# -----------------------------
purchase_query = """
WITH LineData AS (
    ----------------------------------------
    -- US010: IPG Photonics CORPORATION
    ----------------------------------------
    SELECT
        'HISTORY' AS [Status],
        [Document Type],
        [Document No_],
        [Line No_],
        [Shortcut Dimension 1 Code],
        [Buy-from Vendor No_],
        [Type],
        [No_],
        [Location Code],
        [Expected Receipt Date],
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
                             END AS [Unit Cost],
        [Requested Receipt Date],
        CASE 
            WHEN 'HISTORY' = 'HISTORY' THEN ([Quantity] - [Outstanding Quantity]) * [Unit Cost]
            ELSE [Quantity] * [Unit Cost]
        END AS [Total],
        ([Quantity] - [Outstanding Quantity]) * CASE 
                                                    WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                                                    ELSE [Qty_ per Unit of Measure] 
                                                 END AS [Quantity Delivered],
        'US010' AS [Subsidiary]
    FROM [dbo].[IPG Photonics Corporation$Purchase History Line]
    WHERE
        [Order Date] > '2019-01-01'
        AND [Quantity] > 0
        AND [Unit Cost (LCY)] > 0
        AND ([Document Type] = 1 OR [Document Type] = 5)
        AND ([Type] = 1 OR [Type] = 2 OR [Type] = 4)
        AND NOT ([Quantity] - [Outstanding Quantity] = 0)

    UNION ALL

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
                             END AS [Unit Cost],
        [Requested Receipt Date],
        [Quantity] * [Unit Cost] AS [Total],
        ([Quantity] - [Outstanding Quantity]) * CASE 
                                                    WHEN [Qty_ per Unit of Measure] = 0 THEN 1 
                                                    ELSE [Qty_ per Unit of Measure] 
                                                 END AS [Quantity Delivered],
        'US010' AS [Subsidiary]
    FROM [dbo].[IPG Photonics Corporation$Purchase Line]
    WHERE
        [Order Date] > '2019-01-01'
        AND [Quantity] > 0
        AND [Unit Cost (LCY)] > 0
        AND ([Document Type] = 1 OR [Document Type] = 5)
        AND ([Type] = 1 OR [Type] = 2 OR [Type] = 4)
),
HeaderData AS (
    ----------------------------------------
    -- US010: IPG Photonics CORPORATION
    ----------------------------------------
    SELECT
        [Document Type],
        [No_],
        [Order Date],
        [Posting Date],
        [Assigned User ID],
        [Order Confirmation Date],
        [Purchaser Code],
        'US010' AS [Subsidiary]
    FROM [dbo].[IPG Photonics Corporation$Purchase History Header]
    WHERE
        [Order Date] > '2018-12-31'
        AND ([Document Type] = 1 OR [Document Type] = 5)
        AND [Buy-from Vendor No_] <> ''

    UNION ALL

    SELECT
        [Document Type],
        [No_],
        [Order Date],
        [Posting Date],
        [Assigned User ID],
        [Order Confirmation Date],
        [Purchaser Code],
        'US010' AS [Subsidiary]
    FROM [dbo].[IPG Photonics Corporation$Purchase Header]
    WHERE
        [Order Date] > '2018-12-31'
        AND ([Document Type] = 1 OR [Document Type] = 5)
        AND [Buy-from Vendor No_] <> ''
),
ReceiptTable AS (
    ----------------------------------------
    -- US010: IPG Photonics CORPORATION Purchase Receipt Line
    ----------------------------------------
    SELECT 
        [Line No_],
        [Order No_] AS [Order #],
        [No_] AS [Item #],
        [Posting Date]
    FROM (
        SELECT 
            [Line No_],
            [Order No_],
            [No_],
            [Posting Date],
            ROW_NUMBER() OVER (PARTITION BY [Line No_], [Order No_], [No_] ORDER BY [Posting Date]) AS RowNumber
        FROM [dbo].[IPG Photonics Corporation$Purch_ Rcpt_ Line]
        WHERE [Quantity] > 0
    ) AS InnerQuery
    WHERE InnerQuery.RowNumber = 1
      AND YEAR([Posting Date]) > 2017
)
SELECT
    LineData.[Status],
    LineData.[Document Type],
    LineData.[Document No_],
    LineData.[Line No_],
    LineData.[Buy-from Vendor No_],
    LineData.[Type],
    LineData.[No_],
    LineData.[Shortcut Dimension 1 Code] AS [Cost Center],
    LineData.[Location Code],
    LineData.[Expected Receipt Date],
    LineData.[Promised Receipt Date],
    LineData.[Description],
    LineData.[Qty_ per Unit of Measure],
    LineData.[Quantity],
    LineData.[Outstanding Quantity],
    LineData.[Unit Cost],
    LineData.[Requested Receipt Date],
    LineData.[Total],
    LineData.[Planned Receipt Date],
    LineData.[Quantity Delivered],
    HeaderData.[Order Date],
    ReceiptTable.[Posting Date],
    HeaderData.[Assigned User ID],
    HeaderData.[Order Confirmation Date],
    HeaderData.[Purchaser Code],
    LineData.[Subsidiary]
FROM LineData
JOIN HeaderData
    ON LineData.[Document No_] = HeaderData.[No_]
LEFT JOIN ReceiptTable
    ON LineData.[Document No_] = ReceiptTable.[Order #]
    AND LineData.[Line No_] = ReceiptTable.[Line No_]
    AND LineData.[No_] = ReceiptTable.[Item #]
ORDER BY
    LineData.[Document No_], LineData.[Line No_];

"""

# -----------------------------
# Step 4: get_purchase_data() function
# -----------------------------
def get_purchase_data():
    """
    Returns the DataFrame containing the purchase data
    loaded from the large SQL query above.
    """
    df = load_and_process_table(query=purchase_query, engine=engine)
    return df

# -----------------------------
# OPTIONAL: if run directly
# -----------------------------
if __name__ == "__main__":
    # If you run "python purchase_data.py" directly, it will fetch data and show a quick preview.
    purchase_df = get_purchase_data()
    if purchase_df is not None:
        print(purchase_df.head())
        print("Total records:", len(purchase_df))
