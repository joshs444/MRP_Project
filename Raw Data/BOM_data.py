import pandas as pd
from sqlalchemy import create_engine
from item_data import get_item_data  # Import the function from your item_data.py module

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
# Step 3: The BOM Query
# -----------------------------
bom_query = """
SELECT 
    p.[Production BOM No_],
    p.[Version Code],
    p.[No_],
    p.[Quantity per]
FROM 
    [dbo].[IPG Photonics Corporation$Production BOM Line] p
INNER JOIN 
    [dbo].[IPG Photonics Corporation$Item] i
    ON p.[No_] = i.[No_] AND p.[Version Code] = i.[Revision No_]
WHERE 
    p.[Quantity per] <> 0;
"""


def get_bom_data():
    """
    Returns the DataFrame containing the BOM data
    loaded from the SQL query above.
    """
    df = load_and_process_table(query=bom_query, engine=engine)
    return df


# -----------------------------
# Step 4: Merge BOM Data with Item Data and Filter Out "Purchase"
# -----------------------------
def get_processed_bom_data():
    """
    Merges the BOM data with the computed Item data (from item_data.py)
    by matching the BOM's Production BOM No_ with the Item's No_.
    Any BOM rows with a ReplenishmentAdjusted value of "Purchase" are removed.
    """
    bom_df = get_bom_data()
    if bom_df is None:
        print("BOM data could not be loaded.")
        return None

    # Use the imported function to get item data (with ReplenishmentAdjusted)
    item_df = get_item_data()
    if item_df is None:
        print("Item data could not be loaded.")
        return bom_df  # Or handle as needed

    # Merge the BOM data with the item data.
    # Adjust the join keys as per your actual logic. Here we assume that
    # the BOM's "Production BOM No_" should match the item's "No_".
    merged_df = bom_df.merge(
        item_df[['No_', 'ReplenishmentAdjusted']],
        left_on='Production BOM No_',
        right_on='No_',
        how='left',
        suffixes=('', '_item')
    )

    # Optionally drop the extra "No_" column coming from the item data.
    merged_df.drop(columns=['No_item'], inplace=True, errors='ignore')

    # Filter out rows where ReplenishmentAdjusted is "Purchase"
    final_df = merged_df[merged_df['ReplenishmentAdjusted'] != 'Purchase']

    return final_df


# -----------------------------
# OPTIONAL: if run directly
# -----------------------------
if __name__ == "__main__":
    processed_bom_df = get_processed_bom_data()
    if processed_bom_df is not None:
        print(processed_bom_df.head())
        print("Total records:", len(processed_bom_df))
