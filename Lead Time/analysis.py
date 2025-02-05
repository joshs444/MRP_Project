# analysis.py (for example)

from datetime import datetime
import pandas as pd
from purchase_data import get_purchase_data

def main():
    # 1. Load the DataFrame
    df = get_purchase_data()

    # 2. Convert Order Date to datetime if needed
    df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')

    # 3. Calculate the "36 months ago" cutoff date
    #    This uses today's date minus 36 months (3 years).
    thirty_six_months_ago = pd.Timestamp.now() - pd.DateOffset(months=36)

    # 4. Filter the DataFrame to rows with Order Date >= that cutoff
    df_filtered = df[df['Order Date'] >= thirty_six_months_ago]

    # 5. Group by Vendor No and Item No
    grouped = df_filtered.groupby(['Buy-from Vendor No_', 'No_']).agg({
        'Total': 'sum',
        'Quantity': 'sum'
    }).reset_index()

    # 6. Calculate Average Unit Price = sum(Total) / sum(Quantity)
    grouped['Avg Unit Price'] = grouped['Total'] / grouped['Quantity']

    # 7. Inspect the result
    print("Top 10 by Average Unit Price:")
    print(grouped.sort_values('Avg Unit Price', ascending=False).head(10))

if __name__ == "__main__":
    main()
