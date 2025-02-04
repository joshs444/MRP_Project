# inventory_management.py
import pandas as pd
import numpy as np


def prepare_inventory(inventory_df):
    """
    Prepare the inventory DataFrame:
      - Create a copy of the original inventory.
      - Create 'Used' and 'Available' columns as floats (to avoid dtype issues).
      - Set 'Index' as the DataFrame index.
    """
    inventory_df['Initial Inventory'] = inventory_df['Inventory'].copy()
    inventory_df['Used'] = 0.0  # Using float so decimals can be stored
    inventory_df['Available'] = inventory_df['Inventory'].astype(float)
    inventory_df.set_index('Index', inplace=True)
    return inventory_df


def adjust_production_qty(row, inventory_df):
    """
    For each sales order row, reduce the required quantity by the available inventory
    and update inventory usage accordingly.
    Returns a Series with 'Production QTY' and 'Inventory Used'.
    """
    item_index = row['Index']
    qty_needed = row['Open Sales QTY']
    if item_index in inventory_df.index:
        available_inventory = inventory_df.at[item_index, 'Available']
        adjusted_qty = max(qty_needed - available_inventory, 0)
        used_inventory = min(qty_needed, available_inventory)
        inventory_df.at[item_index, 'Used'] += used_inventory
        inventory_df.at[item_index, 'Available'] -= used_inventory
        inventory_used = used_inventory
    else:
        adjusted_qty = qty_needed
        inventory_used = 0
    return pd.Series({'Production QTY': adjusted_qty, 'Inventory Used': inventory_used})


def prepare_sales_orders(sales_orders_df, inventory_df):
    """
    Prepare the sales orders DataFrame:
      - Rename the quantity column to 'Open Sales QTY'.
      - Sort orders by date.
      - Adjust production quantities based on available inventory.
    """
    sales_orders_df = sales_orders_df.rename(columns={'QTY': 'Open Sales QTY'})
    sales_orders_df = sales_orders_df.sort_values(by='Date')
    sales_orders_df[['Production QTY', 'Inventory Used']] = sales_orders_df.apply(
        lambda row: adjust_production_qty(row, inventory_df), axis=1)
    return sales_orders_df


def process_order(df_order, inventory_df, max_level):
    """
    Process a group of sales orders (non-purchase transactions) by:
      - Consuming inventory.
      - Computing net requirements.
      - Updating inventory.
    """
    for level in range(0, max_level + 1):
        current_level_df = df_order[df_order['Level'] == level]
        if current_level_df.empty:
            continue
        for idx, row in current_level_df.iterrows():
            child_index = row['Child Index']
            # Retrieve the initial net requirement from the row.
            # (This column should have been created earlier.)
            initial_net_requirement = row['Initial Net Requirements']
            parent_index = row['Parent Index']

            # Determine the ratio from the parent row (or 1.0 at top-level)
            if level == 0:
                ratio_prior_level = 1.0
            else:
                parent_rows = df_order[
                    (df_order['Production Index'] == row['Production Index']) &
                    (df_order['Child Index'] == parent_index)
                    ]
                ratio_prior_level = parent_rows['Stock Ratio'].values[0] if not parent_rows.empty else 1.0

            df_order.at[idx, 'Ratio Prior Level'] = ratio_prior_level
            net_requirement = initial_net_requirement * ratio_prior_level

            if pd.notnull(child_index) and child_index in inventory_df.index:
                available_inventory = inventory_df.at[child_index, 'Available']
                used_inventory = min(net_requirement, available_inventory)
                inventory_df.at[child_index, 'Used'] += used_inventory
                inventory_df.at[child_index, 'Available'] -= used_inventory
                net_requirement = max(net_requirement - used_inventory, 0)
                df_order.at[idx, 'Net Requirements'] = net_requirement
                df_order.at[idx, 'Inventory Used'] = used_inventory
                stock_ratio = net_requirement / initial_net_requirement if initial_net_requirement != 0 else 0
                df_order.at[idx, 'Stock Ratio'] = stock_ratio
                df_order.at[idx, 'Updated Inventory'] = inventory_df.at[child_index, 'Available']
            else:
                df_order.at[idx, 'Inventory Used'] = 0.0
                stock_ratio = net_requirement / initial_net_requirement if initial_net_requirement != 0 else 0
                df_order.at[idx, 'Stock Ratio'] = stock_ratio
                df_order.at[idx, 'Net Requirements'] = net_requirement
                df_order.at[idx, 'Updated Inventory'] = np.nan
    return df_order, inventory_df


def process_purchase(df_order, inventory_df):
    """
    Process a purchase transaction group by:
      - Increasing on-hand inventory.
      - Recording the updated inventory.
    """
    for idx, row in df_order.iterrows():
        child_index = row['Child Index']
        qty_purchased = row['Production QTY']
        if pd.notnull(child_index):
            if child_index in inventory_df.index:
                inventory_df.at[child_index, 'Available'] += qty_purchased
                inventory_df.at[child_index, 'Inventory'] += qty_purchased
            else:
                inventory_df.loc[child_index] = {
                    'Inventory': qty_purchased,
                    'Used': 0.0,
                    'Available': qty_purchased,
                    'Initial Inventory': 0
                }
            df_order.at[idx, 'Updated Inventory'] = inventory_df.at[child_index, 'Available']
        else:
            df_order.at[idx, 'Updated Inventory'] = np.nan
        df_order.at[idx, 'Net Requirements'] = 0
        df_order.at[idx, 'Inventory Used'] = -qty_purchased
    return df_order, inventory_df


def process_transactions(fully_blow_out_df, mrp_data_file):
    """
    Process transactions by:
      - Loading Sales Orders, Inventory, Item Table, and Purchases from the MRP data file.
      - Merging Sales Orders with the fully exploded BOM.
      - Converting Purchases to a BOM-like structure.
      - Processing orders (consuming inventory) and purchases.
      - Mapping item indices to item numbers.
      - Exporting the final net requirements and updated inventory.

    Returns the final processed DataFrame and the updated inventory DataFrame.
    """
    # Load additional data from the MRP data file
    items_to_produce_df = pd.read_excel(mrp_data_file, sheet_name='Sales Orders')
    inventory_df = pd.read_excel(mrp_data_file, sheet_name='Inventory')
    item_table_df = pd.read_excel(mrp_data_file, sheet_name='Item Table')
    purchases_df = pd.read_excel(mrp_data_file, sheet_name='Purchases')

    # Prepare inventory and sales orders
    inventory_df = prepare_inventory(inventory_df)
    items_to_produce_df = prepare_sales_orders(items_to_produce_df, inventory_df)

    # --- Handle Sales Items with No BOM ---
    bom_parents = fully_blow_out_df['Production Index'].unique()
    sales_items = items_to_produce_df['Index'].unique()
    items_without_bom = np.setdiff1d(sales_items, bom_parents)
    if len(items_without_bom) > 0:
        no_bom_df = pd.DataFrame({
            'Order': 1,
            'Production Index': items_without_bom,
            'Level': 0,
            'Parent Index': items_without_bom,
            'Child Index': items_without_bom,
            'QTY Per': 1,
            'Total Quantity': 1
        })
        fully_blow_out_df = pd.concat([fully_blow_out_df, no_bom_df], ignore_index=True)

    # --- Convert Purchases into a BOM-like Structure ---
    purchases_df['Transaction Type'] = 'Purchase'
    purchases_df['Order'] = 1  # Arbitrary
    purchases_bom_df = pd.DataFrame({
        'Order': purchases_df['Order'],
        'Production Index': purchases_df['Index'],
        'Level': 0,
        'Parent Index': np.nan,
        'Child Index': purchases_df['Index'],
        'QTY Per': 1,
        'Total Quantity': 1,
        'Production QTY': purchases_df['QTY'],
        'Inventory Used': 0,
        'Date': purchases_df['Expected Receipt Date'],
        'Document No_': purchases_df['Document No_'],
        'Transaction Type': purchases_df['Transaction Type'],
        'Open Sales QTY': 0
    })

    # --- Mark Transaction Types for Sales Orders ---
    items_to_produce_df['Transaction Type'] = np.where(
        items_to_produce_df['Index'].isin(items_without_bom),
        'Non Production Items',
        'Production Items'
    )

    # --- Merge Sales Orders into the BOM Hierarchy ---
    merged_sales_df = pd.merge(
        fully_blow_out_df,
        items_to_produce_df[['Index', 'Open Sales QTY', 'Production QTY', 'Inventory Used', 'Date', 'Document No_',
                             'Transaction Type']],
        left_on='Production Index',
        right_on='Index',
        how='inner'
    )
    merged_sales_df = merged_sales_df.drop(columns=['Index'])

    # --- Combine Sales Orders and Purchases ---
    merged_df = pd.concat([merged_sales_df, purchases_bom_df], ignore_index=True)
    # Ensure 'Production QTY' and 'Total Quantity' exist and fill NaNs with 0 before calculation
    merged_df['Production QTY'] = merged_df['Production QTY'].fillna(0)
    merged_df['Total Quantity'] = merged_df['Total Quantity'].fillna(0)

    # Calculate initial net requirements
    merged_df['Initial Net Requirements'] = merged_df['Total Quantity'] * merged_df['Production QTY']
    merged_df['Net Requirements'] = merged_df['Initial Net Requirements']
    merged_df['Stock Ratio'] = 1.0
    merged_df['Ratio Prior Level'] = 1.0

    # Ensure 'Inventory Used' column exists
    if 'Inventory Used' not in merged_df.columns:
        merged_df['Inventory Used'] = 0.0

    # Fill NaN values in 'Production Index' for grouping
    merged_df['Production Index'] = merged_df['Production Index'].fillna(merged_df['Child Index'])

    # Sort the DataFrame
    merged_df.sort_values(by=['Date', 'Transaction Type', 'Production Index', 'Order'], inplace=True)
    merged_df['Order Processed'] = merged_df.groupby(
        ['Date', 'Document No_', 'Production Index', 'Transaction Type']
    ).ngroup() + 1
    max_level = merged_df['Level'].max()

    # --- Process Each Transaction Group ---
    processed_orders = []
    for order_processed in sorted(merged_df['Order Processed'].unique()):
        df_order = merged_df[merged_df['Order Processed'] == order_processed].copy()
        if df_order.empty:
            continue
        transaction_type = df_order['Transaction Type'].iloc[0]
        if transaction_type == 'Purchase':
            df_order, inventory_df = process_purchase(df_order, inventory_df)
        else:
            df_order, inventory_df = process_order(df_order, inventory_df, max_level)
        processed_orders.append(df_order)

    final_df = pd.concat(processed_orders, ignore_index=True)
    final_df = final_df.drop(columns=['Order Processed'])

    # --- Final Cleanup and Renaming ---
    if 'Order' in final_df.columns:
        final_df = final_df.drop(columns=['Order'])
    final_df = final_df.reset_index(drop=True)
    final_df['Order'] = range(1, len(final_df) + 1)

    # Map item indices to item numbers using the Item Table
    item_index_to_no_dict = dict(zip(item_table_df['Item Index'], item_table_df['No_']))
    for col in ['Production Index', 'Child Index', 'Parent Index']:
        if col in final_df.columns:
            final_df[col] = final_df[col].map(item_index_to_no_dict)

    final_df['Child Index'] = final_df['Child Index'].fillna('')
    final_df['Parent Index'] = final_df['Parent Index'].fillna('')
    final_df['Production Index'] = final_df['Production Index'].fillna('')

    final_df.loc[final_df['Transaction Type'] == 'Purchase', 'Production Index'] = ''
    final_df.loc[final_df['Transaction Type'] == 'Purchase', 'Parent Index'] = ''

    final_df = final_df.rename(columns={
        'Production Index': 'Production Item',
        'Child Index': 'Child Item',
        'Parent Index': 'Parent Item'
    })
    cols = final_df.columns.tolist()
    cols = ['Transaction Type', 'Order'] + [c for c in cols if c not in ['Transaction Type', 'Order']]
    final_df = final_df[cols]

    # --- Export Final Results ---
    MAX_ROWS_PER_CHUNK = 500000
    num_chunks = int(np.ceil(len(final_df) / MAX_ROWS_PER_CHUNK))
    with pd.ExcelWriter('Final_Net_Requirements_Based_on_Inventory.xlsx', engine='openpyxl') as writer:
        for i in range(num_chunks):
            start_row = i * MAX_ROWS_PER_CHUNK
            end_row = min((i + 1) * MAX_ROWS_PER_CHUNK, len(final_df))
            chunk = final_df.iloc[start_row:end_row]
            sheet_name = f'Data_Part_{i + 1}'
            chunk.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"Data saved into {num_chunks} sheet(s) in 'Final_Net_Requirements_Based_on_Inventory.xlsx'.")

    # Export the updated inventory
    inventory_df = inventory_df.reset_index()
    inventory_df['Index'] = inventory_df['Index'].map(item_index_to_no_dict)
    inventory_df = inventory_df.rename(columns={'Index': 'No_'})
    inventory_df.to_excel('Updated_Inventory.xlsx', index=False)
    print(
        "Calculation and adjustment complete. Results saved to 'Final_Net_Requirements_Based_on_Inventory.xlsx' and 'Updated_Inventory.xlsx'.")

    return final_df, inventory_df
