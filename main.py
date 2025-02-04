# main.py

from data_loader import load_bom_data, load_sales_orders, load_item_table
from bom_explosion import create_bom_hierarchy
from inventory_management import process_transactions
from config import EXCEL_FILE

def main():
    # Load BOM data and sales orders using functions from data_loader
    bom_data = load_bom_data()
    sales_orders_df = load_sales_orders()

    if bom_data is None or sales_orders_df is None:
        print("Error loading BOM or Sales Orders. Exiting.")
        return

    # Create the BOM hierarchy using top-level indices from the sales orders
    top_level_indices = sales_orders_df['Index'].tolist()
    bom_hierarchy_df, _ = create_bom_hierarchy(bom_data, top_level_indices)

    # Process transactions (net requirements and inventory updates)
    final_df, updated_inventory_df = process_transactions(bom_hierarchy_df, EXCEL_FILE)
    print("Processing complete. Check output files for net requirements and updated inventory.")

if __name__ == '__main__':
    main()
