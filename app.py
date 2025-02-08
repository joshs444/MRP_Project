import streamlit as st
import pandas as pd

# Import your existing MRP functions
from data_loader import load_bom_data, load_sales_orders
from bom_explosion import create_bom_hierarchy
from inventory_management import process_transactions

# App Title
st.title("MRP Tool Dashboard")

# When the button is pressed, the code runs the MRP process and saves the Excel files
if st.button("Run MRP Process"):
    st.write("Processing data...")

    # Load the necessary data
    bom_data = load_bom_data()
    sales_orders_df = load_sales_orders()

    # Create the BOM hierarchy based on the top-level sales order indices
    top_level_indices = sales_orders_df['Index'].tolist()
    bom_hierarchy_df, _ = create_bom_hierarchy(bom_data, top_level_indices)

    # Process transactions (this returns two DataFrames)
    final_df, updated_inventory_df = process_transactions(bom_hierarchy_df, "MRP Data.xlsx")

    # Remove duplicate columns from updated_inventory_df to avoid errors when displaying it
    updated_inventory_df = updated_inventory_df.loc[:, ~updated_inventory_df.columns.duplicated()]

    # Save the resulting DataFrames as Excel files in the current project folder
    final_excel_filename = "Final_Net_Requirements_Based_on_Inventory.xlsx"
    updated_inventory_filename = "Updated_Inventory.xlsx"
    final_df.to_excel(final_excel_filename, index=False)
    updated_inventory_df.to_excel(updated_inventory_filename, index=False)

    st.success("MRP Process Complete! Excel files saved in the project folder.")

    # Display results in the app
    st.subheader("Production Requirements")
    st.dataframe(final_df)
    st.subheader("Updated Inventory")
    st.dataframe(updated_inventory_df)
