# data_loader.py
import pandas as pd
from config import EXCEL_FILE, BOM_SHEET, SALES_ORDERS_SHEET, INVENTORY_SHEET, ITEM_TABLE_SHEET, PURCHASES_SHEET

def load_bom_data():
    try:
        bom_data = pd.read_excel(EXCEL_FILE, sheet_name=BOM_SHEET)
        # Adjust column names as expected
        bom_data.rename(columns={'Parent': 'Parent Index', 'Child': 'Child Index', 'Total': 'QTY Per'}, inplace=True)
        return bom_data
    except Exception as e:
        print(f"Error loading BOM data: {e}")
        return None

def load_sales_orders():
    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name=SALES_ORDERS_SHEET)
        # Remove duplicates and return top-level indices
        df = df.drop_duplicates(subset='Index')
        return df
    except Exception as e:
        print(f"Error loading Sales Orders: {e}")
        return None

def load_inventory():
    try:
        return pd.read_excel(EXCEL_FILE, sheet_name=INVENTORY_SHEET)
    except Exception as e:
        print(f"Error loading Inventory: {e}")
        return None

def load_item_table():
    try:
        return pd.read_excel(EXCEL_FILE, sheet_name=ITEM_TABLE_SHEET)
    except Exception as e:
        print(f"Error loading Item Table: {e}")
        return None

def load_purchases():
    try:
        return pd.read_excel(EXCEL_FILE, sheet_name=PURCHASES_SHEET)
    except Exception as e:
        print(f"Error loading Purchases: {e}")
        return None
