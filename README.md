# MRP Tool: BOM Explosion & Inventory Management

**MRP Tool** is a Python-based Manufacturing Resource Planning (MRP) system designed to:

- **Explode Bill of Materials (BOM):** Recursively build a complete hierarchical view of production items and their subcomponents.
- **Process Sales Orders & Purchases:** Merge exploded BOM data with sales orders and purchase transactions to calculate net production requirements.
- **Manage Inventory:** Adjust on-hand inventory by consuming available stock and processing incoming purchases.
- **Generate Detailed Reports:** Export comprehensive Excel reports including the fully exploded BOM, net requirements based on inventory, and updated inventory levels.

---

## Project Structure

The project is organized into modular Python files, each responsible for specific aspects of the MRP process:

- **`config.py`**  
  Contains configuration settings such as file paths, sheet names, output file names, and constants. This centralizes all configurable parameters.

- **`data_loader.py`**  
  Loads data from an Excel workbook (`MRP Data.xlsx`). It reads the BOM, Sales Orders, Inventory, Item Table, and Purchases sheets into Pandas DataFrames, performing any necessary pre-processing (e.g., renaming columns).

- **`bom_explosion.py`**  
  Implements the logic to "explode" the BOM:
  - **Recursive BOM Explosion:** Builds a hierarchical, indented view of the BOM.
  - **Quantity Calculations:** Computes total quantities at each level.
  - **Circular Reference Handling:** Checks and prevents infinite loops due to circular references.
  - **Output:** Generates a fully blown out BOM index in a DataFrame (and eventually an Excel file).

- **`inventory_management.py`**  
  Manages the integration of production orders with inventory:
  - **Inventory Preparation:** Sets up inventory tracking (initial, used, available).
  - **Sales Order Adjustments:** Updates production quantities based on available inventory.
  - **Transaction Processing:** Handles both production (sales orders) and purchase transactions to compute net requirements.
  - **Final Outputs:** Exports two key Excel files: one with net production requirements and one with updated inventory.

- **`item_mapping.py`**  
  Enhances the BOM data by merging it with item details from the Item Table:
  - **Item Mapping:** Maps internal indices to item numbers and revision numbers.
  - **BOM Item Hierarchy:** Creates an enriched BOM report with human-readable item details.

- **`main.py`**  
  Serves as the entry point of the project:
  - Orchestrates data loading, BOM explosion, transaction processing, and reporting.
  - Notifies the user upon successful completion and output file generation.

---

## How It Works

1. **Data Import:**  
   Reads the Excel workbook (`MRP Data.xlsx`) containing all necessary data (BOM, Sales Orders, Inventory, etc.).

2. **BOM Explosion:**  
   Starting from the top-level production items (derived from sales orders), the BOM is recursively exploded to reveal all subcomponents and calculate their total required quantities.

3. **Inventory & Order Processing:**  
   Sales orders are merged with the exploded BOM, and available inventory is consumed to determine net production requirements. Purchase transactions are also processed to update inventory levels.

4. **Item Mapping & Reporting:**  
   The exploded BOM is enriched with item numbers and revision data. Final outputs are then generated:
   - **Fully Blown Out BOM Index**
   - **Final Net Requirements Based on Inventory**
   - **Updated Inventory**

---

## Installation & Dependencies

- **Python 3.7+**
- **Pandas**
- **NumPy**
- **openpyxl**

To install dependencies, run:
```bash
pip install pandas numpy openpyxl
