# item_mapping.py
import pandas as pd

def merge_and_rename(bom_df, item_df, merge_column, new_column_prefix):
    merged_df = bom_df.merge(
        item_df[['Item Index', 'No_', 'Rev #']],
        left_on=merge_column,
        right_on='Item Index',
        how='left'
    )
    merged_df.rename(
        columns={
            'No_': f'{new_column_prefix} No_',
            'Rev #': f'{new_column_prefix} Rev #'
        },
        inplace=True
    )
    return merged_df

def create_item_hierarchy(bom_hierarchy_df, item_table_df):
    merged_main = merge_and_rename(bom_hierarchy_df, item_table_df, 'Production Index', 'Production')
    merged_parent = merge_and_rename(bom_hierarchy_df, item_table_df, 'Parent Index', 'Parent')
    merged_child = merge_and_rename(bom_hierarchy_df, item_table_df, 'Child Index', 'Child')

    bom_itemhierarchy_df = bom_hierarchy_df.copy()
    bom_itemhierarchy_df['Production No_'] = merged_main['Production No_']
    bom_itemhierarchy_df['Production Rev #'] = merged_main['Production Rev #']

    bom_itemhierarchy_df['Parent No_'] = merged_parent['Parent No_']
    bom_itemhierarchy_df['Parent Rev #'] = merged_parent['Parent Rev #']

    bom_itemhierarchy_df['Child No_'] = merged_child['Child No_']
    bom_itemhierarchy_df['Child Rev #'] = merged_child['Child Rev #']

    desired_order = [
        'Order',
        'Production No_',
        'Production Rev #',
        'Level',
        'Parent No_',
        'Parent Rev #',
        'Child No_',
        'Child Rev #',
        'QTY Per',
        'Total Quantity'
    ]
    bom_itemhierarchy_df = bom_itemhierarchy_df[desired_order]
    return bom_itemhierarchy_df

def save_bom_item(bom_itemhierarchy_df, output_file):
    bom_itemhierarchy_df.to_excel(output_file, index=False)
