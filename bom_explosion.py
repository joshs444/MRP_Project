# bom_explosion.py
import pandas as pd

def check_for_circular_reference(path, component_index):
    return component_index in path

def build_indented_bom(bom, main_number, parent_index, level=0, parent_qty=1, bom_hierarchy=None, path=None, circular_references=None):
    if bom_hierarchy is None:
        bom_hierarchy = []
    if path is None:
        path = []
    if circular_references is None:
        circular_references = set()

    # Track the current path
    path.append(parent_index)
    components = bom[bom['Parent Index'] == parent_index]

    for _, component in components.iterrows():
        component_index = component['Child Index']
        # Avoid circular references
        if check_for_circular_reference(path, component_index):
            circular_references.add((parent_index, component_index))
            continue

        component_total_qty = component['QTY Per'] * parent_qty
        bom_hierarchy.append({
            'Production Index': main_number,
            'Level': level,
            'Parent Index': parent_index,
            'Child Index': component_index,
            'QTY Per': component['QTY Per'],
            'Total Quantity': component_total_qty
        })

        # Recursively process subcomponents
        if bom['Parent Index'].eq(component_index).any():
            build_indented_bom(bom, main_number, component_index, level + 1, component_total_qty, bom_hierarchy, path, circular_references)
    path.pop()
    return bom_hierarchy, circular_references

def create_bom_hierarchy(bom_data, top_level_indices):
    bom_hierarchy_list = []
    circular_references_set = set()
    processed_indices = set()

    for index in top_level_indices:
        if index not in processed_indices:
            hierarchy, circular_refs = build_indented_bom(bom_data, index, index)
            bom_hierarchy_list.extend(hierarchy)
            circular_references_set.update(circular_refs)
            processed_indices.add(index)

    # Convert the list into a DataFrame and add an order column
    bom_hierarchy_df = pd.DataFrame(bom_hierarchy_list)
    bom_hierarchy_df.insert(0, 'Order', range(1, len(bom_hierarchy_df) + 1))
    bom_hierarchy_df.reset_index(drop=True, inplace=True)
    return bom_hierarchy_df, circular_references_set

def save_bom_index(bom_hierarchy_df, output_file):
    bom_hierarchy_df.to_excel(output_file, index=False)
