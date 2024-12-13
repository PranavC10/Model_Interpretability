import pandas as pd
from rapidfuzz import process, fuzz

def map_vita_to_mandates(df_vita, df_mandates, raw_column, id_column, name_column, threshold=80):
    """
    Maps raw_mandate_name from the VITA dataset to src_sys_vldtn_id and vldtn_item_nm in mandates.

    Args:
        df_vita (pd.DataFrame): VITA dataset with raw mandate names.
        df_mandates (pd.DataFrame): Mandate dataset with IDs and mandate names.
        raw_column (str): Column name in VITA dataset for raw mandate names.
        id_column (str): Column name in mandates dataset for mandate IDs.
        name_column (str): Column name in mandates dataset for mandate names.
        threshold (int): Minimum similarity score for matching (default: 80).

    Returns:
        pd.DataFrame: A DataFrame with matched raw_mandate_name, src_sys_vldtn_id, and vldtn_item_nm.
    """
    matches = []

    for idx, row in df_vita.iterrows():
        raw_mandate = row[raw_column]
        
        # Combine mandate ID and name for matching
        mandate_candidates = df_mandates.apply(
            lambda x: f"{x[id_column]} - {x[name_column]}", axis=1
        )
        
        # Find the best match
        best_match = process.extractOne(
            raw_mandate, mandate_candidates, scorer=fuzz.partial_ratio
        )
        
        if best_match and best_match[1] >= threshold:
            matched_index = mandate_candidates[mandate_candidates == best_match[0]].index[0]
            matches.append({
                "raw_mandate_name": raw_mandate,
                "src_sys_vldtn_id": df_mandates.loc[matched_index, id_column],
                "vldtn_item_nm": df_mandates.loc[matched_index, name_column],
                "similarity_score": best_match[1]
            })

    return pd.DataFrame(matches)

# Example usage
# df_vita = pd.read_csv('./data/vita.csv')  # Load VITA dataset
# df_mandates = pd.read_csv('./data/mandates.csv')  # Load Mandate dataset

result = map_vita_to_mandates(
    df_vita, df_mandates,
    raw_column="raw_mandate_name",
    id_column="src_sys_vldtn_id",
    name_column="vldtn_item_nm",
    threshold=85
)

# Display or save result
print(result)
# result.to_csv('./data/mapped_result.csv', index=False)
