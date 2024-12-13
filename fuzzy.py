import pandas as pd
from rapidfuzz import fuzz, process

def fuzzy_join_datasets(
    df1, df2, 
    column1, column2, 
    id1, id2, 
    matching_function=fuzz.ratio, 
    threshold=80
):
    """
    Perform a fuzzy join between two datasets based on the given columns.
    
    Args:
        df1 (pd.DataFrame): First dataset.
        df2 (pd.DataFrame): Second dataset.
        column1 (str): Column name in df1 to match.
        column2 (str): Column name in df2 to match.
        id1 (str): ID column name in df1.
        id2 (str): ID column name in df2.
        matching_function (function): Fuzzy matching function (default: fuzz.ratio).
        threshold (int): Matching threshold (default: 80).
        
    Returns:
        pd.DataFrame: Joined dataset with matched rows and similarity scores.
    """
    matches = []
    
    # Iterate through rows in df1
    for idx1, row1 in df1.iterrows():
        best_match = None
        best_score = 0
        
        # Compare with rows in df2
        for idx2, row2 in df2.iterrows():
            score = matching_function(row1[column1], row2[column2])
            if score > threshold and score > best_score:
                best_match = (row1[id1], row1[column1], row2[id2], row2[column2], score)
                best_score = score
        
        if best_match:
            matches.append(best_match)
    
    # Create a DataFrame from matches
    result_df = pd.DataFrame(matches, columns=[f"{id1}_df1", column1, f"{id2}_df2", column2, "similarity_score"])
    return result_df


# Example datasets
data1 = {
    "id": [1, 2, 3],
    "name": ["Alice Johnson", "Bob Smith", "Charlie Brown"]
}

data2 = {
    "id": ["A", "B", "C"],
    "name": ["Alice Jonson", "Bobby Smith", "Charles Brown"]
}

df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)

# Perform fuzzy join
result = fuzzy_join_datasets(
    df1, df2, 
    column1="name", column2="name", 
    id1="id", id2="id", 
    matching_function=fuzz.ratio, 
    threshold=85
)

# Display result
print(result)


