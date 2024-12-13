import pandas as pd

# Example DataFrames
df1 = pd.DataFrame({
    'id': [1, 2, 3],
    'text': ["apple pie", "banana bread", "cherry tart"]
})

df2 = pd.DataFrame({
    'id': [1, 1, 2, 3, 3],
    'text': ["apple pies", "apple tart", "banana loaf", "cherry tarte", "berry tart"]
})

# First, do an inner merge on 'id' to get candidate pairs
merged = pd.merge(df1, df2, on='id', suffixes=('_df1', '_df2'))


from fuzzywuzzy import fuzz

# Compute similarity scores
merged['similarity'] = merged.apply(lambda row: fuzz.partial_ratio(row['text_df1'], row['text_df2']), axis=1)


best_matches = best_matches[best_matches['similarity'] >= 80]
