import pandas as pd

# Assuming the files are already downloaded as per the provided code
k = pd.read_csv('latest-btw25-bw-kreise.csv', encoding="latin-1")
g = pd.read_csv('latest-btw25-bw-gemeinden.csv', encoding="latin-1")

# Get column names
k_cols = k.columns
g_cols = g.columns

# Find the common columns
common_cols = set(k_cols) & set(g_cols)

# prompt: Create new columns in data frame k based on the column names in common_cols by appending the text _rel to each name. Compute the contents of the columns by dividing each column in common_column that ends in _Erstimmen with the value of Gültige_Erststimmen and dividing each column in common_column that ends in _Zweitstimmen with the value of Gültige_Zweitstimmen

for col in common_cols:
    if col.endswith("_Erststimmen"):
        k[col + "_rel"] = k[col] / k["Gültige_Erststimmen"]
        g[col + "_rel"] = g[col] / g["Gültige_Erststimmen"]
    elif col.endswith("_Zweitstimmen"):
        k[col + "_rel"] = k[col] / k["Gültige_Zweitstimmen"]
        g[col + "_rel"] = g[col] / g["Gültige_Zweitstimmen"]

k.to_csv('latest-btw25-bw-kreise-rel.csv', encoding='latin-1', index=False)
g.to_csv('latest-btw25-bw-gemeinden-rel.csv', encoding='latin-1', index=False)