import pandas as pd

df = pd.read_csv("coins_clustered_full.csv")

# Count coins per cluster
print(df['Cluster'].value_counts())

# Find representative coins of each cluster
for c in sorted(df['Cluster'].unique()):
    print(f"\nCluster {c} sample coins:")
    print(df[df['Cluster'] == c][['Name', 'Change_24h', 'Change_7d', 'Market_Cap']].head(5))

print("Total coins:", len(df))
print(df['Cluster'].value_counts())
