import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import numpy as np

# Load your full CSV (from scraping)
df = pd.read_csv("coingecko_bs4.csv")  # or "coingecko_selenium.csv"
print("Before cleaning:", len(df), "rows")


# --- Clean numeric columns ---
def clean_money(x):
    if isinstance(x, str):
        x = x.replace("$", "").replace(",", "").strip()
    try:
        return float(x)
    except:
        return None


def clean_pct(x):
    if isinstance(x, str):
        x = x.replace("%", "").strip()
    try:
        return float(x)
    except:
        return None


for col in ["Price", "Change_24h", "Change_7d", "Market_Cap"]:
    if "Change" in col:
        df[col] = df[col].apply(clean_pct)
    else:
        df[col] = df[col].apply(clean_money)

# Drop rows that are totally empty in these columns
df = df.dropna(subset=["Price", "Change_24h", "Change_7d", "Market_Cap"])
print("After cleaning:", len(df), "rows")

# --- Define top 20 well-known coins to TEST ---
# Based on your format: "BitcoinBTC", "EthereumETH", etc.
test_coins = [
    "BitcoinBTC",
    "EthereumETH",
    "TetherUSDT",
    "BNBBNB",
    "XRPXRP",
    "SolanaSOL",
    "USDCUSDC",
    "DogecoinDOGE",
    "CardanoADA",
    "TRONTRX",
    "ChainlinkLINK",
    "PolkadotDOT",
    "AvalancheAVAX",
    "Shiba InuSHIB",
    "LitecoinLTC",
    "UniswapUNI",
    "StellarXLM",
    "PolygonMATIC",
    "Bitcoin CashBCH",
    "AlgorandALGO"
]

# Make search case-insensitive
test_coins_lower = [coin.lower() for coin in test_coins]

# Split based on coin name (assuming you have a 'Name' or 'Coin' column)
# Adjust column name if needed: 'Name', 'Coin', 'Symbol', etc.
name_column = 'Name'  # Change this if your column is different

if name_column not in df.columns:
    print(f"⚠️  Column '{name_column}' not found. Available columns:")
    print(df.columns.tolist())
    # Try to find the right column
    possible_names = ['Name', 'Coin', 'Symbol', 'name', 'coin', 'symbol']
    for col in possible_names:
        if col in df.columns:
            name_column = col
            print(f"✅ Using column: {name_column}")
            break

# Create a mask for test coins
test_mask = df[name_column].str.lower().isin(test_coins_lower)

test_df = df[test_mask].copy()
train_df = df[~test_mask].copy()

print(f"\n📊 Training set: {len(train_df)} coins (excluding famous coins)")
print(f"📊 Test set: {len(test_df)} well-known coins")
print("\nTest coins found:")
print(test_df[name_column].tolist())

if len(test_df) < 10:
    print(f"\n⚠️  Warning: Only found {len(test_df)} test coins. They might have different names in your data.")
    print("Showing first 20 coin names from your data:")
    print(df[name_column].head(20).tolist())

# --- Prepare training data ---
X_train = train_df[["Price", "Change_24h", "Change_7d", "Market_Cap"]]

# Fit scaler on training data only
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)

# --- K-Means clustering (train on all coins except top 20) ---
kmeans = KMeans(n_clusters=5, random_state=42)
train_df["Cluster"] = kmeans.fit_predict(X_train_scaled)

print("\n✅ Training completed!")
print("Cluster distribution (Training):")
print(train_df["Cluster"].value_counts().sort_index())

# --- PCA for visualization (fit on training data) ---
pca = PCA(n_components=2)
reduced_train = pca.fit_transform(X_train_scaled)
train_df["PC1"], train_df["PC2"] = reduced_train[:, 0], reduced_train[:, 1]

# --- Predict clusters for test data (top 20 well-known coins) ---
if len(test_df) > 0:
    X_test = test_df[["Price", "Change_24h", "Change_7d", "Market_Cap"]]
    X_test_scaled = scaler.transform(X_test)  # Use same scaler
    test_df["Cluster"] = kmeans.predict(X_test_scaled)  # Predict clusters

    print("\n✅ Test set prediction completed!")
    print("Cluster distribution (Test - Famous Coins):")
    print(test_df["Cluster"].value_counts().sort_index())

    # Transform test data with same PCA
    reduced_test = pca.transform(X_test_scaled)
    test_df["PC1"], test_df["PC2"] = reduced_test[:, 0], reduced_test[:, 1]

    # Show which cluster each famous coin belongs to
    print("\n🌟 Famous Coins Cluster Assignment:")
    for _, row in test_df.iterrows():
        print(f"  {row[name_column]:20s} → Cluster {row['Cluster']}")

# --- Save results ---
train_df.to_csv("coins_clustered_train.csv", index=False)
if len(test_df) > 0:
    test_df.to_csv("coins_clustered_test.csv", index=False)

# Combine for full dataset with predictions
full_df = pd.concat([train_df, test_df], ignore_index=True)
full_df.to_csv("coins_clustered_full.csv", index=False)

print(f"\n💾 Saved:")
print(f"  - coins_clustered_train.csv ({len(train_df)} rows)")
if len(test_df) > 0:
    print(f"  - coins_clustered_test.csv ({len(test_df)} rows)")
print(f"  - coins_clustered_full.csv ({len(full_df)} rows)")

# --- Visualizations ---
if len(test_df) > 0:
    # Plot: Training data
    plt.figure(figsize=(14, 6))

    plt.subplot(1, 2, 1)
    plt.scatter(train_df["PC1"], train_df["PC2"], c=train_df["Cluster"],
                cmap="tab10", alpha=0.6, edgecolors='k', linewidth=0.5, s=20)
    plt.xlabel("PC1")
    plt.ylabel("PC2")
    plt.title(f"Training Set ({len(train_df)} coins)")
    plt.colorbar(label="Cluster")

    # Plot: Test data (famous coins)
    plt.subplot(1, 2, 2)
    scatter = plt.scatter(test_df["PC1"], test_df["PC2"], c=test_df["Cluster"],
                          cmap="tab10", alpha=0.8, edgecolors='k', linewidth=2, s=200, marker='*')
    plt.xlabel("PC1")
    plt.ylabel("PC2")
    plt.title(f"Test Set ({len(test_df)} Famous Coins)")
    plt.colorbar(label="Cluster")

    # Add labels for famous coins
    for _, row in test_df.iterrows():
        plt.annotate(row[name_column], (row["PC1"], row["PC2"]),
                     fontsize=8, ha='right', alpha=0.7)

    plt.tight_layout()
    plt.savefig("cluster_comparison.png", dpi=150)
    print("\n📊 Visualization saved as cluster_comparison.png")
    plt.show()

    # Combined visualization
    plt.figure(figsize=(12, 10))
    plt.scatter(train_df["PC1"], train_df["PC2"], c=train_df["Cluster"],
                cmap="tab10", alpha=0.3, label="Training coins", s=20)
    plt.scatter(test_df["PC1"], test_df["PC2"], c=test_df["Cluster"],
                cmap="tab10", alpha=1.0, marker="*", s=300,
                edgecolors='black', linewidth=2, label="Famous coins")

    # Add labels for famous coins
    for _, row in test_df.iterrows():
        plt.annotate(row[name_column], (row["PC1"], row["PC2"]),
                     fontsize=9, fontweight='bold', ha='right')

    plt.xlabel("PC1")
    plt.ylabel("PC2")
    plt.title("Combined View: Training vs Famous Test Coins")
    plt.legend()
    plt.savefig("cluster_combined.png", dpi=150)
    print("📊 Combined visualization saved as cluster_combined.png")
    plt.show()

# --- Cluster characteristics (for evaluation) ---
print("\n📈 Cluster Characteristics (Training Set):")
for cluster in sorted(train_df["Cluster"].unique()):
    cluster_data = train_df[train_df["Cluster"] == cluster]
    print(f"\nCluster {cluster} ({len(cluster_data)} coins):")
    print(f"  Price: ${cluster_data['Price'].mean():.2f} (avg)")
    print(f"  24h Change: {cluster_data['Change_24h'].mean():.2f}% (avg)")
    print(f"  7d Change: {cluster_data['Change_7d'].mean():.2f}% (avg)")
    print(f"  Market Cap: ${cluster_data['Market_Cap'].mean():,.0f} (avg)")

if len(test_df) > 0:
    print("\n📈 Cluster Characteristics (Test Set - Famous Coins):")
    for cluster in sorted(test_df["Cluster"].unique()):
        cluster_data = test_df[test_df["Cluster"] == cluster]
        coin_names = cluster_data[name_column].tolist()
        print(f"\nCluster {cluster}: {coin_names}")
        print(f"  Price: ${cluster_data['Price'].mean():.2f} (avg)")
        print(f"  24h Change: {cluster_data['Change_24h'].mean():.2f}% (avg)")
        print(f"  7d Change: {cluster_data['Change_7d'].mean():.2f}% (avg)")
        print(f"  Market Cap: ${cluster_data['Market_Cap'].mean():,.0f} (avg)")