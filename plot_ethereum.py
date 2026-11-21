import pandas as pd
import matplotlib.pyplot as plt
import glob

# Read the CSV file from Spark output
september_csv = "outputs/ethereum/september_2015_data.csv"
september_data = pd.read_csv(september_csv)

# Plot block_count histogram
plt.figure(figsize=(10, 6))
plt.bar(september_data["formatted_date"], september_data["block_count"], color="blue")
plt.xlabel("Date")
plt.ylabel("Block Count")
plt.title("Daily Block Count for September 2015")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("figures/block_count_histogram.png")
plt.close()
print("Saved: block_count_histogram.png")

# Plot unique_senders_count_number histogram
plt.figure(figsize=(10, 6))
plt.bar(september_data["formatted_date"], september_data["unique_senders_count_number"], color="green")
plt.xlabel("Date")
plt.ylabel("Unique Senders Count")
plt.title("Daily Unique Senders Count for September 2015")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("figures/unique_senders_histogram.png")
plt.close()
print("Saved: unique_senders_histogram.png")


# Plots for Q6:
# October 2015 transaction fees
october_csv = "outputs/ethereum/october_2015_fees.csv"
october_data = pd.read_csv(october_csv)

# string to float 
october_data['total_transaction_fee'] = october_data['total_transaction_fee'].astype(float)


# Wei to ETH
october_data['total_transaction_fee_eth'] = october_data['total_transaction_fee'] / 1e18

plt.figure(figsize=(12, 6))
bars = plt.bar(range(len(october_data)), october_data["total_transaction_fee_eth"], color="orange")
plt.xlabel("Date")
plt.ylabel("Total Transaction Fee (ETH)")
plt.title("Daily Transaction Fees for October 2015 (transaction_index = 0)")
plt.xticks(range(len(october_data)), october_data["formatted_date"], rotation=45, ha='right')

# value labels on top of bars
for i, v in enumerate(october_data["total_transaction_fee_eth"]):
    plt.text(i, v + 1, f'{v:.1f}', ha='center', va='bottom', fontsize=7)

plt.tight_layout()
plt.savefig("figures/october_transaction_fees_histogram.png")
plt.close()
print("Saved: october_transaction_fees_histogram.png")
# save a summary
print(f"\nOctober 2015 Transaction Fees Summary:")
print(f"Total fees for the month: {october_data['total_transaction_fee_eth'].sum():.2f} ETH")
print(f"Average daily fees: {october_data['total_transaction_fee_eth'].mean():.2f} ETH")
print(f"Peak day: {october_data.loc[october_data['total_transaction_fee_eth'].idxmax(), 'formatted_date']} with {october_data['total_transaction_fee_eth'].max():.2f} ETH")