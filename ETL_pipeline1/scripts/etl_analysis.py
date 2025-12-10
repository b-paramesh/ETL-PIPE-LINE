# ==============================
# etl_analysis.py
# ==============================
# Purpose: Analyze Supabase telco churn data & generate summary metrics
# ==============================

import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
import matplotlib.pyplot as plt

# ----------------------------------------
# Supabase Connection
# ----------------------------------------
def get_supabase_client():
    load_dotenv()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")  # Prefer service_role

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    print(f"🔗 Connecting to Supabase → {url}")
    return create_client(url, key)

# ----------------------------------------
# Load data from Supabase
# ----------------------------------------
def load_supabase_table(table="telco_churn"):
    supabase = get_supabase_client()

    print("📥 Downloading data from Supabase...")

    response = supabase.table(table).select("*").execute()

    data = response.data

    if not data:
        raise ValueError("❌ Supabase returned no data!")

    df = pd.DataFrame(data)
    print(f"📊 Loaded {len(df)} rows from Supabase")

    return df

# ----------------------------------------
# Compute Analysis Metrics
# ----------------------------------------
def analyze_data(df: pd.DataFrame):

    summary = {}

    # 1️⃣ Churn Percentage
    churn_rate = df["churn"].str.lower().value_counts(normalize=True).get("yes", 0) * 100
    summary["churn_percentage"] = churn_rate

    # 2️⃣ Average monthly charges per contract type
    contract_avg = df.groupby("contract")["monthlycharges"].mean()
    summary["avg_monthly_charge_per_contract"] = contract_avg.to_dict()

    # 3️⃣ Count of customer loyalty segments
    loyalty_counts = df["tenure_group"].str.lower().value_counts()
    summary["customer_loyalty_counts"] = loyalty_counts.to_dict()

    # 4️⃣ Internet service distribution
    internet_dist = df["internetservice"].value_counts()
    summary["internet_service_distribution"] = internet_dist.to_dict()

    # 5️⃣ Pivot table → Churn vs Tenure Group
    churn_pivot = pd.pivot_table(
        df,
        values="monthlycharges",
        index="tenure_group",
        columns="churn",
        aggfunc="count",
        fill_value=0
    )
    summary["pivot_churn_by_tenure"] = churn_pivot.to_dict()

    return summary, churn_pivot

# ----------------------------------------
# Visualizations
# ----------------------------------------
def generate_visuals(df):

    plt.style.use("ggplot")
    output_dir = os.path.join("..", "data", "processed")
    os.makedirs(output_dir, exist_ok=True)

    # Churn rate by Monthly Charge Segment
    df.groupby("monthly_charge_segment")["churn"].apply(lambda x: (x=="Yes").mean()).plot(kind="bar")
    plt.title("Churn Rate by Monthly Charge Segment")
    plt.ylabel("Churn Rate")
    plt.xticks(rotation=0)
    plt.savefig(os.path.join(output_dir, "churn_rate_by_charge_segment.png"))
    plt.clf()

    # Histogram of Total Charges
    df["totalcharges"].astype(float).plot(kind="hist", bins=30)
    plt.title("Histogram of Total Charges")
    plt.xlabel("Total Charges")
    plt.savefig(os.path.join(output_dir, "hist_total_charges.png"))
    plt.clf()

    # Bar plot of Contract Types
    df["contract"].value_counts().plot(kind="bar")
    plt.title("Contract Type Distribution")
    plt.xticks(rotation=0)
    plt.savefig(os.path.join(output_dir, "contract_distribution.png"))
    plt.clf()

    print("📊 Visualizations saved.")

# ----------------------------------------
# Save summary output CSV
# ----------------------------------------
def save_summary(summary_dict):
    processed_dir = os.path.join("..", "data", "processed")
    os.makedirs(processed_dir, exist_ok=True)

    summary_df = pd.DataFrame.from_dict(summary_dict, orient="index")
    output_path = os.path.join(processed_dir, "analysis_summary.csv")

    summary_df.to_csv(output_path)

    print(f"📁 Summary saved to → {output_path}")


# ----------------------------------------
# MAIN WORKFLOW
# ----------------------------------------
if __name__ == "__main__":
    df = load_supabase_table()

    summary, pivot_table = analyze_data(df)
    
    print("\n📌 ANALYSIS SUMMARY:")
    for key, value in summary.items():
        print(f"\n🔸 {key}:")
        print(value)

    # Save summary file
    save_summary(summary)

    # Generate optional plots
    generate_visuals(df)

    print("\n🎯 ETL ANALYSIS COMPLETE!")
