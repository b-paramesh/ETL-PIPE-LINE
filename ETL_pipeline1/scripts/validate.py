# ===========================
# validate.py
# ===========================
# Validates Supabase-loaded Telco dataset after ETL

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv


# ----------------------------------------
# Supabase Client
# ----------------------------------------
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


# ----------------------------------------
# Fetch data from Supabase table
# ----------------------------------------
def fetch_supabase_data(table_name="telco_churn"):
    supabase = get_supabase_client()
    response = supabase.table(table_name).select("*").execute()
    df = pd.DataFrame(response.data)
    return df


# ----------------------------------------
# Validation Script
# ----------------------------------------
def validate_data(local_csv_path, table_name="telco_churn"):
    print("\n🔍 Running dataset validation...\n")

    # Load local transformed CSV
    local_df = pd.read_csv(local_csv_path)
    print(f"📄 Local CSV rows: {len(local_df)}")

    # Fetch Supabase data
    supabase_df = fetch_supabase_data(table_name)
    print(f"🗄 Supabase table rows: {len(supabase_df)}")

    print("\n--- VALIDATION CHECKS ---\n")

    # -----------------------------
    # 1. Check missing values
    # -----------------------------
    required_cols = ["tenure", "monthlycharges", "totalcharges"]
    print("🧪 Checking for missing values...")

    missing_report = {
        col: supabase_df[col].isnull().sum() if col in supabase_df.columns else "MISSING COLUMN"
        for col in required_cols
    }

    for col, missing in missing_report.items():
        if missing == 0:
            print(f"✔ {col}: No missing values")
        else:
            print(f"❌ {col}: {missing} missing values")

    # -----------------------------
    # 2. Row count match
    # -----------------------------
    print("\n🧪 Verifying row count consistency...")
    if len(local_df) == len(supabase_df):
        print("✔ Row count matches local CSV")
    else:
        print(f"❌ Row count mismatch! CSV={len(local_df)}, Supabase={len(supabase_df)}")

    # -----------------------------
    # 3. Tenure group segments
    # -----------------------------
    print("\n🧪 Checking tenure_group values...")
    expected_tenure_groups = {"New", "Regular", "Loyal", "Champion"}

    if "tenure_group" in supabase_df.columns:
        actual_groups = set(supabase_df["tenure_group"].unique())
        print("Found:", actual_groups)

        if actual_groups == expected_tenure_groups:
            print("✔ All tenure groups present")
        else:
            print("❌ Missing or extra tenure groups")

    # -----------------------------
    # 4. Monthly Charge Segment
    # -----------------------------
    print("\n🧪 Checking monthly_charge_segment values...")
    expected_charge_segments = {"Low", "Medium", "High"}

    if "monthly_charge_segment" in supabase_df.columns:
        actual_segments = set(supabase_df["monthly_charge_segment"].unique())
        print("Found:", actual_segments)

        if actual_segments == expected_charge_segments:
            print("✔ All charge segments present")
        else:
            print("❌ Missing or extra charge segments")

    # -----------------------------
    # 5. Contract code validation
    # -----------------------------
    print("\n🧪 Checking contract_type_code values...")

    if "contract_type_code" in supabase_df.columns:
        invalid_codes = supabase_df[
            ~supabase_df["contract_type_code"].isin([0, 1, 2])
        ]["contract_type_code"].unique()

        if len(invalid_codes) == 0:
            print("✔ contract_type_code contains only {0,1,2}")
        else:
            print(f"❌ Invalid contract codes found: {invalid_codes}")

    print("\n🎯 VALIDATION COMPLETED\n")


# ----------------------------------------
# Run as script
# ----------------------------------------
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    local_csv = os.path.join(base_dir, "data", "staged", "telco_Customer_transformed.csv")

    validate_data(local_csv, "telco_churn")
