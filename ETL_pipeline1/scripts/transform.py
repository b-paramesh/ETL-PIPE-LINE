# transform.py
import os
import pandas as pd

def transform_data(raw_path):

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    df = pd.read_csv(raw_path)

    # -----------------------------
    # CLEANING
    # -----------------------------
    df["TotalCharges"] = df["TotalCharges"].replace(" ", None)
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

    numeric_cols = ["tenure", "MonthlyCharges", "TotalCharges"]
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())

    cat_cols = df.select_dtypes(include="object").columns
    df[cat_cols] = df[cat_cols].fillna("unknown")

    # -----------------------------
    # FEATURE ENGINEERING
    # -----------------------------
    def tenure_group(t):
        if t <= 12: return "new"
        elif t <= 36: return "regular"
        elif t <= 60: return "loyal"
        else: return "champion"

    df["tenure_group"] = df["tenure"].apply(tenure_group)

    def charge_segment(m):
        if m < 30: return "low"
        elif m <= 70: return "medium"
        else: return "high"

    df["monthly_charge_segment"] = df["MonthlyCharges"].apply(charge_segment)

    df["is_multi_line_user"] = df["MultipleLines"].apply(lambda x: 1 if x == "Yes" else 0)

    df["contract_type_code"] = df["Contract"].map({
        "Month-to-month": 0,
        "One year": 1,
        "Two year": 2
    })

    # Remove unnecessary ID fields
    df = df.drop(columns=["customerID", "gender"], errors="ignore")

    # -----------------------------
    # RENAME COLUMNS → LOWERCASE
    # -----------------------------
    df.columns = df.columns.str.lower()

    # -----------------------------
    # SAVE TRANSFORMED DATA
    # -----------------------------
    staged_path = os.path.join(staged_dir, "telco_customer_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f"✅ Transformed → {staged_path}")
    return staged_path


if __name__ == "__main__":
    from extract import extract_data
    raw = extract_data()
    transform_data(raw)
