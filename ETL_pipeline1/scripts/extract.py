
## extract.py
import os
import pandas as pd

def extract_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Create raw folder
    raw_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    # Source dataset (placed in project root)
    source_file = os.path.join(base_dir, "WA_Fn-UseC_-Telco-Customer-Churn.csv")

    # Validate file
    if not os.path.exists(source_file):
        raise FileNotFoundError(
            f"❌ Dataset not found at: {source_file}\n"
            "➡ Place the file inside the project root."
        )

    # Load dataset
    df = pd.read_csv(source_file)

    # Save into raw directory
    raw_output = os.path.join(raw_dir, "telco_raw.csv")
    df.to_csv(raw_output, index=False)

    print(f"✅ Extracted → {raw_output}")
    return raw_output


if __name__ == "__main__":
    extract_data()
