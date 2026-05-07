import os
import pandas as pd

INPUT_FOLDER = os.path.join(os.path.dirname(__file__), "Panamax Vessels")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "output_raw_sheet.xlsx")

MAX_SHEET_NAME_LEN = 31  # Excel sheet name limit


def get_sheet_name(filename: str) -> str:
    stem = os.path.splitext(filename)[0]  # strip file extension
    # Strip leading "DPN_" by keeping everything after the first "_"
    if "_" in stem:
        stem = stem.split("_", 1)[1]
    return stem[:MAX_SHEET_NAME_LEN]


def main():
    files = sorted(
        f for f in os.listdir(INPUT_FOLDER)
        if f.lower().endswith((".csv", ".xlsx", ".xls"))
    )

    if not files:
        print("No CSV/Excel files found in the folder.")
        return

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        for filename in files:
            filepath = os.path.join(INPUT_FOLDER, filename)
            sheet_name = get_sheet_name(filename)

            if filename.lower().endswith(".csv"):
                df = pd.read_csv(filepath)
            else:
                df = pd.read_excel(filepath)

            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"  Added sheet: '{sheet_name}'  ←  {filename}")

    print(f"\nDone. Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
