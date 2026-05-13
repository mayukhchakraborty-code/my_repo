import os
from typing import Optional
import pandas as pd

INPUT_FILE = os.path.join(os.path.dirname(__file__), "output_raw_sheet.xlsx")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "mt_dpp_ingestion_may.xlsx")

# Source: mt_vesselname_imo.gsheet (Google Sheet ID: 15paRs1P86T-veEquxMC_EmqFyOIwjmDhMEqD8RDcK38)
IMO_LOOKUP = {
    "FAIRWAY": 9590319,
    "AIGEORGIS": 9891660,
    "CAPE BENAT": 9406013,
    "CORAL PEARL": 9397793,
    "AFRA MARE": 9404936,
    "DUBAI BEAUTY": 9422548,
    "PRIMEWAY": 9817626,
    "DUBAI ANGEL": 9422524,
    "BLUEFIN PEARL": 9397808,
    "YUAN BEI HAI": 9843352,
    "DOLPHIN PEARL": 9402237,
    "CAPE TAFT": 9401221,
    "CAPE TAURA": 9407251,
    "CAPE TEMPEST": 9407263,
    "CHEMTRANS NOVA": 9316232,
    "CHEMTRANS TAURUS": 9295036,
    "CONSTELLATION": 9308091,
    "DUBAI DIAMOND": 9314181,
    "DUBAI GOLD": 9314193,
    "DUBAI GREEN": 9298674,
    "EVRIDIKI": 9318137,
    "HELLESPONT PROTECTOR": 9351452,
    "LADY CHRISTINA": 9372858,
    "DUBAI ATTRACTION": 9422536,
    "DUBAI BRILLIANCE": 9422550,
    "DUBAI CHARM": 9402495,
    "DUBAI GLAMOUR": 9402483,
    "JAG LEELA": 9568184,
    "NEW ABILITY": 9361512,
    "NEW ADVANCE": 9337212,
    "ORCA PEARL": 9402249,
    "PEGASUS STAR": 9891672,
    "PERSEUS STAR": 9891684,
    "SEAGALAXY": 9847231,
    "SEASENATOR": 9304368,
    "SEAVOYAGER": 9408762,
    "SIGMA TRIUMPH": 9410650,
}


def find_vessel(sheet_name: str) -> Optional[str]:
    """Match a sheet name to a vessel in IMO_LOOKUP.

    Handles:
    - Trailing underscores (e.g. 'DUBAI GOLD_' → 'DUBAI GOLD')
    - Vessel name embedded in a multi-name sheet (e.g. '...LADY CHRISTIN' → 'LADY CHRISTINA')
    - Excel 31-char truncation of long sheet names
    """
    clean = sheet_name.rstrip("_ ").upper()

    # 1. Exact match after cleaning
    if clean in IMO_LOOKUP:
        return clean

    # 2. Full vessel name is a substring of the sheet name
    for vessel in IMO_LOOKUP:
        if vessel in clean:
            return vessel

    # 3. Sheet name is truncated — end of sheet name is a prefix of a vessel name
    #    (handles Excel 31-char limit cutting off the last few chars)
    for vessel in IMO_LOOKUP:
        for k in range(len(vessel), max(7, len(vessel) - 3), -1):
            if clean.endswith(vessel[:k]):
                return vessel

    return None


def main():
    xl = pd.ExcelFile(INPUT_FILE)
    matched, skipped = 0, []

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        for sheet_name in xl.sheet_names:
            vessel = find_vessel(sheet_name)
            if vessel is None:
                skipped.append(sheet_name)
                continue

            df = xl.parse(sheet_name)
            df = df.drop(index=[0, 1, 2]).reset_index(drop=True)  # drop Excel rows 2, 3, 4
            imo = IMO_LOOKUP[vessel]

            # Insert Vessel Name (col A) and IMO (col B), shifting existing data right
            df.insert(0, "IMO", imo)
            df.insert(0, "Vessel Name", vessel)

            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"  Written: '{sheet_name}'  →  {vessel}  (IMO {imo})")
            matched += 1

    print(f"\nDone. {matched} sheet(s) written to: {OUTPUT_FILE}")
    if skipped:
        print(f"Skipped (no match in GSheet): {', '.join(skipped)}")


if __name__ == "__main__":
    main()
