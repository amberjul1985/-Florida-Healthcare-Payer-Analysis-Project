"""
PURPOSE: 
    Process raw hospital pricing data through a complete pipeline:
    1. Load raw radiology charges data
    2. Extract and consolidate CPT codes
    3. Filter for applicable radiation oncology CPT codes
    4. Extract Florida city information
    5. Classify healthcare payers into groups

PLATFORM: Windows PC
INPUT FILE: 0_charges_radiology.csv
OUTPUT FILE: 3_filtered_cpt_codes_with_payer_group.csv

"""

import pandas as pd
import numpy as np
import warnings
import re
import os
from pathlib import Path

warnings.filterwarnings('ignore')

# ================================================================================
# CONFIGURATION SECTION - UPDATE THESE PATHS FOR YOUR SYSTEM
# ================================================================================

DATA_FOLDER = r'C:\Robby\BANA 650_Healthcare\DATA'

INPUT_FILE = r'0_charges_radiology.csv'
CPT_MAPPING_FILE = r'radiation_treatment_cpt_codes.csv'

# Full paths
INPUT_PATH = os.path.join(DATA_FOLDER, INPUT_FILE)
CPT_MAPPING_PATH = os.path.join(DATA_FOLDER, CPT_MAPPING_FILE)

# Intermediate files (will be created)
NEWID_FILE = os.path.join(DATA_FOLDER, '1_NEWID_charges_radiology.csv')
CPT_ONLY_FILE = os.path.join(DATA_FOLDER, '2_cpt_only_by_id.csv')
MERGED_FILE = os.path.join(DATA_FOLDER, '3_NEWID_radiology.csv')
FILTERED_CPT_FILE = os.path.join(DATA_FOLDER, '4_filtered_for_cpt_codes.csv')
CITY_EXTRACTED_FILE = os.path.join(DATA_FOLDER, '5_with_city.csv')
COLUMNS_FILTERED_FILE = os.path.join(DATA_FOLDER, '6_filtered_columns.csv')

# FINAL OUTPUT
OUTPUT_FILE = os.path.join(DATA_FOLDER, '3_filtered_cpt_codes_with_payer_group.csv')

print("\n" + "=" * 80)
print("CONFIGURATION CHECK")
print("=" * 80)
print(f"Data Folder: {DATA_FOLDER}")
print(f"Input File: {INPUT_PATH}")
print(f"CPT Mapping: {CPT_MAPPING_PATH}")
print(f"Output File: {OUTPUT_FILE}")
print()

# ================================================================================
# STEP 1: GENERATE NEW UNIQUE IDs
# ================================================================================

def step_1_generate_unique_ids():
    """Generate sequential unique IDs for all rows in raw data"""
    print("\n" + "=" * 80)
    print("STEP 1: GENERATING NEW UNIQUE IDs")
    print("=" * 80)
    
    try:
        df = pd.read_csv(INPUT_PATH, low_memory=False)
        n = len(df)
        print(f"✓ Loaded {n:,} rows from {INPUT_FILE}")
        
        # Create sequential IDs
        width = max(6, len(str(n)))
        df['unique_id'] = [f"A{str(i).zfill(width)}" for i in range(1, n + 1)]
        
        df.to_csv(NEWID_FILE, index=False)
        print(f"✓ Generated unique IDs: A000001 ... A{str(n).zfill(width)}")
        print(f"✓ Saved to: {NEWID_FILE}\n")
        
        return df
    except Exception as e:
        print(f"✗ ERROR in Step 1: {e}")
        return None


# ================================================================================
# STEP 2: CONSOLIDATE CPT CODES
# ================================================================================

def step_2_consolidate_cpt_codes(df):
    """Extract and consolidate all CPT codes into single column"""
    print("\n" + "=" * 80)
    print("STEP 2: CONSOLIDATING CPT CODES")
    print("=" * 80)
    
    try:
        ID_COL = "unique_id"
        
        # Find all code/type column pairs
        code_cols = [col for col in df.columns if "code" in col.lower()
                     and "type" not in col.lower()]
        pairs = [(col, f"{col} | type") for col in code_cols 
                 if f"{col} | type" in df.columns]
        
        print(f"✓ Found {len(pairs)} code/type column pairs")
        
        # Reshape all code pairs
        dfs = []
        for code_col, type_col in pairs:
            temp = df[[ID_COL, code_col, type_col]].rename(
                columns={code_col: "code", type_col: "type"})
            dfs.append(temp)
        
        long_df = pd.concat(dfs, ignore_index=True)
        
        # Filter for CPT only
        cpt_df = (
            long_df[long_df["type"].str.strip().str.upper() == "CPT"]
            .dropna(subset=["code"])
            .reset_index(drop=True)
        )
        
        # Reorder by unique_id
        cpt_df = cpt_df.sort_values(by=ID_COL, kind="stable")
        cpt_df = cpt_df[[ID_COL, "type", "code"]]
        
        cpt_df.to_csv(CPT_ONLY_FILE, index=False)
        print(f"✓ Extracted {len(cpt_df):,} CPT code rows")
        print(f"✓ Saved to: {CPT_ONLY_FILE}\n")
        
        return cpt_df
    except Exception as e:
        print(f"✗ ERROR in Step 2: {e}")
        return None


# ================================================================================
# STEP 3: MERGE CPT CODES WITH ORIGINAL DATA
# ================================================================================

def step_3_merge_cpt_with_data(df_newid, df_cpt):
    """Merge CPT codes back with original data"""
    print("\n" + "=" * 80)
    print("STEP 3: MERGING CPT CODES WITH ORIGINAL DATA")
    print("=" * 80)
    
    try:
        merged_df = pd.merge(df_cpt, df_newid, on='unique_id', how='inner')
        merged_df.to_csv(MERGED_FILE, index=False)
        
        print(f"✓ Merged {len(merged_df):,} rows")
        print(f"✓ Saved to: {MERGED_FILE}\n")
        
        return merged_df
    except Exception as e:
        print(f"✗ ERROR in Step 3: {e}")
        return None


# ================================================================================
# STEP 4: CLEAN CPT CODES & COUNT HOSPITALS
# ================================================================================

def clean_cpt_code(raw):
    """Normalize CPT codes and handle modifiers"""
    if pd.isna(raw):
        return None
    s = str(raw).strip().upper()
    
    # Remove trailing .0
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]
    
    # Normalize modifiers
    m = re.fullmatch(
        r"(\d{5})[\s\.-]*(\d{2}|TC|GC|GN|GO|GP|QK|QX|QY|26|50|59|76|77|78|79|80|81|82|AS|LT|RT)", s)
    if m:
        base, mod = m.groups()
        return f"{base}-{mod}"
    
    # Plain 5-digit CPT
    if re.fullmatch(r"\d{5}", s):
        return s
    
    # Base + modifier smashed
    m2 = re.fullmatch(
        r"(\d{5})(\d{2}|TC|GC|GN|GO|GP|QK|QX|QY|26|50|59|76|77|78|79|80|81|82|AS|LT|RT)", s)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}"
    
    s = re.sub(r"\.$", "", s)
    return s or None


def step_4_clean_and_count_cpt(df):
    """Clean CPT codes and count unique hospitals per CPT"""
    print("\n" + "=" * 80)
    print("STEP 4: CLEANING CPT CODES & COUNTING HOSPITALS")
    print("=" * 80)
    
    try:
        df["code_clean"] = df["code"].map(clean_cpt_code)
        df = df[df["code_clean"].notna() & (df["code_clean"].str.len() > 0)]
        
        # Count unique hospitals per CPT
        hospitals_per_cpt = (
            df.groupby("code_clean", dropna=False)["hospital_name"]
            .nunique()
            .reset_index(name="unique_hospitals")
            .rename(columns={"code_clean": "code"})
            .sort_values(["unique_hospitals", "code"], ascending=[False, True])
        )
        
        print(f"✓ Cleaned CPT codes: {len(df):,} rows")
        print(f"✓ Unique hospitals per CPT calculated")
        print(f"  - Total unique CPT codes: {len(hospitals_per_cpt):,}")
        print(f"  - Average hospitals per CPT: {hospitals_per_cpt['unique_hospitals'].mean():.1f}\n")
        
        return df
    except Exception as e:
        print(f"✗ ERROR in Step 4: {e}")
        return None


# ================================================================================
# STEP 5: FILTER FOR RADIATION ONCOLOGY CPT CODES
# ================================================================================

def step_5_filter_radiation_oncology_cpts(df):
    """Filter for specific radiation oncology CPT codes"""
    print("\n" + "=" * 80)
    print("STEP 5: FILTERING FOR RADIATION ONCOLOGY CPT CODES")
    print("=" * 80)
    
    try:
        # Radiation oncology CPT codes from your dataset
        cpt_codes = [
            77280, 77290, 77295, 77300, 77470, 77306, 77307, 77318, 77331, 77333,
            77334, 77336, 77370, 77373, 77387, 77417, 77790, 77301, 77316, 77321,
            77332, 77338, 77385, 77402, 77407, 77412, 77771, 77772, 77285, 77293,
            77317, 77372, 77386, 77770, 77424, 77778, 77401, 77763, 77767, 77768,
            77371, 77761, 77762, 77789, 77750
        ]
        
        df["code"] = pd.to_numeric(df["code"], errors="coerce")
        filtered_df = df[df["code"].isin(cpt_codes)].copy()
        
        filtered_df.to_csv(FILTERED_CPT_FILE, index=False)
        
        print(f"✓ Filtered for {len(cpt_codes)} radiation oncology CPT codes")
        print(f"✓ Remaining rows: {len(filtered_df):,}")
        print(f"✓ Saved to: {FILTERED_CPT_FILE}\n")
        
        return filtered_df
    except Exception as e:
        print(f"✗ ERROR in Step 5: {e}")
        return None


# ================================================================================
# STEP 6: EXTRACT FLORIDA CITIES
# ================================================================================

def extract_city(raw):
    """Extract city from hospital address"""
    STREET_WORDS = {
        "AVE", "AVENUE", "BLVD", "BOULEVARD", "CIR", "CIRCLE", "CT", "COURT", "DR", "DRIVE",
        "HWY", "HIGHWAY", "LN", "LANE", "PKWY", "PARKWAY", "PL", "PLACE", "PLZ", "PLAZA",
        "RD", "ROAD", "SQ", "SQUARE", "ST", "STREET", "TER", "TERRACE", "TRL", "TRAIL", "WAY"
    }
    
    def norm_token(tok: str) -> str:
        return re.sub(r"[^A-Z]", "", tok.upper())
    
    if pd.isna(raw):
        return None
    
    s = str(raw).strip()
    if not s:
        return None
    
    # Split by first |
    s = s.split("|", 1)[0].strip()
    if not s:
        return None
    
    # Try comma-style first
    m = re.search(
        r""",\s*([^,]+?)\s*,\s*(?:FL(?:\s*\d{5}(?:-\d{4})?)?|Florida(?:\s*\d{5}(?:-\d{4})?)?)\b""",
        s, flags=re.IGNORECASE
    )
    if m:
        return m.group(1).strip()
    
    # If commas exist but simple pattern failed
    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        state_idx = None
        for i, p in enumerate(parts):
            if re.search(r"\b(?:FL|Florida)\b", p, flags=re.IGNORECASE):
                state_idx = i
                break
        if state_idx is not None and state_idx > 0:
            return parts[state_idx - 1].strip()
        if len(parts) >= 2:
            return parts[-2].strip()
        return None
    
    # No-commas style
    tokens = s.split()
    if not tokens:
        return None
    
    state_idx = None
    for i, tok in enumerate(tokens):
        if norm_token(tok) in {"FL", "FLORIDA"}:
            state_idx = i
            break
    if state_idx is None:
        return None
    
    street_idx = None
    for i in range(state_idx - 1, -1, -1):
        if norm_token(tokens[i]) in STREET_WORDS:
            street_idx = i
            break
    
    if street_idx is not None and street_idx + 1 < state_idx:
        city = " ".join(tokens[street_idx + 1:state_idx]).strip(", ")
        return city or None
    
    if state_idx > 0:
        return tokens[state_idx - 1].strip(", ")
    
    return None


def step_6_extract_cities(df):
    """Extract Florida cities from hospital addresses"""
    print("\n" + "=" * 80)
    print("STEP 6: EXTRACTING FLORIDA CITIES")
    print("=" * 80)
    
    try:
        df["City"] = df["hospital_address"].apply(extract_city)
        
        # Clean up city values
        df["City"] = df["City"].astype(object)
        df["City"].replace([None, np.nan], np.nan, inplace=True)
        df["City"] = df["City"].astype(str)
        df["City"].replace(r"^\s*$", np.nan, regex=True, inplace=True)
        df["City"].replace(r"(?i)^(na|n/a|null|none|nan)$", np.nan, regex=True, inplace=True)
        
        # Fill missing with Orlando (default)
        df["City"] = df["City"].fillna("Orlando")
        
        df.to_csv(CITY_EXTRACTED_FILE, index=False)
        
        missing_count = df["City"].isna().sum()
        print(f"✓ Extracted cities from hospital addresses")
        print(f"✓ Unique cities: {df['City'].nunique()}")
        print(f"✓ Missing values filled with 'Orlando': {len(df) - df['City'].notna().sum()}")
        print(f"✓ Saved to: {CITY_EXTRACTED_FILE}\n")
        
        return df
    except Exception as e:
        print(f"✗ ERROR in Step 6: {e}")
        return None


# ================================================================================
# STEP 7: SELECT FOCUS COLUMNS
# ================================================================================

def step_7_select_columns(df):
    """Select and organize focus columns for analysis"""
    print("\n" + "=" * 80)
    print("STEP 7: SELECTING FOCUS COLUMNS")
    print("=" * 80)
    
    try:
        keep_cols = [
            "unique_id", "type", "code", "hospital_name", "hospital_address", 
            "payer_name", "plan_name", "estimated_amount", "negotiated_algorithm",
            "negotiated_percentage", "negotiated_dollar", "setting", "gross",
            "discounted_cash", "min", "max", "City"
        ]
        
        existing_cols = [col for col in keep_cols if col in df.columns]
        missing_cols = [col for col in keep_cols if col not in df.columns]
        
        filtered_df = df[existing_cols].copy()
        filtered_df.to_csv(COLUMNS_FILTERED_FILE, index=False)
        
        print(f"✓ Selected {len(existing_cols)} columns")
        if missing_cols:
            print(f"⚠ Missing columns: {missing_cols}")
        print(f"✓ Saved to: {COLUMNS_FILTERED_FILE}\n")
        
        return filtered_df
    except Exception as e:
        print(f"✗ ERROR in Step 7: {e}")
        return None


# ================================================================================
# STEP 8: CLASSIFY HEALTHCARE PAYERS
# ================================================================================

def classify_payer_row(row) -> str:
    """Classify payer into group (Medicare, Medicaid, Private/Commercial, etc.)"""
    
    MEDICARE_KEYWORDS = {
        "medicare", "railroad medicare", "medicare advantage", "part c",
        "mapd", "dsnp", "dual complete", "healthsun medicare"
    }
    
    MEDICAID_KEYWORDS = {
        "medicaid", "medi-cal", "masshealth", "badgercare", "husky", "peachcare",
        "kancare", "ahcccs", "apple health", "health first colorado", "mainecare",
        "nj familycare", "ohio medicaid", "texas medicaid", "chip",
        "better health", "community plan", "mhs", "molina", "amerihealth", "passport",
        "buckeye", "peach state", "caresource", "care source", "wellcare", "amerigroup",
        "louisiana healthcare connections", "unitedhealthcare community plan",
        "sunshine health", "health plan of nevada"
    }
    
    SELF_PAY_KEYWORDS = {
        "self-pay", "self pay", "self", "cash", "uninsured", "no insurance",
        "charity", "out-of-pocket", "out of pocket"
    }
    
    txt = row.get("payer_text", "")
    
    # Rule 1: Empty payer text with cash/gross price = Self-pay
    if (txt == "" or txt in {"na", "n/a", "none", "null"}) and (
        pd.notna(row.get("discounted_cash")) or pd.notna(row.get("gross"))
    ):
        return "Self-pay / Other"
    
    # Rule 2-4: Check for keywords
    if any(k in txt for k in MEDICARE_KEYWORDS):
        return "Medicare"
    
    if any(k in txt for k in MEDICAID_KEYWORDS):
        return "Medicaid"
    
    if any(k in txt for k in SELF_PAY_KEYWORDS):
        return "Self-pay / Other"
    
    # Rule 5: No payer information = Unknown
    payer_name_val = (row.get("payer_name") or "") if pd.notna(row.get("payer_name")) else ""
    plan_name_val = (row.get("plan_name") or "") if pd.notna(row.get("plan_name")) else ""
    
    if payer_name_val.strip() == "" and plan_name_val.strip() == "":
        return "Unknown"
    
    # Rule 6: Default = Private/Commercial
    return "Private / Commercial"


def step_8_classify_payers(df):
    """Classify healthcare payers into groups"""
    print("\n" + "=" * 80)
    print("STEP 8: CLASSIFYING HEALTHCARE PAYERS")
    print("=" * 80)
    
    try:
        # Prepare required columns
        required_columns = ["payer_name", "plan_name", "additional_payer_notes",
                           "discounted_cash", "gross"]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan
        
        # Convert price columns to numeric
        for col in ["discounted_cash", "gross"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Create combined payer text field
        df["payer_text"] = (
            df["payer_name"].fillna("").astype(str) + " " +
            df["plan_name"].fillna("").astype(str) + " " +
            df.get("additional_payer_notes", pd.Series(np.nan)).fillna("").astype(str)
        ).str.strip().str.lower()
        
        # Apply classification
        df["payer_group"] = df.apply(classify_payer_row, axis=1)
        
        # Reorder columns to put payer_group next to payer_name
        cols = list(df.columns)
        if "payer_name" in cols and "payer_group" in cols:
            cols.remove("payer_group")
            insert_position = cols.index("payer_name") + 1
            cols.insert(insert_position, "payer_group")
            df = df[cols]
        
        df.to_csv(OUTPUT_FILE, index=False)
        
        # Summary statistics
        payer_counts = df["payer_group"].value_counts(dropna=False).sort_index()
        
        print(f"✓ Classified {len(df):,} rows into payer groups\n")
        print("PAYER GROUP DISTRIBUTION:")
        print("-" * 80)
        for group, count in payer_counts.items():
            percentage = (count / len(df)) * 100
            print(f"  {group:.<35} {count:>8,} ({percentage:>5.1f}%)")
        print("-" * 80)
        print(f"  {'TOTAL':.<35} {len(df):>8,} (100.0%)")
        print(f"\n✓ Final output saved to: {OUTPUT_FILE}\n")
        
        return df
    except Exception as e:
        print(f"✗ ERROR in Step 8: {e}")
        return None


# ================================================================================
# MAIN EXECUTION
# ================================================================================

def main():
    """Execute the complete pipeline"""
    print("\n" + "=" * 80)
    print("BANA 650 HEALTHCARE ANALYTICS - DATA PREPARATION PIPELINE")
    print("=" * 80)
    print("\nStarting data preparation pipeline...\n")
    
    # Step 1: Generate unique IDs
    df = step_1_generate_unique_ids()
    if df is None:
        print("✗ Pipeline halted at Step 1")
        return
    
    # Step 2: Consolidate CPT codes
    df_cpt = step_2_consolidate_cpt_codes(df)
    if df_cpt is None:
        print("✗ Pipeline halted at Step 2")
        return
    
    # Step 3: Merge
    df_merged = step_3_merge_cpt_with_data(df, df_cpt)
    if df_merged is None:
        print("✗ Pipeline halted at Step 3")
        return
    
    # Step 4: Clean and count
    df_clean = step_4_clean_and_count_cpt(df_merged)
    if df_clean is None:
        print("✗ Pipeline halted at Step 4")
        return
    
    # Step 5: Filter for radiation oncology
    df_filtered = step_5_filter_radiation_oncology_cpts(df_clean)
    if df_filtered is None:
        print("✗ Pipeline halted at Step 5")
        return
    
    # Step 6: Extract cities
    df_cities = step_6_extract_cities(df_filtered)
    if df_cities is None:
        print("✗ Pipeline halted at Step 6")
        return
    
    # Step 7: Select columns
    df_selected = step_7_select_columns(df_cities)
    if df_selected is None:
        print("✗ Pipeline halted at Step 7")
        return
    
    # Step 8: Classify payers
    df_final = step_8_classify_payers(df_selected)
    if df_final is None:
        print("✗ Pipeline halted at Step 8")
        return
    
    # Success!
    print("=" * 80)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    print(f"\nFinal output: {OUTPUT_FILE}")
    print(f"Total rows: {len(df_final):,}")
    print(f"Total columns: {len(df_final.columns)}")
    print("\nYour data is now ready for the next phase:")
    print("  → Advanced imputation based on negotiated_percentage and gross")
    print("  → Treatment category mapping")
    print("  → Statistical analysis and visualization")
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    import os
    main()