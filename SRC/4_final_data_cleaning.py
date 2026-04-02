"""
PURPOSE:
    Clean hospital pricing data using professor-approved methodology:
    
    METHOD 1 (Priority): Calculate negotiated_dollar from gross × negotiated_percentage
                        (Only if BOTH gross AND negotiated_percentage are available)
    
    HANDLING OF REMAINING MISSING: Drop rows where negotiated_dollar is still missing
                                   after Method 1 (do NOT impute with median)

"""

import pandas as pd
import numpy as np
import warnings
import os
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

warnings.filterwarnings('ignore')

print("\n" + "=" * 80)
print("SCRIPT: FINAL DATA CLEANING WITH METHOD 1 IMPUTATION + DROPPING MISSING")
print("=" * 80 + "\n")

# ================================================================================
# CONFIGURATION SECTION - WINDOWS PATHS (UPDATE THESE)
# ================================================================================

# YOUR DATA FOLDER - UPDATE THIS PATH FOR YOUR SYSTEM
DATA_FOLDER = r'C:\Robby\BANA 650_Healthcare\DATA'

# OUTPUT FOLDER - WHERE VISUALIZATIONS AND CLEANED DATA WILL BE SAVED
RESULTS_FOLDER = r'C:\Robby\BANA 650_Healthcare\Results'
VISUALISATION_FOLDER = os.path.join(RESULTS_FOLDER, 'visualisation')  # NEW: Subfolder for all visualizations

# INPUT AND OUTPUT FILES
INPUT_FILE = '3_filtered_cpt_codes_with_payer_group.csv'
CPT_MAPPING_FILE = 'radiation_treatment_cpt_codes.csv'
OUTPUT_FILE = '4_cleaned_prices_with_imputation_tracking.csv'

# CONSTRUCT FULL PATHS
INPUT_PATH = os.path.join(DATA_FOLDER, INPUT_FILE)
CPT_MAPPING_PATH = os.path.join(DATA_FOLDER, CPT_MAPPING_FILE)
OUTPUT_PATH = os.path.join(DATA_FOLDER, OUTPUT_FILE)  # CSV goes to DATA folder

# Create results folder if it doesn't exist
if not os.path.exists(RESULTS_FOLDER):
    os.makedirs(RESULTS_FOLDER, exist_ok=True)
    print(f"✓ Created results folder: {RESULTS_FOLDER}")

# Create visualisation subfolder if it doesn't exist
if not os.path.exists(VISUALISATION_FOLDER):
    os.makedirs(VISUALISATION_FOLDER, exist_ok=True)
    print(f"✓ Created visualisation subfolder: {VISUALISATION_FOLDER}\n")
else:
    print(f"✓ Visualisation folder exists: {VISUALISATION_FOLDER}\n")

# Verify input folder exists
if not os.path.exists(DATA_FOLDER):
    print(f"\n❌ ERROR: Data folder not found at: {DATA_FOLDER}")
    print("Please verify the path and try again.")
    exit()

print(f"✓ Data Folder:           {DATA_FOLDER}")
print(f"✓ Results Folder:        {RESULTS_FOLDER}")
print(f"✓ Visualisation Folder:  {VISUALISATION_FOLDER}")
print(f"✓ Input file:            {INPUT_FILE}")
print(f"✓ CPT mapping:           {CPT_MAPPING_FILE}")
print(f"✓ Output file:           {OUTPUT_FILE}\n")

# ================================================================================
# SECTION 1: LOAD DATA
# ================================================================================

print("Step 1: Loading data...")
print("-" * 80)

try:
    df = pd.read_csv(INPUT_PATH, low_memory=False)
    print(f"✓ Loaded main dataset: {df.shape[0]:,} rows × {df.shape[1]} columns")
except Exception as e:
    print(f"✗ ERROR loading main data: {e}")
    exit()

try:
    cpt_mapping = pd.read_csv(CPT_MAPPING_PATH)
    print(f"✓ Loaded CPT mapping: {cpt_mapping.shape[0]} treatment categories")
except Exception as e:
    print(f"⚠ WARNING: Could not load CPT mapping: {e}")
    print("  Will skip treatment category assignment")
    cpt_mapping = None

print()

# ================================================================================
# SECTION 2: MAP TREATMENT CATEGORIES
# ================================================================================

if cpt_mapping is not None:
    print("Step 2: Mapping CPT codes to treatment categories...")
    print("-" * 80)
    
    def assign_treatment_category(cpt_code, mapping_df):
        """Map CPT code to treatment category based on ranges"""
        try:
            code_numeric = float(cpt_code)
        except (ValueError, TypeError):
            return 'Unknown'
        
        for _, row in mapping_df.iterrows():
            range_str = str(row['CPT_Code_Range']).strip()
            parts = range_str.split('-')
            
            if len(parts) == 2:
                try:
                    start = float(parts[0].strip())
                    end = float(parts[1].strip())
                    if start <= code_numeric <= end:
                        return row['Treatment_Category']
                except ValueError:
                    continue
        
        return 'Unknown'
    
    df['Treatment_Category'] = df['code'].apply(
        lambda x: assign_treatment_category(x, cpt_mapping)
    )
    
    print("✓ Treatment categories assigned")
    print("\nTreatment Category Distribution:")
    for category, count in df['Treatment_Category'].value_counts().items():
        pct = (count / len(df)) * 100
        print(f"  {category:50s}: {count:7,} ({pct:5.1f}%)")
    print()
else:
    print("Step 2: SKIPPING treatment category mapping (CPT file not available)")
    df['Treatment_Category'] = 'Unknown'
    print()

# ================================================================================
# SECTION 3: SELECT FOCUS COLUMNS
# ================================================================================

print("Step 3: Selecting focus columns...")
print("-" * 80)

focus_cols = [
    'code', 'unique_id', 'hospital_name', 'hospital_address', 'City',
    'Treatment_Category',
    'payer_name', 'payer_group', 'plan_name',
    'estimated_amount', 'negotiated_percentage', 'negotiated_dollar',
    'gross'
]

focus_cols = [c for c in focus_cols if c in df.columns]
df = df[focus_cols].copy()

print(f"✓ Kept {len(focus_cols)} columns\n")

# ================================================================================
# SECTION 4: BASELINE MISSINGNESS REPORT
# ================================================================================

print("Step 4: Baseline missingness assessment...")
print("-" * 80)

key_pricing_cols = ['negotiated_percentage', 'negotiated_dollar', 'estimated_amount', 'gross']

baseline_missing = {}
for col in key_pricing_cols:
    if col in df.columns:
        missing_count = df[col].isnull().sum()
        missing_pct = (missing_count / len(df)) * 100
        baseline_missing[col] = missing_count
        print(f"{col:30s}: {missing_count:7,} missing ({missing_pct:6.2f}%)")

print(f"\nTarget column baseline: negotiated_dollar has {baseline_missing['negotiated_dollar']:,} missing values")
print()

# ================================================================================
# SECTION 5: IMPUTATION FLAGS (SET BEFORE ANY WORK)
# ================================================================================

print("Step 5: Creating imputation tracking flags...")
print("-" * 80)

# Flag what was originally missing BEFORE imputation
df['was_negotiated_dollar_missing'] = df['negotiated_dollar'].isna().astype(int)
df['imputation_method'] = 'Original'  # Will track which method was used

original_missing_negotiated_dollar = int(df['was_negotiated_dollar_missing'].sum())
rows_before = len(df)

print(f"✓ Identified {original_missing_negotiated_dollar:,} rows with missing negotiated_dollar")
print()

# ================================================================================
# SECTION 6: ANALYZE FEASIBILITY FOR METHOD 1
# ================================================================================

print("Step 6: Analyzing imputation feasibility (Method 1)...")
print("-" * 80)

# Identify rows that are candidates for Method 1 imputation
method_1_candidates = df[
    (df['negotiated_dollar'].isna()) &
    (df['negotiated_percentage'].notna()) &
    (df['gross'].notna())
].copy()

print(f"Rows eligible for Method 1 (missing negotiated_dollar, have % & gross): {len(method_1_candidates):,}")

# Check percentage scale
pct_vals = method_1_candidates['negotiated_percentage'].dropna()
if len(pct_vals) > 0:
    pct_over_1 = (pct_vals > 1).sum()
    pct_le_1 = (pct_vals <= 1).sum()

    print(f"\nPercentage scale analysis for Method 1 candidates:")
    print(f"  Values > 1.0:  {pct_over_1:,} ({pct_over_1/len(pct_vals)*100:.1f}%)")
    print(f"  Values ≤ 1.0:  {pct_le_1:,} ({pct_le_1/len(pct_vals)*100:.1f}%)")
    print(f"  Min value: {pct_vals.min():.4f}")
    print(f"  Max value: {pct_vals.max():.4f}")
    print(f"  Mean value: {pct_vals.mean():.4f}")

    # Determine scale automatically
    decimal_scale = (pct_le_1 / len(pct_vals)) > 0.70

    if decimal_scale:
        print(f"\n⚠ Detected: DECIMAL SCALE (0.0 to 1.0)")
        print("  Percentages will be used as-is (e.g., 0.281 = 28.1%)")
    else:
        print(f"\n⚠ Detected: WHOLE NUMBER SCALE (0 to 100)")
        print("  Percentages will be divided by 100 first (e.g., 39.5 → 0.395)")
else:
    decimal_scale = False
    print("\n⚠ No valid negotiated_percentage values found for Method 1 scale detection")

print()

# ================================================================================
# SECTION 7: METHOD 1 IMPUTATION
# ================================================================================

print("Step 7: Implementing METHOD 1 imputation...")
print("-" * 80)
print("Logic: negotiated_dollar = gross × (negotiated_percentage / 100 or as-is)\n")

# Identify rows for Method 1
method_1_mask = (
    (df['negotiated_dollar'].isna()) &
    (df['negotiated_percentage'].notna()) &
    (df['gross'].notna())
)

method_1_count = int(method_1_mask.sum())

if method_1_count > 0:
    # Get the values to compute
    pct_raw = df.loc[method_1_mask, 'negotiated_percentage'].copy()
    gross_raw = df.loc[method_1_mask, 'gross'].copy()
    
    # Convert to numeric
    pct_numeric = pd.to_numeric(pct_raw, errors='coerce')
    gross_numeric = pd.to_numeric(gross_raw, errors='coerce')
    
    # Apply scale correction
    if decimal_scale:
        pct_factor = pct_numeric
    else:
        pct_factor = pct_numeric / 100.0
    
    # Calculate negotiated_dollar
    calculated_values = gross_numeric * pct_factor
    
    # Impute
    df.loc[method_1_mask, 'negotiated_dollar'] = calculated_values
    
    # Track which rows used Method 1
    df.loc[method_1_mask, 'imputation_method'] = 'Method 1: gross × negotiated_percentage'
    
    print(f"✓ Imputed {method_1_count:,} rows using Method 1")
    if len(method_1_candidates) > 0:
        sample_idx = method_1_candidates.index[0]
        sample_pct = df.loc[sample_idx, 'negotiated_percentage']
        sample_gross = df.loc[sample_idx, 'gross']
        sample_result = df.loc[sample_idx, 'negotiated_dollar']
        print(f"  Sample: gross ({sample_gross}) × pct ({sample_pct}) = ${sample_result:.2f}")
else:
    print(f"⚠ No rows eligible for Method 1 imputation")

# Report remaining missing
remaining_missing = int(df['negotiated_dollar'].isna().sum())
print(f"\nRemaining missing negotiated_dollar after Method 1: {remaining_missing:,}\n")

# ================================================================================
# SECTION 8: CLEAN NUMERIC COLUMNS
# ================================================================================

print("Step 8: Cleaning numeric columns...")
print("-" * 80)

num_cols = [c for c in ['estimated_amount', 'negotiated_percentage', 'negotiated_dollar']
            if c in df.columns]

for col in num_cols:
    df[col] = (
        df[col].astype(str)
               .str.replace(r'[\$,%]', '', regex=True)
               .str.strip()
               .replace({'': np.nan, 'nan': np.nan, 'NaN': np.nan, 'None': np.nan})
    )
    df[col] = pd.to_numeric(df[col], errors='coerce')
    
    valid = df[col].notna().sum()
    print(f"✓ {col}: {valid:,} valid values")

print()

# ================================================================================
# SECTION 9: CLEAN CATEGORICAL COLUMNS
# ================================================================================

print("Step 9: Cleaning categorical columns...")
print("-" * 80)

cat_cols = [c for c in [
    'code', 'unique_id', 'hospital_name', 'hospital_address',
    'City', 'payer_name', 'payer_group', 'plan_name', 'Treatment_Category'
] if c in df.columns]

for col in cat_cols:
    df[col] = df[col].astype(str).str.strip()
    df[col] = df[col].replace({'': np.nan, 'nan': np.nan, 'NaN': np.nan, 'None': np.nan})

print(f"✓ Cleaned {len(cat_cols)} categorical columns\n")

# ================================================================================
# SECTION 10: DETAILED ANALYSIS OF ROWS TO BE DROPPED
# ================================================================================

print("Step 10: Analyzing rows with remaining missing negotiated_dollar...")
print("-" * 80)

df_to_drop = df[df['negotiated_dollar'].isna()].copy()
rows_to_drop_count = len(df_to_drop)

if rows_to_drop_count > 0:
    print(f"\n{rows_to_drop_count:,} rows will be dropped due to missing negotiated_dollar\n")
    
    # Breakdown by payer group
    print("Breakdown by PAYER_GROUP:")
    print("-" * 80)
    payer_breakdown = df_to_drop['payer_group'].value_counts().reset_index()
    payer_breakdown.columns = ['Payer Group', 'Count']
    payer_breakdown['Percent'] = (payer_breakdown['Count'] / rows_to_drop_count * 100).round(2)
    payer_breakdown = payer_breakdown.sort_values('Count', ascending=False)
    print(payer_breakdown.to_string(index=False))
    
    # Breakdown by treatment category
    print("\n\nBreakdown by TREATMENT_CATEGORY:")
    print("-" * 80)
    treatment_breakdown = df_to_drop['Treatment_Category'].value_counts().reset_index()
    treatment_breakdown.columns = ['Treatment Category', 'Count']
    treatment_breakdown['Percent'] = (treatment_breakdown['Count'] / rows_to_drop_count * 100).round(2)
    treatment_breakdown = treatment_breakdown.sort_values('Count', ascending=False)
    print(treatment_breakdown.to_string(index=False))
    
    # Cross-tabulation: Payer × Treatment
    print("\n\nCross-tabulation: PAYER_GROUP × TREATMENT_CATEGORY:")
    print("-" * 80)
    crosstab = pd.crosstab(df_to_drop['payer_group'], df_to_drop['Treatment_Category'], margins=True)
    print(crosstab)
    
    # Sample of rows to be dropped
    print("\n\nSample rows that will be DROPPED:")
    print("-" * 80)
    sample_drop = df_to_drop[['code', 'hospital_name', 'payer_group', 'Treatment_Category', 
                               'gross', 'negotiated_percentage', 'negotiated_dollar']].head(10)
    print(sample_drop.to_string())
    
    print()
else:
    print(f"✓ No rows to drop - all negotiated_dollar values are complete after Method 1!\n")

# ================================================================================
# SECTION 11: DROP ROWS WITH REMAINING MISSING NEGOTIATED_DOLLAR
# ================================================================================

print("Step 11: Dropping rows with remaining missing negotiated_dollar...")
print("-" * 80)

missing_before_drop = int(df['negotiated_dollar'].isna().sum())
total_before = len(df)

if missing_before_drop > 0:
    df = df[df['negotiated_dollar'].notna()].copy()
    total_after = len(df)
    rows_dropped = total_before - total_after
    retention_pct = (total_after / total_before) * 100
    
    print(f"✓ Dropped {rows_dropped:,} rows with missing negotiated_dollar")
    print(f"✓ Retained {total_after:,} rows ({retention_pct:.1f}% of original dataset)")
else:
    rows_dropped = 0
    total_after = len(df)
    retention_pct = 100.0
    print("✓ No rows dropped - all negotiated_dollar values present after Method 1")

print()

# ================================================================================
# SECTION 12: BEFORE/AFTER COMPARISON BY PAYER GROUP
# ================================================================================

print("Step 12: Impact analysis on payer groups...")
print("-" * 80)

print("\nData retention by PAYER_GROUP:")
payer_impact = (
    pd.DataFrame({
        'Before': df_to_drop['payer_group'].value_counts() if rows_dropped > 0 else pd.Series(dtype=int),
        'Dropped': df_to_drop['payer_group'].value_counts() if rows_dropped > 0 else pd.Series(dtype=int),
    })
)

# Recalculate for correct impact analysis
if rows_dropped > 0:
    before_payer = df_to_drop.groupby('payer_group').size()
    after_payer = df.groupby('payer_group').size()
    
    impact = pd.DataFrame({
        'Dropped_Count': before_payer,
        'Remaining_Count': after_payer,
    }).fillna(0).astype(int)
    
    impact['Dropped_Percent'] = (impact['Dropped_Count'] / (impact['Dropped_Count'] + impact['Remaining_Count']) * 100).round(2)
    impact['Retention_Percent'] = (impact['Remaining_Count'] / (impact['Dropped_Count'] + impact['Remaining_Count']) * 100).round(2)
    
    print(impact.to_string())
else:
    print("No rows dropped")

print()

# ================================================================================
# SECTION 13: OUTLIER DETECTION
# ================================================================================

print("Step 13: Detecting outliers (IQR method)...")
print("-" * 80)

if num_cols:
    Q1 = df[num_cols].quantile(0.25)
    Q3 = df[num_cols].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    print("\n⚠️  OUTLIERS IDENTIFIED (not removed – kept for analysis)\n")
    
    for col in num_cols:
        outliers_low = (df[col] < lower_bound[col]).sum()
        outliers_high = (df[col] > upper_bound[col]).sum()
        outliers_total = outliers_low + outliers_high
        pct = (outliers_total / len(df)) * 100
        
        print(f"{col}:")
        print(f"  Range: [{lower_bound[col]:,.2f} to {upper_bound[col]:,.2f}]")
        print(f"  Low outliers:  {outliers_low:,}")
        print(f"  High outliers: {outliers_high:,}")
        print(f"  TOTAL: {outliers_total:,} ({pct:.2f}%)")
    
    # Flag outliers for negotiated_dollar
    if 'negotiated_dollar' in num_cols:
        df['outlier_flag_negotiated_dollar'] = (
            (df['negotiated_dollar'] < lower_bound['negotiated_dollar']) |
            (df['negotiated_dollar'] > upper_bound['negotiated_dollar'])
        ).astype(int)

print()

# ================================================================================
# SECTION 14: CREATE LOG-TRANSFORMED COLUMNS
# ================================================================================

print("Step 14: Creating log-transformed columns...")
print("-" * 80)

if 'negotiated_dollar' in df.columns:
    df['log_negotiated_dollar'] = np.log1p(df['negotiated_dollar'])
    print("✓ Created log_negotiated_dollar")

if 'estimated_amount' in df.columns:
    df['log_estimated_amount'] = np.log1p(df['estimated_amount'])
    print("✓ Created log_estimated_amount\n")

# ================================================================================
# SECTION 15: FINAL DATA QUALITY CHECK
# ================================================================================

print("Step 15: Final data quality check...")
print("-" * 80)

missing_final = df.isnull().sum()
pricing_cols_in_df = [c for c in ['negotiated_percentage', 'negotiated_dollar', 'estimated_amount'] if c in df.columns]
missing_pricing = missing_final[pricing_cols_in_df]

if missing_pricing.sum() == 0:
    print("✓ No missing values in key pricing columns\n")
else:
    print("Remaining missing values:")
    print(missing_pricing[missing_pricing > 0].to_string())
    print()

# ================================================================================
# SECTION 16: IMPUTATION & DROPPING SUMMARY REPORT
# ================================================================================

print("\n" + "=" * 80)
print("IMPUTATION & DATA RETENTION SUMMARY")
print("=" * 80)

print("\nImputation Method Distribution (among RETAINED rows):")
print("-" * 80)
imputation_counts = df['imputation_method'].value_counts()
for method, count in imputation_counts.items():
    pct = (count / len(df)) * 100
    print(f"{method:60s}: {count:7,} ({pct:5.1f}%)")

print("\n\nData handling summary:")
print("-" * 80)
print(f"Originally missing negotiated_dollar:       {original_missing_negotiated_dollar:,}")
print(f"  ├─ Imputed via Method 1:                  {method_1_count:,}")
print(f"  └─ Dropped (no Method 1 feasible):        {rows_dropped:,}")
print()
print(f"Final dataset:")
print(f"  Total rows:                               {len(df):,}")
print(f"  Rows retained:                            {len(df):,} ({retention_pct:.1f}% of original)")
print(f"  Rows dropped:                             {rows_dropped:,}")

if original_missing_negotiated_dollar == (method_1_count + rows_dropped):
    print(f"\n✓ VERIFICATION PASSED: All originally missing values accounted for.")
else:
    print(f"\n⚠ WARNING: Mismatch in accounting.")

print()

# ================================================================================
# SECTION 17: SUMMARY STATISTICS
# ================================================================================

print("\n" + "=" * 80)
print("FINAL CLEANED DATASET SUMMARY")
print("=" * 80)

summary_stats = {
    'Total Rows': len(df),
    'Unique Hospitals': df['hospital_name'].nunique(),
    'Unique Cities': df['City'].nunique(),
    'Unique CPT Codes': df['code'].nunique(),
    'Unique Treatment Categories': df['Treatment_Category'].nunique(),
    'Unique Payers': df['payer_name'].nunique(),
    'Unique Payer Groups': df['payer_group'].nunique(),
}

print("\nDataset Summary:")
for key, value in summary_stats.items():
    print(f"  {key:30s}: {value:>10,}")

print("\nPRICING STATISTICS (negotiated_dollar):")
price_stats = df['negotiated_dollar'].describe()
print(f"  Mean:   ${price_stats['mean']:>12,.2f}")
print(f"  Median: ${df['negotiated_dollar'].median():>12,.2f}")
print(f"  Min:    ${price_stats['min']:>12,.2f}")
print(f"  Max:    ${price_stats['max']:>12,.2f}")
print(f"  Std:    ${price_stats['std']:>12,.2f}")
print(f"  CV:     {price_stats['std']/price_stats['mean']*100:>12.1f}%")

print("\nTREATMENT CATEGORY DISTRIBUTION:")
for category, count in df['Treatment_Category'].value_counts().items():
    pct = (count / len(df)) * 100
    print(f"  {category:50s}: {count:7,} ({pct:5.1f}%)")

print("\nPAYER GROUP DISTRIBUTION:")
for payer, count in df['payer_group'].value_counts().items():
    pct = (count / len(df)) * 100
    print(f"  {payer:30s}: {count:7,} ({pct:5.1f}%)")

print()

# ================================================================================
# SECTION 18: SAVE CLEANED DATA
# ================================================================================

print("Step 18: Saving cleaned dataset...")
print("-" * 80)

df.to_csv(OUTPUT_PATH, index=False)
print(f"✓ Saved to: {OUTPUT_PATH}")
print(f"  Rows: {len(df):,}")
print(f"  Columns: {len(df.columns)}\n")

# ================================================================================
# COMPLETION MESSAGE
# ================================================================================

print("=" * 80)
print("✅ DATA CLEANING COMPLETE")
print("=" * 80)
print("\nKey Deliverables:")
print(f"  ✓ Method 1 imputation applied (gross × negotiated_percentage)")
print(f"  ✓ {method_1_count:,} rows imputed via Method 1")
print(f"  ✓ {rows_dropped:,} rows dropped (remaining missing negotiated_dollar)")
print(f"  ✓ Detailed breakdown of dropped rows by payer and treatment")
print(f"  ✓ Outlier flags created")
print(f"  ✓ Log-transformed columns created")
print(f"  ✓ Data retention: {retention_pct:.1f}% of original dataset\n")

print("=" * 80 + "\n")

# ================================================================================
# SECTION 19: VISUALIZATION & EXPLORATORY ANALYSIS
# ================================================================================

print("=" * 80)
print("SECTION 19: EXPLORATORY VISUALIZATIONS")
print("=" * 80 + "\n")

df_viz = df[df['negotiated_dollar'].notna()].copy()

print(f"Generating visualizations for {len(df_viz):,} rows...\n")

# ================================================================================
# VISUALIZATION 1: BOXPLOT BY CITY
# ================================================================================

print("Step 19.1: Creating Boxplot - Negotiated Dollar by City...")
print("-" * 80)

try:
    city_stats_viz = (
        df_viz.groupby('City')['negotiated_dollar']
        .agg(['count', 'mean', 'median', 'min', 'max'])
        .reset_index()
    )
    
    MIN_ROWS_PER_CITY = 200
    big_cities = city_stats_viz[city_stats_viz['count'] >= MIN_ROWS_PER_CITY]['City'].tolist()
    
    print(f"✓ Cities with ≥{MIN_ROWS_PER_CITY} observations: {len(big_cities)}")
    
    if len(big_cities) > 0:
        df_big_cities = df_viz[df_viz['City'].isin(big_cities)].copy()
        
        order_for_plot = (
            df_big_cities.groupby('City')['negotiated_dollar']
            .median()
            .sort_values()
            .index
            .tolist()
        )
        
        plt.figure(figsize=(14, 7))
        data_for_box = [
            df_big_cities.loc[df_big_cities['City'] == c, 'negotiated_dollar'].values 
            for c in order_for_plot
        ]
        
        plt.boxplot(
            data_for_box, 
            labels=order_for_plot, 
            showfliers=False, 
            patch_artist=True,
            boxprops=dict(facecolor='lightblue', alpha=0.7),
            medianprops=dict(color='red', linewidth=2)
        )
        
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.ylabel("Negotiated Dollar ($)", fontsize=12)
        plt.xlabel("City", fontsize=12)
        plt.title(
            f"Negotiated Dollar by City (n={len(big_cities)} cities with ≥{MIN_ROWS_PER_CITY} observations)\nOutliers hidden for clarity", 
            fontsize=13, fontweight='bold'
        )
        plt.grid(axis='y', alpha=0.3, linestyle='--')
        plt.tight_layout()
        
        plot_path_1 = os.path.join(VISUALISATION_FOLDER, 'viz_01_negotiated_dollar_by_city.png')
        plt.savefig(plot_path_1, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {plot_path_1}")
        plt.close()
    else:
        print("⚠ No cities with sufficient data (≥200 observations)")

except Exception as e:
    print(f"⚠ Error: {e}")

print()

# ================================================================================
# VISUALIZATION 2: BOXPLOT BY TREATMENT CATEGORY
# ================================================================================

print("Step 19.2: Creating Boxplot - Negotiated Dollar by Treatment Category...")
print("-" * 80)

try:
    plt.figure(figsize=(16, 7))
    
    df_viz.boxplot(
        column='negotiated_dollar',
        by='Treatment_Category',
        showfliers=False,
        patch_artist=True,
        figsize=(16, 7)
    )
    
    plt.suptitle("")
    plt.title(
        "Negotiated Dollar by Treatment Category\n(Outliers hidden for clarity)", 
        fontsize=13, fontweight='bold'
    )
    plt.xlabel("Treatment Category", fontsize=12)
    plt.ylabel("Negotiated Dollar ($)", fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    plot_path_2 = os.path.join(VISUALISATION_FOLDER, 'viz_02_negotiated_dollar_by_treatment.png')
    plt.savefig(plot_path_2, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {plot_path_2}")
    plt.close()

except Exception as e:
    print(f"⚠ Error: {e}")

print()

# ================================================================================
# VISUALIZATION 3: HEATMAP - TREATMENT × PAYER GROUP
# ================================================================================

print("Step 19.3: Creating Heatmap - Median Price by Treatment × Payer Group...")
print("-" * 80)

try:
    cat_payer = (
        df_viz.groupby(['Treatment_Category', 'payer_group'])['negotiated_dollar']
        .median()
        .reset_index()
    )
    
    pivot_cat_payer = cat_payer.pivot(
        index='Treatment_Category',
        columns='payer_group',
        values='negotiated_dollar'
    )
    
    plt.figure(figsize=(12, 10))
    sns.heatmap(
        pivot_cat_payer, 
        annot=True, 
        fmt='.0f', 
        cmap='YlOrRd', 
        cbar_kws={'label': 'Median Negotiated Dollar ($)'}, 
        linewidths=0.5
    )
    
    plt.title(
        "Median Negotiated Dollar by Treatment Category and Payer Group", 
        fontsize=13, fontweight='bold', pad=20
    )
    plt.xlabel("Payer Group", fontsize=12)
    plt.ylabel("Treatment Category", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    
    plot_path_3 = os.path.join(VISUALISATION_FOLDER, 'viz_03_heatmap_treatment_payer.png')
    plt.savefig(plot_path_3, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {plot_path_3}")
    plt.close()

except Exception as e:
    print(f"⚠ Error: {e}")

print()

# ================================================================================
# VISUALIZATION 4: BOXPLOT BY PAYER GROUP
# ================================================================================

print("Step 19.4: Creating Boxplot - Negotiated Dollar by Payer Group...")
print("-" * 80)

try:
    plt.figure(figsize=(12, 7))
    
    df_viz.boxplot(
        column='negotiated_dollar',
        by='payer_group',
        showfliers=False,
        patch_artist=True
    )
    
    plt.suptitle("")
    plt.title(
        "Negotiated Dollar by Payer Group\n(Outliers hidden for clarity)", 
        fontsize=13, fontweight='bold'
    )
    plt.xlabel("Payer Group", fontsize=12)
    plt.ylabel("Negotiated Dollar ($)", fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    plot_path_4 = os.path.join(VISUALISATION_FOLDER, 'viz_04_negotiated_dollar_by_payer.png')
    plt.savefig(plot_path_4, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {plot_path_4}")
    plt.close()

except Exception as e:
    print(f"⚠ Error: {e}")

print()

# ================================================================================
# VISUALIZATION SUMMARY
# ================================================================================

print("=" * 80)
print("VISUALIZATION SUMMARY")
print("=" * 80)
print("\n✓ 4 exploratory visualizations created and saved to visualisation subfolder:\n")
print("  1. visualisation/viz_01_negotiated_dollar_by_city.png")
print("  2. visualisation/viz_02_negotiated_dollar_by_treatment.png")
print("  3. visualisation/viz_03_heatmap_treatment_payer.png")
print("  4. visualisation/viz_04_negotiated_dollar_by_payer.png")
print()
print(f"Location: {VISUALISATION_FOLDER}\n")
print("=" * 80 + "\n")

# ================================================================================
# FINAL COMPLETION
# ================================================================================

print("\n" + "=" * 80)
print("🎉 ALL PROCESSING COMPLETE!")
print("=" * 80)
print(f"\n✓ Cleaned data saved to: {OUTPUT_PATH}")
print(f"✓ Visualizations saved to: {VISUALISATION_FOLDER}/")
print(f"\nFolder Structure:")
print(f"  Results/")
print(f"  ├─ visualisation/  (4 PNG files)")
print(f"  └─ (other analysis files)")
print(f"\nNext steps: Load the cleaned data and begin statistical analysis!")
print("=" * 80 + "\n")