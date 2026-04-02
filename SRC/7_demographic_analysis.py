"""
================================================================================
DEMOGRAPHIC ANALYSIS - FOCUSED ON KEY VARIABLES
================================================================================

PURPOSE:
    Analyze healthcare pricing in relation to 5 KEY demographic factors:
    1. Income (% Population earning $100K+)
    2. Age (% Population 65+)
    3. Insurance (% Private, % Public, % Uninsured)
    4. Population Size (Market competition indicator)
    5. Diversity (Optional: % Hispanic + simple diversity metric)

APPROACH:
    - Skip granular data, focus on what matters
    - Cleaner analysis, stronger signals
    - Easier to interpret and present
    - Publication-ready results

INPUT:
    1. 4_cleaned_prices_with_imputation_tracking.csv (your cleaned pricing)
    2. Florida_City_Demographics.xlsx (Census Bureau data)

OUTPUT:
    1. City-level merged data (key variables only)
    2. Comparison: HIGH vs LOW price variation
    3. Correlation analysis (focused on key variables)
    4. 4 focused visualizations
    5. Summary report with clear findings

================================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import spearmanr, ttest_ind
import warnings
import os

warnings.filterwarnings('ignore')

print("\n" + "=" * 80)
print("DEMOGRAPHIC ANALYSIS - FOCUSED ON KEY VARIABLES")
print("Healthcare Pricing × Income × Age × Insurance × Population")
print("=" * 80 + "\n")

# ================================================================================
# CONFIGURATION - ALL PATHS DEFINED HERE
# ================================================================================

# Input Data Folders
DATA_FOLDER = r'C:\Robby\BANA 650_Healthcare\DATA'

# Input Files
PRICING_FILE = '4_cleaned_prices_with_imputation_tracking.csv'
DEMOGRAPHICS_FILE = r'C:\Robby\BANA 650_Healthcare\DATA\Florida City Demographics.xlsx'  # With space in filename

PRICING_PATH = os.path.join(DATA_FOLDER, PRICING_FILE)

# Output Results Folders
RESULTS_FOLDER = r'C:\Robby\BANA 650_Healthcare\RESULTS'
VISUALISATION_FOLDER = os.path.join(RESULTS_FOLDER, 'visualisation')  # For PNG visualizations
GEOSPATIAL_FOLDER = os.path.join(RESULTS_FOLDER, 'geospatial')       # For future geospatial work

# Script Location
SRC_FOLDER = r'C:\Robby\BANA 650_Healthcare\SRC'
SCRIPT_NAME = '7_demographic_analysis.py'

# Create visualisation subfolder if it doesn't exist
if not os.path.exists(VISUALISATION_FOLDER):
    os.makedirs(VISUALISATION_FOLDER)
    print(f"✓ Created visualisation folder: {VISUALISATION_FOLDER}\n")

# ================================================================================
# SECTION 1: LOAD DATA
# ================================================================================

print("SECTION 1: LOADING DATA")
print("-" * 80)

try:
    df_pricing = pd.read_csv(PRICING_PATH, low_memory=False)
    print(f"✓ Loaded pricing data: {df_pricing.shape[0]:,} rows")
    print(f"  • Cities: {df_pricing['City'].nunique()}")
except Exception as e:
    print(f"✗ ERROR loading pricing: {e}")
    exit()

try:
    df_demo_raw = pd.read_excel(DEMOGRAPHICS_FILE, sheet_name='Sheet1')
    print(f"✓ Loaded demographics: {df_demo_raw.shape[0]} cities × {df_demo_raw.shape[1]} columns")
except Exception as e:
    print(f"✗ ERROR loading demographics Excel file")
    print(f"   Update DEMOGRAPHICS_FILE path in script (line 51)")
    exit()

print()

# ================================================================================
# SECTION 2: EXTRACT KEY DEMOGRAPHIC VARIABLES ONLY
# ================================================================================

print("SECTION 2: EXTRACTING KEY DEMOGRAPHIC VARIABLES")
print("-" * 80)

df_demo = pd.DataFrame()

# City identifier
df_demo['Geographic_Full'] = df_demo_raw['Geographic Area Name']
df_demo['City_Demo'] = df_demo_raw['Geographic Area Name'].str.split(' CCD').str[0].str.strip()

# ============================================================================
# KEY VARIABLE 1: INCOME
# ============================================================================
print("Extracting: Income (% earning $100K+)...")

# Get income brackets
income_100k_plus = df_demo_raw['Estimate!!Total:!!$100,000 or more:']
total_population = df_demo_raw['Estimate!!SEX AND AGE!!Total population']

df_demo['Total_Population'] = total_population
df_demo['Income_100K_Plus_Count'] = income_100k_plus
df_demo['Pct_Income_100K_Plus'] = (income_100k_plus / total_population * 100).round(1)

# Also get income distribution for context
df_demo['Income_Under25K'] = df_demo_raw['Estimate!!Total:!!Under $25,000:']
df_demo['Income_25K_50K'] = df_demo_raw['Estimate!!Total:!!$25,000 to $49,999:']
df_demo['Income_50K_75K'] = df_demo_raw['Estimate!!Total:!!$50,000 to $74,999:']
df_demo['Income_75K_100K'] = df_demo_raw['Estimate!!Total:!!$75,000 to $99,999:']

# Calculate percentages
df_demo['Pct_Income_Under25K'] = (df_demo['Income_Under25K'] / total_population * 100).round(1)
df_demo['Pct_Income_25K_50K'] = (df_demo['Income_25K_50K'] / total_population * 100).round(1)
df_demo['Pct_Income_50K_75K'] = (df_demo['Income_50K_75K'] / total_population * 100).round(1)
df_demo['Pct_Income_75K_100K'] = (df_demo['Income_75K_100K'] / total_population * 100).round(1)

# ============================================================================
# KEY VARIABLE 2: AGE (65+)
# ============================================================================
print("Extracting: Age (% 65+)...")

df_demo['Population_65Plus'] = df_demo_raw['Estimate!!SEX AND AGE!!Total population!!65 years and over']
df_demo['Pct_65Plus'] = (df_demo['Population_65Plus'] / total_population * 100).round(1)
df_demo['Median_Age'] = df_demo_raw['Estimate!!SEX AND AGE!!Total population!!Median age (years)']

# ============================================================================
# KEY VARIABLE 3: INSURANCE COVERAGE
# ============================================================================
print("Extracting: Insurance (Private, Public, Uninsured)...")

# Insurance data is organized under income brackets
# Using the $100K+ bracket as it aligns with our income focus
df_demo['Population_Insured'] = df_demo_raw['Estimate!!Total:!!$100,000 or more:!!With health insurance coverage']
df_demo['Population_Private_Insurance'] = df_demo_raw['Estimate!!Total:!!$100,000 or more:!!With health insurance coverage!!With private health insurance']
df_demo['Population_Public_Insurance'] = df_demo_raw['Estimate!!Total:!!$100,000 or more:!!With health insurance coverage!!With public coverage']
df_demo['Population_Uninsured'] = df_demo_raw['Estimate!!Total:!!$100,000 or more:!!No health insurance coverage']

# Calculate percentages (using $100K+ population as base)
income_100k_pop = df_demo_raw['Estimate!!Total:!!$100,000 or more:']
df_demo['Pct_Insured'] = (df_demo['Population_Insured'] / income_100k_pop * 100).round(1)
df_demo['Pct_Private_Insurance'] = (df_demo['Population_Private_Insurance'] / income_100k_pop * 100).round(1)
df_demo['Pct_Public_Insurance'] = (df_demo['Population_Public_Insurance'] / income_100k_pop * 100).round(1)
df_demo['Pct_Uninsured'] = (df_demo['Population_Uninsured'] / income_100k_pop * 100).round(1)

# ============================================================================
# KEY VARIABLE 4: RACE/ETHNICITY (for diversity & optional analysis)
# ============================================================================
print("Extracting: Race/Ethnicity (for diversity index)...")

df_demo['Population_White'] = df_demo_raw['Estimate!!RACE!!Total population!!One race!!White']
df_demo['Population_Black'] = df_demo_raw['Estimate!!RACE!!Total population!!One race!!Black or African American']
df_demo['Population_Asian'] = df_demo_raw['Estimate!!RACE!!Total population!!One race!!Asian']
df_demo['Population_Hispanic'] = df_demo_raw['Estimate!!HISPANIC OR LATINO AND RACE!!Total population!!Hispanic or Latino (of any race)']

# Calculate percentages
df_demo['Pct_White'] = (df_demo['Population_White'] / total_population * 100).round(1)
df_demo['Pct_Black'] = (df_demo['Population_Black'] / total_population * 100).round(1)
df_demo['Pct_Asian'] = (df_demo['Population_Asian'] / total_population * 100).round(1)
df_demo['Pct_Hispanic'] = (df_demo['Population_Hispanic'] / total_population * 100).round(1)

# Diversity Index (0-100, higher = more diverse)
df_demo['Diversity_Index'] = (100 - (
    df_demo['Pct_White']**2 + df_demo['Pct_Black']**2 + 
    df_demo['Pct_Asian']**2 + df_demo['Pct_Hispanic']**2
) / 100).round(1)

print(f"✓ Extracted all key variables successfully")
print()

# ================================================================================
# SECTION 3: MATCH AND MERGE DATASETS
# ================================================================================

print("SECTION 3: MATCHING AND MERGING DATASETS")
print("-" * 80)

# Clean pricing city names
df_pricing['City_Clean'] = df_pricing['City'].str.upper().str.strip()

# Clean demographic city names
df_demo['City_Demo_Upper'] = df_demo['City_Demo'].str.upper()

# Get unique cities
pricing_cities = set(df_pricing['City_Clean'].unique())
demo_cities = set(df_demo['City_Demo_Upper'].unique())

matched = len(pricing_cities.intersection(demo_cities))

print(f"Pricing data cities: {len(pricing_cities)}")
print(f"Demographics cities: {len(demo_cities)}")
print(f"Matched cities: {matched}")
print()

# ================================================================================
# SECTION 4: CALCULATE CITY-LEVEL STATISTICS
# ================================================================================

print("SECTION 4: CALCULATING CITY-LEVEL STATISTICS")
print("-" * 80)

# Group pricing by city
city_stats = df_pricing.groupby('City_Clean').agg({
    'negotiated_dollar': ['mean', 'median', 'std', 'count'],
    'outlier_flag_negotiated_dollar': 'sum'
}).reset_index()

# Flatten column names
city_stats.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in city_stats.columns.values]

# Rename
city_stats.rename(columns={
    'City_Clean': 'City',
    'negotiated_dollar_mean': 'Mean_Price',
    'negotiated_dollar_median': 'Median_Price',
    'negotiated_dollar_std': 'Std_Price',
    'negotiated_dollar_count': 'N_Records',
    'outlier_flag_negotiated_dollar_sum': 'N_Outliers'
}, inplace=True)

# Calculate CV
city_stats['CV_Percent'] = (city_stats['Std_Price'] / city_stats['Mean_Price'] * 100).round(2)

# Merge with demographics (KEY VARIABLES ONLY)
demo_for_merge = df_demo[[
    'City_Demo_Upper',
    'Total_Population',
    'Pct_Income_100K_Plus',
    'Pct_Income_Under25K',
    'Pct_65Plus',
    'Median_Age',
    'Pct_Private_Insurance',
    'Pct_Public_Insurance',
    'Pct_Uninsured',
    'Pct_Hispanic',
    'Diversity_Index'
]]

city_stats = city_stats.merge(
    demo_for_merge,
    left_on='City',
    right_on='City_Demo_Upper',
    how='left'
)

city_stats = city_stats.drop('City_Demo_Upper', axis=1)

print(f"✓ City-level statistics: {len(city_stats)} cities")
print(f"✓ Merged with key demographic variables")
print()

# Display sample
print("Sample city-level data:")
sample_cols = ['City', 'Mean_Price', 'CV_Percent', 'Pct_Income_100K_Plus', 'Pct_65Plus', 'Pct_Private_Insurance']
print(city_stats[sample_cols].head(10).to_string(index=False))
print()

# ================================================================================
# SECTION 5: CLASSIFY CITIES BY PRICE VARIATION
# ================================================================================

print("SECTION 5: CLASSIFYING CITIES BY PRICE VARIATION")
print("-" * 80)

median_cv = city_stats['CV_Percent'].median()
mean_cv = city_stats['CV_Percent'].mean()

city_stats['Variation_Group'] = city_stats['CV_Percent'].apply(
    lambda x: 'HIGH' if x > median_cv else 'LOW'
)

high_var = city_stats[city_stats['Variation_Group'] == 'HIGH'].copy()
low_var = city_stats[city_stats['Variation_Group'] == 'LOW'].copy()

print(f"Median CV: {median_cv:.1f}%")
print(f"Mean CV: {mean_cv:.1f}%")
print(f"HIGH variation cities: {len(high_var)} (CV > {median_cv:.1f}%)")
print(f"LOW variation cities: {len(low_var)} (CV ≤ {median_cv:.1f}%)")
print()

# ================================================================================
# SECTION 6: COMPARE KEY VARIABLES BETWEEN GROUPS
# ================================================================================

print("SECTION 6: COMPARING KEY VARIABLES (HIGH vs LOW Variation)")
print("-" * 80)

key_metrics = [
    'Pct_Income_100K_Plus',
    'Pct_65Plus',
    'Pct_Private_Insurance',
    'Pct_Public_Insurance',
    'Pct_Uninsured',
    'Total_Population',
    'Pct_Hispanic',
    'Diversity_Index'
]

comparison = []

for metric in key_metrics:
    high_val = high_var[metric].dropna()
    low_val = low_var[metric].dropna()
    
    high_median = high_val.median()
    low_median = low_val.median()
    diff = high_median - low_median
    
    # T-test
    if len(high_val) > 1 and len(low_val) > 1:
        t_stat, p_value = ttest_ind(high_val, low_val)
        sig = "✓ YES" if p_value < 0.05 else "  NO"
    else:
        p_value, sig = np.nan, "  N/A"
    
    comparison.append({
        'Metric': metric,
        'High_Var_Median': round(high_median, 2),
        'Low_Var_Median': round(low_median, 2),
        'Difference': round(diff, 2),
        'P_Value': round(p_value, 4),
        'Significant': sig
    })

comparison_df = pd.DataFrame(comparison)

print("Group Comparison Results:")
print(comparison_df.to_string(index=False))
print()

# ================================================================================
# SECTION 7: CORRELATION ANALYSIS
# ================================================================================

print("SECTION 7: CORRELATION ANALYSIS (Demographics vs Pricing)")
print("-" * 80)

analysis_set = city_stats.dropna(subset=key_metrics)

correlations = []

for metric in key_metrics:
    # Correlation with Mean Price
    if len(analysis_set) > 2:
        corr_median, p_median = spearmanr(analysis_set[metric], analysis_set['Median_Price'])
        corr_cv, p_cv = spearmanr(analysis_set[metric], analysis_set['CV_Percent'])
    else:
        corr_median = p_median = corr_cv = p_cv = np.nan
    
    correlations.append({
        'Metric': metric,
        'Corr_Median_Price': round(corr_median, 3),
        'P_Median': round(p_median, 4),
        'Sig_Median': "✓" if p_median < 0.05 else "",
        'Corr_CV': round(corr_cv, 3),
        'P_CV': round(p_cv, 4),
        'Sig_CV': "✓" if p_cv < 0.05 else ""
    })

corr_df = pd.DataFrame(correlations)

print("Correlation Results (with Mean Price and Price Variation):")
print(corr_df.to_string(index=False))
print()

# Find significant relationships
sig_median = corr_df[corr_df['Sig_Median'] == '✓'].sort_values('Corr_Median_Price', key=abs, ascending=False)
sig_cv = corr_df[corr_df['Sig_CV'] == '✓'].sort_values('Corr_CV', key=abs, ascending=False)

print(f"✓ Metrics correlated with MEDIAN PRICE: {len(sig_median)}")
for _, row in sig_median.iterrows():
    direction = "↑ increases" if row['Corr_Median_Price'] > 0 else "↓ decreases"
    print(f"    • {row['Metric']}: r={row['Corr_Median_Price']:.3f} {direction}")

print(f"\n✓ Metrics correlated with PRICE VARIATION (CV): {len(sig_cv)}")
for _, row in sig_cv.iterrows():
    direction = "↑ increases" if row['Corr_CV'] > 0 else "↓ decreases"
    print(f"    • {row['Metric']}: r={row['Corr_CV']:.3f} {direction}")

print()

# ================================================================================
# SECTION 8: CREATE FOCUSED VISUALIZATIONS
# ================================================================================

print("SECTION 8: CREATING VISUALIZATIONS")
print("-" * 80)

sns.set_style("whitegrid")

# ============================================================================
# VIZ 1: Key Variables Comparison (HIGH vs LOW variation)
# ============================================================================

focus_metrics = [
    'Pct_Income_100K_Plus',
    'Pct_65Plus',
    'Pct_Private_Insurance',
    'Pct_Uninsured'
]

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Key Demographics: HIGH vs LOW Price Variation Cities', fontsize=16, fontweight='bold')

for idx, metric in enumerate(focus_metrics):
    ax = axes[idx // 2, idx % 2]
    
    data_to_plot = [
        high_var[metric].dropna().values,
        low_var[metric].dropna().values
    ]
    
    bp = ax.boxplot(data_to_plot, labels=['HIGH Variation', 'LOW Variation'], patch_artist=True)
    bp['boxes'][0].set_facecolor('lightcoral')
    bp['boxes'][1].set_facecolor('lightblue')
    
    ax.set_title(metric.replace('Pct_', ''), fontweight='bold', fontsize=12)
    ax.set_ylabel('Percentage (%)')
    ax.grid(True, alpha=0.3)

plt.tight_layout()
viz1_path = os.path.join(VISUALISATION_FOLDER, '13_key_variables_comparison.png')
plt.savefig(viz1_path, dpi=300, bbox_inches='tight')
plt.close()
print(f"✓ Saved: visualisation/13_key_variables_comparison.png")

# ============================================================================
# VIZ 2: Correlation Scatter Plots (Key Variables vs Price Variation)
# ============================================================================

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Key Variables vs Price Variation', fontsize=16, fontweight='bold')

focus_metrics_scatter = [
    'Pct_Income_100K_Plus',
    'Pct_65Plus',
    'Pct_Private_Insurance',
    'Pct_Uninsured'
]

for idx, metric in enumerate(focus_metrics_scatter):
    ax = axes[idx // 2, idx % 2]
    
    data_plot = city_stats.dropna(subset=[metric, 'CV_Percent'])
    
    scatter = ax.scatter(data_plot[metric], data_plot['CV_Percent'],
                        c=data_plot['Median_Price'], cmap='viridis',
                        s=150, alpha=0.6, edgecolors='black', linewidth=1.5)
    
    # Add correlation info
    corr, p = spearmanr(data_plot[metric], data_plot['CV_Percent'])
    sig_marker = "✓" if p < 0.05 else ""
    
    ax.set_xlabel(metric.replace('Pct_', ''), fontweight='bold', fontsize=11)
    ax.set_ylabel('Price Variation (CV%)', fontweight='bold', fontsize=11)
    ax.set_title(f'{metric.replace("Pct_", "")} vs CV\nr={corr:.3f}, p={p:.4f} {sig_marker}', 
                fontweight='bold', fontsize=11)
    ax.grid(True, alpha=0.3)
    
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('Mean Price ($)', fontweight='bold')

plt.tight_layout()
viz2_path = os.path.join(VISUALISATION_FOLDER, '14_key_variables_vs_cv_scatter.png')
plt.savefig(viz2_path, dpi=300, bbox_inches='tight')
plt.close()
print(f"✓ Saved: visualisation/14_key_variables_vs_cv_scatter.png")

# ============================================================================
# VIZ 3: Focused Correlation Heatmap
# ============================================================================

fig, ax = plt.subplots(figsize=(12, 8))

# Select key variables for heatmap
heatmap_metrics = focus_metrics + ['Median_Price', 'CV_Percent']
heatmap_data = city_stats[heatmap_metrics].corr(method='spearman')

sns.heatmap(heatmap_data, annot=True, fmt='.2f', cmap='coolwarm', center=0,
            square=True, ax=ax, cbar_kws={'label': 'Correlation'},
            vmin=-1, vmax=1)

ax.set_title('Correlation Matrix: Key Variables & Pricing', fontsize=14, fontweight='bold')
plt.tight_layout()
viz3_path = os.path.join(VISUALISATION_FOLDER, '15_key_variables_correlation_heatmap.png')
plt.savefig(viz3_path, dpi=300, bbox_inches='tight')
plt.close()
print(f"✓ Saved: visualisation/15_key_variables_correlation_heatmap.png")

# ============================================================================
# VIZ 4: Mean Price by Income & Insurance Groups
# ============================================================================

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle('Mean Price by Income and Insurance Groups', fontsize=16, fontweight='bold')

# Income quartiles
ax1 = axes[0]
city_stats_plot = city_stats.dropna(subset=['Pct_Income_100K_Plus', 'Median_Price']).copy()
city_stats_plot['Income_Quartile'] = pd.qcut(city_stats_plot['Pct_Income_100K_Plus'], 
                                              q=4, labels=['Q1 (Low)', 'Q2', 'Q3', 'Q4 (High)'])
income_stats = city_stats_plot.groupby('Income_Quartile')['Median_Price'].agg(['mean', 'std', 'count'])

ax1.bar(range(len(income_stats)), income_stats['mean'], 
        yerr=income_stats['std'], capsize=5, alpha=0.7, 
        color=['#d62728', '#ff7f0e', '#2ca02c', '#1f77b4'])
ax1.set_xticks(range(len(income_stats)))
ax1.set_xticklabels(income_stats.index)
ax1.set_ylabel('Median Price ($)', fontweight='bold')
ax1.set_xlabel('Income Level Quartile', fontweight='bold')
ax1.set_title('Median Price by Income Quartile')
ax1.grid(True, alpha=0.3, axis='y')

# Private Insurance quartiles
ax2 = axes[1]
city_stats_plot2 = city_stats.dropna(subset=['Pct_Private_Insurance', 'Median_Price']).copy()
city_stats_plot2['Insurance_Quartile'] = pd.qcut(city_stats_plot2['Pct_Private_Insurance'],
                                                   q=4, labels=['Q1 (Low)', 'Q2', 'Q3', 'Q4 (High)'])
insurance_stats = city_stats_plot2.groupby('Insurance_Quartile')['Median_Price'].agg(['mean', 'std', 'count'])

ax2.bar(range(len(insurance_stats)), insurance_stats['mean'],
        yerr=insurance_stats['std'], capsize=5, alpha=0.7,
        color=['#d62728', '#ff7f0e', '#2ca02c', '#1f77b4'])
ax2.set_xticks(range(len(insurance_stats)))
ax2.set_xticklabels(insurance_stats.index)
ax2.set_ylabel('Median Price ($)', fontweight='bold')
ax2.set_xlabel('Private Insurance Quartile', fontweight='bold')
ax2.set_title('Median Price by Private Insurance Quartile')
ax2.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
viz4_path = os.path.join(VISUALISATION_FOLDER, '16_median_price_by_key_groups.png')
plt.savefig(viz4_path, dpi=300, bbox_inches='tight')
plt.close()
print(f"✓ Saved: visualisation/16_mean_price_by_key_groups.png")

print()

# ================================================================================
# SECTION 9: SAVE RESULTS
# ================================================================================

print("SECTION 9: SAVING RESULTS")
print("-" * 80)

# Save merged dataset (KEY VARIABLES ONLY)
output_cols = ['City', 'Mean_Price', 'Median_Price', 'CV_Percent', 'N_Records',
               'Pct_Income_100K_Plus', 'Pct_65Plus', 
               'Pct_Private_Insurance', 'Pct_Public_Insurance', 'Pct_Uninsured',
               'Total_Population', 'Pct_Hispanic', 'Diversity_Index', 'Variation_Group']

city_stats[output_cols].to_csv(
    os.path.join(RESULTS_FOLDER, '5_city_demographic_analysis_focused.csv'),
    index=False
)
print(f"✓ Saved: 5_city_demographic_analysis_focused.csv")

# Save comparison
comparison_df.to_csv(
    os.path.join(RESULTS_FOLDER, '13_key_variables_comparison_stats.csv'),
    index=False
)
print(f"✓ Saved: 13_key_variables_comparison_stats.csv")

# Save correlations
corr_df.to_csv(
    os.path.join(RESULTS_FOLDER, '14_key_variables_correlations.csv'),
    index=False
)
print(f"✓ Saved: 14_key_variables_correlations.csv")

print()

# ================================================================================
# SECTION 10: GENERATE FOCUSED SUMMARY REPORT
# ================================================================================

print("SECTION 10: GENERATING SUMMARY REPORT")
print("-" * 80)

report = f"""
{'='*80}
FOCUSED DEMOGRAPHIC ANALYSIS - FINAL REPORT
Healthcare Pricing × Key Demographic Factors
{'='*80}

FOCUS: 4 KEY VARIABLES
─────────────────────────────────────────────────────────────────────────────
1. Income Level (% earning $100K+)
   → Proxy for purchasing power & negotiating ability

2. Age (% Population 65+)
   → Proxy for healthcare usage & Medicare prevalence

3. Insurance Mix (% Private, % Public, % Uninsured)
   → Directly determines pricing mechanisms & negotiation patterns

4. Population Size
   → Indicator of market competition

ANALYSIS DATASET
─────────────────────────────────────────────────────────────────────────────
Pricing Data: {df_pricing.shape[0]:,} records from {df_pricing['City'].nunique()} cities
Demographics: {df_demo_raw.shape[0]} cities
Merged Data: {len(city_stats)} cities with both pricing & demographics

PRICE VARIATION CLASSIFICATION
─────────────────────────────────────────────────────────────────────────────
Median CV: {median_cv:.1f}%
Mean CV: {mean_cv:.1f}%
Range: {city_stats['CV_Percent'].min():.1f}% to {city_stats['CV_Percent'].max():.1f}%

HIGH Variation Cities: {len(high_var)} (CV > {median_cv:.1f}%)
LOW Variation Cities: {len(low_var)} (CV ≤ {median_cv:.1f}%)

KEY FINDINGS: GROUP COMPARISON (HIGH vs LOW Variation)
─────────────────────────────────────────────────────────────────────────────

Income Level (% earning $100K+):
  HIGH variation cities: {comparison_df[comparison_df['Metric']=='Pct_Income_100K_Plus']['High_Var_Median'].values[0]:.1f}%
  LOW variation cities:  {comparison_df[comparison_df['Metric']=='Pct_Income_100K_Plus']['Low_Var_Median'].values[0]:.1f}%
  Difference: {comparison_df[comparison_df['Metric']=='Pct_Income_100K_Plus']['Difference'].values[0]:.1f}% {comparison_df[comparison_df['Metric']=='Pct_Income_100K_Plus']['Significant'].values[0]}

Senior Population (% 65+):
  HIGH variation cities: {comparison_df[comparison_df['Metric']=='Pct_65Plus']['High_Var_Median'].values[0]:.1f}%
  LOW variation cities:  {comparison_df[comparison_df['Metric']=='Pct_65Plus']['Low_Var_Median'].values[0]:.1f}%
  Difference: {comparison_df[comparison_df['Metric']=='Pct_65Plus']['Difference'].values[0]:.1f}% {comparison_df[comparison_df['Metric']=='Pct_65Plus']['Significant'].values[0]}

Private Insurance Coverage:
  HIGH variation cities: {comparison_df[comparison_df['Metric']=='Pct_Private_Insurance']['High_Var_Median'].values[0]:.1f}%
  LOW variation cities:  {comparison_df[comparison_df['Metric']=='Pct_Private_Insurance']['Low_Var_Median'].values[0]:.1f}%
  Difference: {comparison_df[comparison_df['Metric']=='Pct_Private_Insurance']['Difference'].values[0]:.1f}% {comparison_df[comparison_df['Metric']=='Pct_Private_Insurance']['Significant'].values[0]}

Public Insurance Coverage:
  HIGH variation cities: {comparison_df[comparison_df['Metric']=='Pct_Public_Insurance']['High_Var_Median'].values[0]:.1f}%
  LOW variation cities:  {comparison_df[comparison_df['Metric']=='Pct_Public_Insurance']['Low_Var_Median'].values[0]:.1f}%
  Difference: {comparison_df[comparison_df['Metric']=='Pct_Public_Insurance']['Difference'].values[0]:.1f}% {comparison_df[comparison_df['Metric']=='Pct_Public_Insurance']['Significant'].values[0]}

Uninsured Population:
  HIGH variation cities: {comparison_df[comparison_df['Metric']=='Pct_Uninsured']['High_Var_Median'].values[0]:.1f}%
  LOW variation cities:  {comparison_df[comparison_df['Metric']=='Pct_Uninsured']['Low_Var_Median'].values[0]:.1f}%
  Difference: {comparison_df[comparison_df['Metric']=='Pct_Uninsured']['Difference'].values[0]:.1f}% {comparison_df[comparison_df['Metric']=='Pct_Uninsured']['Significant'].values[0]}

Population Size (Total):
  HIGH variation cities: {comparison_df[comparison_df['Metric']=='Total_Population']['High_Var_Median'].values[0]:,.0f}
  LOW variation cities:  {comparison_df[comparison_df['Metric']=='Total_Population']['Low_Var_Median'].values[0]:,.0f}
  Difference: {comparison_df[comparison_df['Metric']=='Total_Population']['Difference'].values[0]:,.0f} {comparison_df[comparison_df['Metric']=='Total_Population']['Significant'].values[0]}

KEY FINDINGS: CORRELATION ANALYSIS
─────────────────────────────────────────────────────────────────────────────

PREDICTING MEAN PRICE:
"""

if len(sig_median) > 0:
    for _, row in sig_median.iterrows():
        report += f"\n  ✓ {row['Metric']}: r={row['Corr_Median_Price']:.3f} (p={row['P_Median']:.4f})"
else:
    report += "\n  No significant predictors of median price"

report += f"\n\nPREDICTING PRICE VARIATION (CV):\n"

if len(sig_cv) > 0:
    for _, row in sig_cv.iterrows():
        report += f"\n  ✓ {row['Metric']}: r={row['Corr_CV']:.3f} (p={row['P_CV']:.4f})"
else:
    report += "\n  No significant predictors of price variation"

report += f"""

INTERPRETATION
─────────────────────────────────────────────────────────────────────────────
The analysis reveals the relationship between key demographic factors and 
healthcare pricing patterns in Florida cities.

Income appears to {"SIGNIFICANTLY" if len(sig_median) > 0 else "NOT"} correlate with pricing levels,
suggesting that affluence {"influences" if len(sig_median) > 0 else "does not influence"} healthcare costs.

Senior population {"appears" if "Pct_65Plus" in sig_median['Metric'].values or "Pct_65Plus" in sig_cv['Metric'].values else "does not appear"} to be associated with pricing patterns,
suggesting age composition {"matters" if "Pct_65Plus" in sig_median['Metric'].values or "Pct_65Plus" in sig_cv['Metric'].values else "doesn't matter"} for healthcare economics.

Insurance mix is {"a key" if len([m for m in sig_median['Metric'].values if 'Insurance' in m]) > 0 else "not a major"} factor in pricing,
with insurance coverage {"significantly" if len([m for m in sig_median['Metric'].values if 'Insurance' in m]) > 0 else "not"} affecting prices.

VISUALIZATIONS
─────────────────────────────────────────────────────────────────────────────
4 focused visualizations generated:

1. 13_key_variables_comparison.png
   → Box plots comparing income, age, insurance between groups

2. 14_key_variables_vs_cv_scatter.png
   → Scatter plots showing key variables vs price variation

3. 15_key_variables_correlation_heatmap.png
   → Correlation matrix of key variables

4. 16_mean_price_by_key_groups.png
   → Mean prices by income and insurance quartiles

NEXT STEPS
─────────────────────────────────────────────────────────────────────────────
1. Review the 4 visualizations
2. Focus discussion on significant relationships
3. Prepare for presentation with key findings
4. Consider drill-down analysis by treatment type or payer type

{'='*80}
Report Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*80}
"""

report_path = os.path.join(RESULTS_FOLDER, '15_focused_demographic_analysis_report.txt')
with open(report_path, 'w', encoding='utf-8') as f:
    f.write(report)

print(f"✓ Saved: 15_focused_demographic_analysis_report.txt")

print("\n" + "=" * 80)
print("✅ FOCUSED DEMOGRAPHIC ANALYSIS COMPLETE")
print("=" * 80)

print(f"\nOutput files saved:")
print(f"  Data Files (in Results folder):")
print(f"    • 5_city_demographic_analysis_focused.csv")
print(f"    • 13_key_variables_comparison_stats.csv")
print(f"    • 14_key_variables_correlations.csv")
print(f"\n  Visualizations (in Results/visualisation folder):")
print(f"    • visualisation/13_key_variables_comparison.png")
print(f"    • visualisation/14_key_variables_vs_cv_scatter.png")
print(f"    • visualisation/15_key_variables_correlation_heatmap.png")
print(f"    • visualisation/16_mean_price_by_key_groups.png")
print(f"\n  Report (in Results folder):")
print(f"    • 15_focused_demographic_analysis_report.txt")

print(f"\nKey Findings:")
print(f"  • Significant demographic predictors of median price: {len(sig_median)}")
print(f"  • Significant demographic predictors of price variation: {len(sig_cv)}")
print(f"  • Cities in analysis: {len(city_stats)}")

print("\n✅ Ready for presentation!\n")