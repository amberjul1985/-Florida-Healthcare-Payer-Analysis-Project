# Hospital Location and Payer Type Impact on Radiation Oncology Costs
## A Florida Market Analysis

**Course:** BANA 650: Healthcare Analytics  
**Institution:** California State University, Northridge (CSUN)  
**Team:** Julian Amberg, Robby Deffo, Ani Harutyunyan, Diana Manukyan

---

## 🎯 Executive Summary

This project investigates how **hospital location** and **payer type** influence price variation in radiation oncology procedures across Florida using hospital price transparency data. Our analysis reveals a **critical finding: payer type is a far more significant driver of price variation than geographic location.**

### Key Findings

| Metric | Result | Significance |
|--------|--------|--------------|
| **Price Variation by Payer** | Commercial: 756.3% CV vs Medicare: 149.3% CV | 5.1x difference |
| **Payer Effect** | Kruskal-Wallis: p < 0.001 | Highly significant |
| **Location Effect** | Kruskal-Wallis: p < 0.001 | Significant, but secondary to payer |
| **Demographic Factors** | Income, age, insurance mix correlations: r < 0.23, p > 0.31 | NOT significant predictors |
| **Extreme Outlier** | Tarpon Springs: 1133.8% CV | Single outlier, investigate separately |
| **Dataset** | 67,026 records | 38 Florida cities, 7 CPT codes, 4 payers |

### Main Insight

**Commercial insurance creates dramatic pricing variability while Medicare negotiates stable rates.** Demographic characteristics (income, age, population composition) do NOT statistically explain price variation—suggesting market structure and payer negotiation power, not community demographics, drive healthcare pricing disparities.

---

## 📊 Project Overview

### Research Question
How do **hospital location** and **payer classification** (Medicare, Medicaid, Private Insurance, Self-Pay) shape the pricing and price variation of radiation oncology procedures in Florida?

### Data Source
- **Provider:** Hospital Pricing Files (https://hospitalpricingfiles.org/)
- **State:** Florida
- **Format:** 246 hospital pricing CSV files (107 TALL format, 139 WIDE format)
- **Date Range:** 2024 pricing data
- **Processing:** ETL pipeline with PostgreSQL database

### Dataset Specifications

```
Total Records:              67,026
Unique Hospitals:           67 hospitals across 38 cities
Procedures (CPT Codes):     7 radiation oncology treatments
Payer Types:                4 (Medicare, Medicaid, Commercial, Self-Pay)
Data Quality:               100% (no missing values in key fields)
Outliers Flagged:           8,040 (12.0%) - retained for analysis
```

### Procedure Types (CPT Codes)

| Treatment Category | Records | Mean Price | Median | CV% |
|-------------------|---------|-----------|--------|-----|
| Clinical Treatment Planning | 8,131 | $1,863 | $1,050 | 503.5% |
| Medical Radiation Physics & Dosimetry | 22,265 | $1,170 | $581 | 732.6% |
| Stereotactic Radiation Treatment | 9,854 | $6,658 | $1,819 | 519.2% |
| Radiation Treatment Delivery | 9,297 | $604 | $364 | 351.4% |
| Clinical Brachytherapy | 14,794 | $2,006 | $941 | 617.9% |
| Radiation Treatment Management | 1,726 | $1,557 | $1,068 | 381.0% |
| Neutron Beam Treatment | 959 | $14,093 | $9,312 | 567.5% |

---

## 🏗️ Project Architecture

### Data Processing Pipeline

```
RAW DATA SOURCES
  ├─ 107 TALL-format CSV files (pre-normalized)
  └─ 139 WIDE-format CSV files (require transformation)
           ↓
    [tall_vs_wide.py] ← File format validation & counting
           ↓
[2_wide_to_tall_to_pg.py] ← ETL: Format transformation, deduplication, DB loading
           ↓
    PostgreSQL Database (pricing.charges table)
           ↓
[3_refined_data.py] ← CPT code preparation, payer classification, city cleaning
           ↓
    Intermediate Dataset (3_filtered_cpt_codes_with_payer_group.csv)
           ↓
[4_final_data_cleaning.py] ← Data validation, imputation (Method 1), feature engineering
           ↓
    Analysis-Ready Dataset (4_cleaned_prices.csv, 56,107 records)
           ↓
    ├─ [5_analysis_and_visual.py] ← Statistical summaries & PNG visualizations
    ├─ [6_geospatial_visual.py] ← Interactive HTML maps by geography
    └─ [7_demographic_analysis.py] ← Demographic correlation analysis
           ↓
    OUTPUT FILES (24 visualizations + 10 summary tables)
```

### Hybrid Folder Structure

```
Results/
├─ geospatial/                 ← 11 interactive HTML maps
│  ├─ 01_Map_Overall_Price_Landscape.html
│  ├─ 02_Map_Price_Variation_Hotspots.html
│  ├─ 03_Map_CPT_*.html (7 maps, one per procedure)
│  ├─ 04_Map_Payer_Disparities.html
│  └─ 05_Map_Data_Coverage.html
│
├─ visualisation/              ← 13 PNG statistical visualizations
│  ├─ 01_Viz_Price_Distribution_by_Payer.png
│  ├─ 02_Viz_Mean_Price_by_CPT.png
│  ├─ 03_Viz_CV_by_Payer.png
│  ├─ 04_Viz_Heatmap_CPT_Payer_Prices.png
│  ├─ 05_Viz_Top_Cities_by_Variation.png
│  └─ (8 additional demographic & correlation visualizations)
│
├─ 4_cleaned_prices.csv        ← Analysis-ready dataset (56,107 records)
├─ 01_Summary_by_CPT_Code.csv
├─ 02_Summary_by_Payer_Type.csv
├─ 03_Summary_by_CPT_and_Payer.csv
├─ 04_Summary_by_Location.csv
├─ 5_city_demographic_analysis_focused.csv
├─ DATA_SUMMARY_REPORT.txt
├─ 15_focused_demographic_analysis_report.txt
└─ (other report files)
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL 12+ (for database operations)
- Libraries: `pandas`, `numpy`, `scipy`, `matplotlib`, `seaborn`, `folium`, `sqlalchemy`, `psycopg2`

### Installation

```bash
# Clone repository
git clone <your-repo-url>
cd BANA_650_Healthcare

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Test database connection
python src/test_db.py
```

### Running the Analysis

```bash
# Step 1: Validate file formats (before processing)
python src/tall_vs_wide.py

# Step 2: ETL pipeline (wide → tall format, database loading)
python src/2_wide_to_tall_to_pg.py

# Step 3: Data preparation (CPT codes, payer classification)
python src/3_refined_data.py

# Step 4: Final cleaning & imputation
python src/4_final_data_cleaning.py

# Step 5: Statistical analysis & visualizations
python src/5_analysis_and_visual.py

# Step 6: Geospatial maps
python src/6_geospatial_visual.py

# Step 7: Demographic analysis
python src/7_demographic_analysis.py
```

---

## 📁 Source Code

All scripts include comprehensive documentation and error handling. Windows-compatible with centralized configuration.

### Main Analysis Scripts

| Script | Purpose | Inputs | Outputs |
|--------|---------|--------|---------|
| `2_wide_to_tall_to_pg.py` | ETL: Format conversion + DB load | Raw CSV files | `pricing.charges` table |
| `3_refined_data.py` | CPT prep, payer classification | `pricing.charges` | `3_filtered_cpt_codes_with_payer_group.csv` |
| `4_final_data_cleaning.py` | Data validation, imputation, cleaning | Filtered data | `4_cleaned_prices.csv` (56K records) |
| `5_analysis_and_visual.py` | Summary stats & visualizations | Cleaned data | 5 PNG charts + 4 CSV tables |
| `6_geospatial_visual.py` | Interactive geographic maps | Cleaned data | 11 HTML maps |
| `7_demographic_analysis.py` | Demographics × pricing correlation | Census data | 4 visualizations + analysis |

### Utility Scripts

| Script | Purpose |
|--------|---------|
| `tall_vs_wide.py` | **File Format Validator** - Counts TALL vs WIDE format files in data directory (reads without modifying) |
| `test_db.py` | **Connection Checker** - Verifies PostgreSQL connection before running ETL pipeline |

---

## 📈 Key Visualizations

### Price Variation Analysis (Payer Type)

**Figure 1:** Coefficient of Variation by Payer Type
- Commercial Insurance: 756.3% (highest variability)
- Self-Pay/Other: 329.1%
- Medicaid: 227.8%
- Medicare: 149.3% (most stable)

### Geographic Hot Spots

**Figure 2:** Top 10 Cities by Price Variation
- Tarpon Springs: 1,133.8% (extreme outlier)
- Daytona Beach: 336.2%
- Port Charlotte: 310.9%
- Other cities: 244-285% range

### Demographic Analysis

**Figure 3:** Income & Insurance Effects on Variation
- Income ($100K+): r = 0.226, p = 0.312 (NOT significant)
- Senior Population (65+): r = 0.192, p = 0.392 (NOT significant)
- Private Insurance: r = 0.173, p = 0.442 (NOT significant)
- **Conclusion:** Demographics do NOT explain price variation

---

## 📊 Database Architecture

This project uses PostgreSQL for scalable data management:

```sql
-- Main table: All hospital pricing data
pricing.charges (500K-1M rows)
  ├─ Hospital info (name, address, city, CCN)
  ├─ CPT codes (up to 6 slots per record)
  ├─ Payer details (name, plan)
  └─ Pricing (estimated, negotiated %, negotiated $)

-- Analysis table: Radiology procedures only (CPT 70010-79999)
pricing.charges_radiology (56K rows)
  └─ Filtered from charges for faster analysis

-- Tracking table: ETL operation log
pricing.ingestion_log
  └─ Prevents duplicate data loading
```

**For detailed database documentation, see:** [docs/DATABASE.md](docs/DATABASE.md)

---

## 📝 Detailed Findings

### Statistical Significance Testing

**Kruskal-Wallis H-Tests** (non-parametric ANOVA):
- **Payer Type Effect:** H = 6,786.29, p < 0.001 ✓ Highly significant
- **Location Effect:** H = 6,750.92, p < 0.001 ✓ Highly significant
- Both factors matter, but **payer type shows greater price discrimination**

### Main Findings by Category

**Payer Type (Primary Finding):**
- Commercial payers pay 5x more variation than Medicare
- Private insurance: median $934, mean $3,011 (huge outlier impact)
- Medicare: median $694, mean $1,231 (stable, negotiated)
- Medicaid: median $127, mean $906 (lower volume, limited data)

**Procedure Type (Secondary Finding):**
- Neutron beam treatment: highest cost ($14,093 mean), extremely rare (n=959)
- Stereotactic radiation: $6,658 mean, specialized procedure
- Standard treatments: $600-$2,000 range (routine procedures)

**Geographic Variation:**
- Tarpon Springs: 1,133.8% CV (investigate data quality)
- Urban centers: 200-300% CV range
- Pricing varies significantly across all Florida markets

**Demographic Factors (Null Finding):**
- Income level does NOT correlate with price variation (r=0.226, p=0.31)
- Senior population (65+) does NOT correlate with variation (r=0.192, p=0.39)
- Insurance mix does NOT predict variation
- **Interpretation:** Market structure matters more than community demographics

---

## 📄 Output Files

### Summary Data Tables (CSV)
- `01_Summary_by_CPT_Code.csv` - Price stats by procedure type
- `02_Summary_by_Payer_Type.csv` - Price stats by payer
- `03_Summary_by_CPT_and_Payer.csv` - Cross-tabulation (7 procedures × 4 payers)
- `04_Summary_by_Location.csv` - City-level statistics (38 cities)
- `5_city_demographic_analysis_focused.csv` - Demographics + pricing by city

### Visualizations
- **Payer Analysis:** 4 PNG files showing distribution, mean, CV, heatmaps
- **Geographic Analysis:** 11 interactive HTML maps (pan/zoom enabled)
- **Demographic Analysis:** 4 PNG visualizations (box plots, scatter, correlation, quartiles)

### Reports
- `DATA_SUMMARY_REPORT.txt` - Milestone D summary with all statistics
- `15_focused_demographic_analysis_report.txt` - Demographic findings
- `BANA_650_Final_Report.pdf` - **Comprehensive 8-page final report** (see DOCS/)

---

## 🔍 How to Interpret Results

### Reading the Visualizations

**Interactive Maps:**
- Red circles = High prices or high variation (market concern)
- Green circles = Low variation, stable pricing (efficient market)
- Circle size = Market volume (larger = more data points)
- Hover/click for exact city statistics

**Statistical Charts:**
- **CV% > 500%** = Extreme price variation (market inefficiency)
- **CV% 200-500%** = High variation (common in healthcare)
- **CV% < 200%** = Moderate variation (stable market)

**Payer Disparities:**
- **Ratio 1.0x** = Payers negotiate same price (competitive)
- **Ratio 2-3x** = Commercial pays 2-3× more (typical)
- **Ratio 5x+** = Severe disparity (our finding for commercial vs Medicare)

---

## 📚 Documentation

### In This Repository

- **[docs/DATABASE.md](docs/DATABASE.md)** - Complete database schema, queries, troubleshooting
- **[docs/database_setup.sql](docs/database_setup.sql)** - PostgreSQL DDL statements
- **[BANA_650_Final_Report.pdf](docs/BANA_650_Final_Report.pdf)** - Comprehensive analysis report

### Data Source

- **Hospital Pricing Files:** https://hospitalpricingfiles.org/
- **CPT Code Reference:** https://www.aapc.com/cpt-codes/
- **Healthcare Pricing Literature:** See Final Report bibliography

---

## ✅ Technical Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.8+ |
| Data Processing | Pandas, NumPy |
| Statistics | SciPy (Kruskal-Wallis, correlation) |
| Visualization | Matplotlib, Seaborn, Folium |
| Database | PostgreSQL 12+ |
| ORM | SQLAlchemy |
| Version Control | Git/GitHub |
| Development | VS Code, Windows/macOS |

---

## Collaboration

**BANA 650 Healthcare Analytics - CSUN**

**Team Leads:**
- Robby Deffo
- Ani Harutyunyan
- Diana Manukyan
- Julian Amberg

**Instructor:**
- Dr. Akash Gupta (CSUN BANA 650)

**Academic Institution:**
- California State University, Northridge
- Department: Business Administration
- Program: Master of Science in Business Analytics

---

## 📋 Version History

| Version | Date | Status | Key Updates |
|---------|------|--------|------------|
| 1.0 | Nov 17, 2024 | ✅ Complete | Milestones 1-D submitted |
| 1.1 | Dec 7, 2024 | ✅ Complete | Milestone E (geospatial + demographic analysis) |
| 2.0 | Dec 11, 2024 | 🔄 In Progress | Final Report (8 pages) |

---

## Questions & Support

For detailed analysis and interpretation of findings, refer to:
- **Statistical Details:** See [docs/DATA_SUMMARY_REPORT.txt](Results/DATA_SUMMARY_REPORT.txt)
- **Demographic Analysis:** See [docs/15_focused_demographic_analysis_report.txt](Results/15_focused_demographic_analysis_report.txt)
- **Complete Analysis:** See **BANA_650_Final_Report.pdf** in DOCS/ folder

---

**Last Updated:** December 2024  
**Project Status:** ✅ Milestones 1-E Complete | Final Report In Progress  
**Repository:** [Your GitHub URL]

---

*This README provides an overview of the BANA 650 Healthcare Analytics project. For comprehensive analysis, methodology, and detailed findings, please refer to the full 8-page Final Report in the DOCS folder.*
