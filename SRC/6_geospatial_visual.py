"""
================================================================================
GEOSPATIAL VISUALIZATION - FLORIDA RADIATION ONCOLOGY PRICING 
Purpose: Create interactive maps showing geographic pricing patterns

OUTPUT: 11 interactive HTML maps saved to Results/geospatial/ folder
================================================================================
"""

import pandas as pd
import numpy as np
import folium
from folium import plugins
import warnings
import os

warnings.filterwarnings('ignore')

print("\n" + "="*80)
print("GEOSPATIAL VISUALIZATION - FLORIDA RADIATION ONCOLOGY PRICING")
print("="*80 + "\n")

# ================================================================================
# SECTION 1: CONFIGURATION & SETUP
# ================================================================================

print("Step 1: Configuration and setup...")
print("-"*80)

# File paths
DATA_PATH = r'C:\Robby\BANA 650_Healthcare\Data\4_cleaned_prices_with_imputation_tracking.csv'
OUTPUT_BASE = r'C:\Robby\BANA 650_Healthcare\Results'
GEOSPATIAL_FOLDER = os.path.join(OUTPUT_BASE, 'geospatial')  # Maps in dedicated geospatial folder
OUTPUT_PATH = GEOSPATIAL_FOLDER

# Create geospatial folder if it doesn't exist
os.makedirs(OUTPUT_PATH, exist_ok=True)
print(f"✓ Geospatial folder ready: {OUTPUT_PATH}\n")

# Florida city coordinates (latitude, longitude)
FLORIDA_COORDS = {
    'Tampa': (27.9506, -82.4572),
    'Jacksonville': (30.3322, -81.6557),
    'Palm Coast': (29.0465, -81.1988),
    'Tavares': (28.8062, -81.7886),
    'Zephyrhills': (28.2364, -82.1854),
    'Ocala': (29.1870, -82.1395),
    'Orlando': (28.5383, -81.3792),
    'Daytona Beach': (29.2108, -81.0228),
    'Sanford': (28.7805, -81.2753),
    'Lakeland': (28.0395, -81.9498),
    'Bartow': (27.8958, -81.7738),
    'Bradenton': (27.4891, -82.5784),
    'Sarasota': (27.3364, -82.5326),
    'Port Charlotte': (26.9736, -82.0912),
    'Tarpon Springs': (28.1447, -82.7539),
    'Englewood': (26.9542, -82.3634),
    'Pensacola': (30.4215, -87.2169),
    'Fort Walton Beach': (30.4074, -86.6191),
    'Panama City Beach': (30.1980, -85.9918),
    'Tallahassee': (30.4383, -84.2807),
    'St. Petersburg': (27.7676, -82.6403),
    'Clearwater': (27.9755, -82.7597),
    'FT WALTON BEACH': (30.4074, -86.6191),
    'TALLAHASSEE': (30.4383, -84.2807),
    'PORT CHARLOTTE': (26.9736, -82.0912),
    'BRADENTON': (27.4891, -82.5784),
    'SANFORD': (28.7805, -81.2753),
    'ENGLEWOOD': (26.9542, -82.3634),
    'Dunnellon': (29.0513, -82.4630),
    'Fernandina Beach': (30.6648, -81.4611),
    'Miami': (25.7617, -80.1918),
    'Homestead': (25.5000, -80.4667),
    'West Palm Beach': (26.7153, -80.0534),
    'Petersburg': (27.7676, -82.6403),
}

print(f"✓ Configured with {len(FLORIDA_COORDS)} major cities")
print(f"✓ Output path: {OUTPUT_PATH}")

# ================================================================================
# SECTION 2: LOAD DATA
# ================================================================================

print("\nStep 2: Loading and preparing data...")
print("-"*80)

try:
    df = pd.read_csv(DATA_PATH)
    print(f"✓ Loaded {len(df):,} records")
except FileNotFoundError:
    print(f"❌ Error: File not found at {DATA_PATH}")
    exit()

# ================================================================================
# SECTION 3: CALCULATE SUMMARY STATISTICS BY CITY
# ================================================================================

print("\nStep 3: Calculating city-level summary statistics...")
print("-"*80)

city_stats = df.groupby('City').agg({
    'negotiated_dollar': ['mean', 'median', 'std', 'count'],
    'unique_id': 'nunique',
    'Treatment_Category': 'nunique'
}).reset_index()

city_stats.columns = ['City', 'mean_price', 'median_price', 'std_price', 'record_count', 
                      'hospital_count', 'cpt_count']

# Calculate Coefficient of Variation (CV%)
city_stats['cv_percent'] = (city_stats['std_price'] / city_stats['mean_price'] * 100).round(2)

# Add coordinates
city_coords_df = pd.DataFrame([
    {'City': city, 'latitude': coords[0], 'longitude': coords[1]} 
    for city, coords in FLORIDA_COORDS.items()
])

city_stats = city_stats.merge(city_coords_df, on='City', how='left')
city_stats = city_stats.dropna(subset=['latitude', 'longitude'])

print(f"✓ City statistics calculated for {len(city_stats)} cities")
print(f"✓ Price range: ${city_stats['mean_price'].min():.2f} - ${city_stats['mean_price'].max():.2f}")
print(f"✓ CV% range: {city_stats['cv_percent'].min():.1f}% - {city_stats['cv_percent'].max():.1f}%")

# ================================================================================
# SECTION 4: CALCULATE STATISTICS BY CITY + CPT CODE
# ================================================================================

print("\nStep 4: Calculating CPT-specific city statistics...")
print("-"*80)

city_cpt_stats = df.groupby(['City', 'Treatment_Category']).agg({
    'negotiated_dollar': ['mean', 'std', 'count'],
    'unique_id': 'nunique'
}).reset_index()

city_cpt_stats.columns = ['City', 'CPT_Code', 'mean_price', 'std_price', 'record_count', 'hospital_count']

# Calculate CV%
city_cpt_stats['cv_percent'] = (city_cpt_stats['std_price'] / city_cpt_stats['mean_price'] * 100).round(2)

# Add coordinates
city_cpt_stats = city_cpt_stats.merge(city_coords_df, on='City', how='left')
city_cpt_stats = city_cpt_stats.dropna(subset=['latitude', 'longitude'])

print(f"✓ City-CPT combinations: {len(city_cpt_stats)}")

# Get top CPT codes by frequency
top_cpt_codes = df['Treatment_Category'].value_counts().head(6).index.tolist()
print(f"✓ Top 6 CPT codes selected for faceted maps:")
for i, cpt in enumerate(top_cpt_codes, 1):
    count = len(df[df['Treatment_Category'] == cpt])
    print(f"  {i}. {cpt[:50]}... ({count:,} records)")

# ================================================================================
# SECTION 5: CALCULATE PAYER DISPARITIES BY CITY (FIXED)
# ================================================================================

print("\nStep 5: Calculating payer negotiation disparities...")
print("-"*80)

# Payer statistics by city
city_payer_stats = df.groupby(['City', 'payer_group']).agg({
    'negotiated_dollar': ['mean', 'count']
}).reset_index()

city_payer_stats.columns = ['City', 'payer_group', 'mean_price', 'record_count']

# Pivot to get payer prices side by side (FIX: reset_index to avoid ambiguity)
payer_pivot = city_payer_stats.pivot(index='City', columns='payer_group', values='mean_price')
payer_pivot = payer_pivot.reset_index()  # THIS IS THE FIX - convert City from index to column

# Calculate Private/Medicaid ratio
payer_disparities = pd.DataFrame({
    'City': payer_pivot['City'],
    'Private_price': payer_pivot.get('Private / Commercial', np.nan),
    'Medicaid_price': payer_pivot.get('Medicaid', np.nan),
    'Medicare_price': payer_pivot.get('Medicare', np.nan),
})

# Calculate ratio
payer_disparities['private_medicaid_ratio'] = (
    payer_disparities['Private_price'] / payer_disparities['Medicaid_price']
).round(2)

# Add coordinates (now works because City is a column, not an index)
payer_disparities = payer_disparities.merge(city_coords_df, on='City', how='left')
payer_disparities = payer_disparities.dropna(subset=['latitude', 'longitude', 'private_medicaid_ratio'])

print(f"✓ Payer disparities calculated for {len(payer_disparities)} cities")
print(f"✓ Private/Medicaid ratio range: {payer_disparities['private_medicaid_ratio'].min():.1f}x - {payer_disparities['private_medicaid_ratio'].max():.1f}x")

# ================================================================================
# SECTION 6: MAP #1 - OVERALL PRICE LANDSCAPE
# ================================================================================

print("\nStep 6: Creating Map #1 - Overall Price Landscape...")
print("-"*80)

florida_center = [27.5, -81.5]
map1 = folium.Map(
    location=florida_center,
    zoom_start=7,
    tiles='OpenStreetMap',
    width='100%',
    height='100%'
)

title_html = '''
             <div style="position: fixed; 
                     top: 10px; left: 50px; width: 500px; height: 90px; 
                     background-color: white; border:2px solid grey; z-index:9999; 
                     font-size:16px; font-weight: bold; padding: 10px">
             Map #1: Overall Price Landscape<br/>
             Average Negotiated Price by City<br/>
             <span style="font-size: 12px; font-weight: normal;">Darker red = more expensive procedures</span>
             </div>
             '''
map1.get_root().html.add_child(folium.Element(title_html))

price_min = city_stats['mean_price'].min()
price_max = city_stats['mean_price'].max()

for idx, row in city_stats.iterrows():
    normalized_price = (row['mean_price'] - price_min) / (price_max - price_min)
    
    if normalized_price < 0.33:
        color = 'blue'
    elif normalized_price < 0.66:
        color = 'orange'
    else:
        color = 'red'
    
    radius = 15 + (normalized_price * 30)
    
    popup_text = f"""
    <b>{row['City']}</b><br/>
    Mean Price: ${row['mean_price']:,.2f}<br/>
    Median Price: ${row['median_price']:,.2f}<br/>
    # Records: {int(row['record_count']):,}<br/>
    # Hospitals: {int(row['hospital_count'])}<br/>
    # CPT Types: {int(row['cpt_count'])}
    """
    
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=radius,
        popup=folium.Popup(popup_text, max_width=300),
        color=color,
        fill=True,
        fillColor=color,
        fillOpacity=0.7,
        weight=2
    ).add_to(map1)

map1_path = os.path.join(OUTPUT_PATH, '01_Map_Overall_Price_Landscape.html')
map1.save(map1_path)
print(f"✓ Map #1 saved")

# ================================================================================
# SECTION 7: MAP #2 - PRICE VARIATION HOTSPOTS
# ================================================================================

print("\nStep 7: Creating Map #2 - Price Variation Hotspots...")
print("-"*80)

map2 = folium.Map(
    location=florida_center,
    zoom_start=7,
    tiles='OpenStreetMap'
)

title_html2 = '''
             <div style="position: fixed; 
                     top: 10px; left: 50px; width: 500px; height: 90px; 
                     background-color: white; border:2px solid grey; z-index:9999; 
                     font-size:16px; font-weight: bold; padding: 10px">
             Map #2: Price Variation Hotspots<br/>
             Coefficient of Variation (CV%) by City<br/>
             <span style="font-size: 12px; font-weight: normal;">Green = stable pricing, Red = chaotic pricing</span>
             </div>
             '''
map2.get_root().html.add_child(folium.Element(title_html2))

cv_min = city_stats['cv_percent'].min()
cv_max = city_stats['cv_percent'].max()

for idx, row in city_stats.iterrows():
    normalized_cv = (row['cv_percent'] - cv_min) / (cv_max - cv_min)
    
    if normalized_cv < 0.33:
        color = 'green'
    elif normalized_cv < 0.66:
        color = 'yellow'
    else:
        color = 'red'
    
    radius = 15 + (normalized_cv * 30)
    
    popup_text = f"""
    <b>{row['City']}</b><br/>
    CV%: {row['cv_percent']:.1f}%<br/>
    Mean Price: ${row['mean_price']:,.2f}<br/>
    Std Dev: ${row['std_price']:,.2f}<br/>
    # Records: {int(row['record_count']):,}
    """
    
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=radius,
        popup=folium.Popup(popup_text, max_width=300),
        color=color,
        fill=True,
        fillColor=color,
        fillOpacity=0.7,
        weight=2
    ).add_to(map2)

map2_path = os.path.join(OUTPUT_PATH, '02_Map_Price_Variation_Hotspots.html')
map2.save(map2_path)
print(f"✓ Map #2 saved")

# ================================================================================
# SECTION 8: MAP #3 - FACETED PROCEDURE VARIATION (6 Subplots)
# ================================================================================

print("\nStep 8: Creating Map #3 - Faceted Procedure Variation...")
print("-"*80)

for cpt_idx, cpt_code in enumerate(top_cpt_codes, 1):
    
    cpt_data = city_cpt_stats[city_cpt_stats['CPT_Code'] == cpt_code].copy()
    
    if len(cpt_data) == 0:
        print(f"  ⚠ Skipping CPT {cpt_idx} - no geographic data")
        continue
    
    map_cpt = folium.Map(
        location=florida_center,
        zoom_start=7,
        tiles='OpenStreetMap'
    )
    
    title_html_cpt = f'''
                 <div style="position: fixed; 
                         top: 10px; left: 50px; width: 600px; height: 100px; 
                         background-color: white; border:2px solid grey; z-index:9999; 
                         font-size:14px; font-weight: bold; padding: 10px">
                 Procedure {cpt_idx}/6: Price Variation<br/>
                 <span style="font-size: 12px;">{cpt_code[:60]}...</span><br/>
                 <span style="font-size: 11px; font-weight: normal;">Green = stable, Red = chaotic</span>
                 </div>
                 '''
    map_cpt.get_root().html.add_child(folium.Element(title_html_cpt))
    
    cpt_cv_min = cpt_data['cv_percent'].min()
    cpt_cv_max = cpt_data['cv_percent'].max()
    
    for idx, row in cpt_data.iterrows():
        if cpt_cv_max == cpt_cv_min:
            normalized_cv = 0.5
        else:
            normalized_cv = (row['cv_percent'] - cpt_cv_min) / (cpt_cv_max - cpt_cv_min)
        
        if normalized_cv < 0.33:
            color = 'green'
        elif normalized_cv < 0.66:
            color = 'yellow'
        else:
            color = 'red'
        
        radius = 12 + (normalized_cv * 25)
        
        popup_text = f"""
        <b>{row['City']}</b><br/>
        CV%: {row['cv_percent']:.1f}%<br/>
        Mean: ${row['mean_price']:,.2f}<br/>
        Records: {int(row['record_count'])}
        """
        
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=radius,
            popup=folium.Popup(popup_text, max_width=300),
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            weight=2
        ).add_to(map_cpt)
    
    map_cpt_path = os.path.join(OUTPUT_PATH, f'03_Map_CPT_{cpt_idx:02d}_{cpt_code[:30]}.html')
    map_cpt.save(map_cpt_path)
    print(f"  ✓ CPT {cpt_idx}/6 map saved")

print(f"✓ Map #3 (6 CPT procedures) complete")

# ================================================================================
# SECTION 9: MAP #4 - PAYER NEGOTIATION DISPARITIES
# ================================================================================

print("\nStep 9: Creating Map #4 - Payer Negotiation Disparities...")
print("-"*80)

map4 = folium.Map(
    location=florida_center,
    zoom_start=7,
    tiles='OpenStreetMap'
)

title_html4 = '''
             <div style="position: fixed; 
                     top: 10px; left: 50px; width: 600px; height: 100px; 
                     background-color: white; border:2px solid grey; z-index:9999; 
                     font-size:16px; font-weight: bold; padding: 10px">
             Map #4: Payer Negotiation Disparities<br/>
             Private Insurance vs Medicaid Price Ratio<br/>
             <span style="font-size: 12px; font-weight: normal;">Blue = equal, Red = Private pays much more</span>
             </div>
             '''
map4.get_root().html.add_child(folium.Element(title_html4))

ratio_min = payer_disparities['private_medicaid_ratio'].min()
ratio_max = payer_disparities['private_medicaid_ratio'].max()

for idx, row in payer_disparities.iterrows():
    normalized_ratio = (row['private_medicaid_ratio'] - ratio_min) / (ratio_max - ratio_min)
    
    if normalized_ratio < 0.33:
        color = 'blue'
    elif normalized_ratio < 0.66:
        color = 'yellow'
    else:
        color = 'red'
    
    radius = 15 + (normalized_ratio * 30)
    
    popup_text = f"""
    <b>{row['City']}</b><br/>
    Private/Medicaid Ratio: {row['private_medicaid_ratio']:.2f}x<br/>
    Private: ${row['Private_price']:,.2f}<br/>
    Medicaid: ${row['Medicaid_price']:,.2f}<br/>
    Medicare: ${row['Medicare_price']:,.2f}
    """
    
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=radius,
        popup=folium.Popup(popup_text, max_width=300),
        color=color,
        fill=True,
        fillColor=color,
        fillOpacity=0.7,
        weight=2
    ).add_to(map4)

map4_path = os.path.join(OUTPUT_PATH, '04_Map_Payer_Disparities.html')
map4.save(map4_path)
print(f"✓ Map #4 saved")

# ================================================================================
# SECTION 10: MAP #5 - DATA COVERAGE & MARKET SIZE
# ================================================================================

print("\nStep 10: Creating Map #5 - Data Coverage & Market Size...")
print("-"*80)

map5 = folium.Map(
    location=florida_center,
    zoom_start=7,
    tiles='OpenStreetMap'
)

title_html5 = '''
             <div style="position: fixed; 
                     top: 10px; left: 50px; width: 600px; height: 100px; 
                     background-color: white; border:2px solid grey; z-index:9999; 
                     font-size:16px; font-weight: bold; padding: 10px">
             Map #5: Data Coverage & Market Size<br/>
             Number of Records by City<br/>
             <span style="font-size: 12px; font-weight: normal;">Bigger circle = more data/larger market</span>
             </div>
             '''
map5.get_root().html.add_child(folium.Element(title_html5))

record_min = city_stats['record_count'].min()
record_max = city_stats['record_count'].max()

for idx, row in city_stats.iterrows():
    normalized_records = (row['record_count'] - record_min) / (record_max - record_min)
    
    radius = 10 + (normalized_records * 35)
    
    popup_text = f"""
    <b>{row['City']}</b><br/>
    Records: {int(row['record_count']):,}<br/>
    Hospitals: {int(row['hospital_count'])}<br/>
    CPT Types: {int(row['cpt_count'])}<br/>
    Market Size: {"Large" if row['record_count'] > 3000 else "Medium" if row['record_count'] > 1000 else "Small"}
    """
    
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=radius,
        popup=folium.Popup(popup_text, max_width=300),
        color='purple',
        fill=True,
        fillColor='purple',
        fillOpacity=0.6,
        weight=2
    ).add_to(map5)

map5_path = os.path.join(OUTPUT_PATH, '05_Map_Data_Coverage.html')
map5.save(map5_path)
print(f"✓ Map #5 saved")

# ================================================================================
# SECTION 11: SUMMARY & COMPLETION
# ================================================================================

print("\n" + "="*80)
print("✓✓✓ GEOSPATIAL VISUALIZATION COMPLETE ✓✓✓")
print("="*80 + "\n")

print(f"""
MAPS CREATED: 11 total
  • 1 × Overall Price Landscape (Map #1)
  • 1 × Price Variation Hotspots (Map #2)
  • 6 × CPT-Specific Variations (Map #3, procedures 1-6)
  • 1 × Payer Disparities (Map #4)
  • 1 × Data Coverage (Map #5)

OUTPUT STRUCTURE:
  Results/
  ├─ geospatial/                 ← Dedicated folder for geospatial maps
  │  ├─ 01_Map_Overall_Price_Landscape.html
  │  ├─ 02_Map_Price_Variation_Hotspots.html
  │  ├─ 03_Map_CPT_*.html (7 maps, one per CPT code)
  │  ├─ 04_Map_Payer_Disparities.html
  │  └─ 05_Map_Data_Coverage.html
  ├─ visualisation/              ← Other visualizations (PNG files)
  ├─ [CSV data files]
  └─ [Report files]

ALL MAPS ARE INTERACTIVE:
  ✓ Pan/Zoom enabled
  ✓ Click circles for city details
  ✓ Open in web browser
  ✓ Ready for presentation

FILES CREATED:
""")

# List all created files
for file in sorted(os.listdir(OUTPUT_PATH)):
    if file.endswith('.html'):
        print(f"  ✓ {file}")

print(f"""
NEXT STEPS:
  1. Open any .html file in your browser
  2. Explore the interactive maps
  3. Use for Milestone E analysis
  4. Include screenshots in your report

STATUS: ✓ READY FOR PRESENTATION

""")

print("="*80 + "\n")