"""
Egypt Air Traffic Surveillance - Analytics Dashboard
----------------------------------------------------
Description: 
    This script connects to the Data Warehouse (PostgreSQL), executes analytical SQL queries, 
    and generates a high-resolution visual dashboard (PNG) summarizing traffic insights.

Key Metrics Visualized:
    1. Market Share (Top 10 Airlines)
    2. Traffic Origins (Top Airports)
    3. Flight Velocity Distribution

Author: Abdelrhman
Project: End-to-End Data Engineering Portfolio
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import datetime

# ==========================================
# 1. Configuration & Setup
# ==========================================
# Set a dark theme for a "Radar-like" aesthetic
plt.style.use('dark_background')
sns.set_palette("bright")

# Database Connection (Localhost for running script outside Docker)
# Note: Ensure 'my_postgres' container is running on port 5432
DB_CONNECTION_STR = 'postgresql://admin:admin@localhost:5432/postgres'
engine = create_engine(DB_CONNECTION_STR)

print("🚀 Connecting to Data Warehouse and fetching analytics...")

# ==========================================
# 2. Data Extraction (SQL -> Pandas)
# ==========================================

# Query A: Top 10 Airlines by Flight Volume
q_airlines = """
SELECT 
    COALESCE(ac.full_name, t.airline) as name, 
    COUNT(*) as total 
FROM egypt_sky_traffic t
LEFT JOIN airline_codes ac ON t.airline = ac.code
WHERE t.airline IS NOT NULL
GROUP BY name 
ORDER BY total DESC 
LIMIT 10;
"""
df_airlines = pd.read_sql(q_airlines, engine)

# Query B: Top Origins (Airports)
q_airports = """
SELECT 
    origin_airport, 
    COUNT(DISTINCT icao24) as total
FROM egypt_sky_traffic
WHERE origin_airport != 'Unknown'
GROUP BY origin_airport
ORDER BY total DESC
LIMIT 10;
"""
df_airports = pd.read_sql(q_airports, engine)

# Query C: Velocity Distribution (Filtering outliers < 100 km/h)
q_speed = "SELECT velocity_kmh FROM egypt_sky_traffic WHERE velocity_kmh > 100"
df_speed = pd.read_sql(q_speed, engine)

# ==========================================
# 3. Visualization (Matplotlib & Seaborn)
# ==========================================
print("🎨 Rendering High-Res Dashboard...")

# Initialize the main figure layout (2000x1200 px approx)
fig = plt.figure(figsize=(20, 12))
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
fig.suptitle(
    f"Egypt Air Traffic Surveillance Report\nGenerated on: {current_time}", 
    fontsize=24, 
    color="#dcdfdf", 
    fontweight='bold'
)

# --- Chart 1: Market Share (Bar Chart) ---
ax1 = fig.add_subplot(2, 2, 1) # Top-Left
sns.barplot(
    ax=ax1, x='total', y='name', data=df_airlines, 
    palette='viridis', edgecolor='white'
)
ax1.set_title('🏆 Top 10 Airlines (Most Frequent)', fontsize=16, color='gold')
ax1.set_xlabel('Number of Flights')
ax1.set_ylabel('')
ax1.grid(color='gray', linestyle=':', linewidth=0.5, alpha=0.5)

# --- Chart 2: Top Origins (Donut Chart) ---
ax2 = fig.add_subplot(2, 2, 2) # Top-Right

# Prepare Data: Aggregate airports outside top 5 into 'Others'
top_5_airports = df_airports.head(5)
others_count = df_airports.iloc[5:]['total'].sum()
if others_count > 0:
    new_row = pd.DataFrame({'origin_airport': ['Others'], 'total': [others_count]})
    top_5_airports = pd.concat([top_5_airports, new_row], ignore_index=True)

colors = sns.color_palette('pastel')[0:len(top_5_airports)]
wedges, texts, autotexts = ax2.pie(
    top_5_airports['total'], 
    labels=top_5_airports['origin_airport'], 
    autopct='%1.1f%%', 
    startangle=90, 
    colors=colors, 
    pctdistance=0.85
)

# Draw circle for Donut effect
centre_circle = plt.Circle((0,0), 0.70, fc='#000000')
ax2.add_artist(centre_circle)
ax2.set_title('🌍 Top Flight Origins', fontsize=16, color='cyan')
plt.setp(texts, color='white', fontweight='bold')

# --- Chart 3: Speed Distribution (Histogram + KDE) ---
ax3 = fig.add_subplot(2, 1, 2) # Bottom (Full Width)
sns.histplot(
    data=df_speed, x='velocity_kmh', bins=50, kde=True, 
    color='#ff0055', ax=ax3, fill=True, alpha=0.6
)
ax3.set_title('🚀 Aircraft Speed Distribution (km/h)', fontsize=16, color='#ff0055')
ax3.set_xlabel('Speed (km/h)')
ax3.grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.3)

# ==========================================
# 4. Export
# ==========================================
plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust layout to fit title
filename = 'egypt_radar_dashboard.png'
plt.savefig(filename, dpi=300, facecolor='black') # High DPI for Portfolio
print(f"✅ Dashboard saved successfully: {filename}")

# Optional: Show the plot window
# plt.show()