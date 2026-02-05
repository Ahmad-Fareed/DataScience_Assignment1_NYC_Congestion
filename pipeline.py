# pipeline.py

# ---------- Imports ----------
# requests downloads web pages
import requests

# BeautifulSoup parses HTML content
from bs4 import BeautifulSoup

# os handles folders and file paths
import os

# duckdb allows querying parquet files without loading fully
import duckdb


# ----------Config----------
TLC_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
DATA_FOLDER = r"D:\Coding\DataScience\DataScience_Assignment1_NYC_Congestion\data"


# Ensure folder exists
os.makedirs(DATA_FOLDER, exist_ok=True)


# -------Scrape Dataset Links ----------
def scrape_parquet_links():
    """
    Scrapes TLC website and returns parquet links for 2025.
    """
    response = requests.get(TLC_URL)
    soup = BeautifulSoup(response.text, "html.parser")

    links = []
    for link in soup.find_all("a"):
        href = link.get("href")

        if href and "2025" in href and "parquet" in href:
            if "yellow" in href or "green" in href:
                links.append(href)

    return links


# --------Download Files ----------
def download_files(links):
    """
    Downloads parquet files if not already present.
    """
    for url in links:
        filename = url.split("/")[-1]
        filepath = os.path.join(DATA_FOLDER, filename)

        if not os.path.exists(filepath):
            print("Downloading:", filename)

            data = requests.get(url).content
            with open(filepath, "wb") as f:
                f.write(data)

        else:
            print("Already exists:", filename)


# -----Detect Missing December ----------
def december_missing():
    """
    Check if December 2025 dataset exists.
    """

    for file in os.listdir(DATA_FOLDER):
        if "2025-12" in file:
            return False

    return True


def download_if_missing(year, month):
    """
    Downloads specific year-month taxi data if missing.
    """

    base_urls = [
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet",
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"
    ]

    for url in base_urls:
        filename = url.split("/")[-1]
        filepath = os.path.join(DATA_FOLDER, filename)

        if not os.path.exists(filepath):
            print("Downloading required file:", filename)
            data = requests.get(url).content
            with open(filepath, "wb") as f:
                f.write(data)
        else:
            print("Already exists:", filename)


def impute_december_if_missing():
    """
    Detect missing December 2025 and compute
    weighted December using 2023 and 2024 data.
    """

    if not december_missing():
        print("December 2025 available.")
        return

    print("December 2025 missing. Preparing imputation.")

    download_if_missing("2023", "12")
    download_if_missing("2024", "12")

    con = duckdb.connect()

    files_2023 = [
        os.path.join(DATA_FOLDER, "yellow_tripdata_2023-12.parquet"),
        os.path.join(DATA_FOLDER, "green_tripdata_2023-12.parquet")
    ]

    files_2024 = [
        os.path.join(DATA_FOLDER, "yellow_tripdata_2024-12.parquet"),
        os.path.join(DATA_FOLDER, "green_tripdata_2024-12.parquet")
    ]

    # Validate files exist
    for f in files_2023 + files_2024:
        if not os.path.exists(f):
            raise Exception(f"Required file missing: {f}")

    stats23 = con.execute(f"""
        SELECT
            COUNT(*) AS trips,
            AVG(trip_distance) AS avg_distance,
            AVG(fare_amount) AS avg_fare,
            AVG(total_amount) AS avg_total,
            AVG(congestion_surcharge) AS avg_surcharge
        FROM read_parquet({files_2023})
    """).fetchdf()

    stats24 = con.execute(f"""
        SELECT
            COUNT(*) AS trips,
            AVG(trip_distance) AS avg_distance,
            AVG(fare_amount) AS avg_fare,
            AVG(total_amount) AS avg_total,
            AVG(congestion_surcharge) AS avg_surcharge
        FROM read_parquet({files_2024})
    """).fetchdf()

    weighted = 0.3 * stats23 + 0.7 * stats24

    weighted.to_parquet("imputed_december_2025.parquet")

    print("Imputed December statistics saved.")


#-----Unified Schema ----------
def create_unified_schema():
    """
    Standardize schema and preserve taxi type and tips.
    """

    con = duckdb.connect()

    # Yellow taxi
    yellow_query = f"""
        SELECT
            'Yellow' AS taxi_type,
            tpep_pickup_datetime AS pickup_time,
            tpep_dropoff_datetime AS dropoff_time,
            PULocationID AS pickup_loc,
            DOLocationID AS dropoff_loc,
            trip_distance,
            fare_amount AS fare,
            tip_amount,
            total_amount,
            congestion_surcharge
        FROM read_parquet('{DATA_FOLDER}/yellow_*.parquet')
    """

    # Green taxi
    green_query = f"""
        SELECT
            'Green' AS taxi_type,
            lpep_pickup_datetime AS pickup_time,
            lpep_dropoff_datetime AS dropoff_time,
            PULocationID AS pickup_loc,
            DOLocationID AS dropoff_loc,
            trip_distance,
            fare_amount AS fare,
            tip_amount,
            total_amount,
            congestion_surcharge
        FROM read_parquet('{DATA_FOLDER}/green_*.parquet')
    """

    con.execute(f"""
        CREATE OR REPLACE TABLE unified_trips AS
        {yellow_query}
        UNION ALL
        {green_query}
    """)

    con.execute("""
        COPY unified_trips TO 'unified_trips.parquet'
        (FORMAT PARQUET);
    """)

    print("Unified dataset created successfully.")


#---- Ghost Trip Filter-----
def ghost_trip_filter():
    """
    Detect fraudulent trips and create audit logs.
    """

    con = duckdb.connect()

    print("Loading unified dataset...")

    # Load unified parquet file instead of table
    con.execute("""
        CREATE OR REPLACE TABLE trips_with_metrics AS
        SELECT *,
            EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 60
                AS duration_minutes,

            CASE
                WHEN EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) > 0
                THEN trip_distance /
                     (EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 3600)
                ELSE 0
            END AS avg_speed_mph
       FROM read_parquet('unified_trips.parquet')
WHERE EXTRACT(YEAR FROM pickup_time) >= 2023;

    """)

    print("Detecting ghost trips...")

    con.execute("""
        CREATE OR REPLACE TABLE ghost_trips AS
        SELECT *
        FROM trips_with_metrics
        WHERE
            avg_speed_mph > 65
            OR (duration_minutes < 1 AND fare > 20)
            OR (trip_distance = 0 AND fare > 0);
    """)

    con.execute("""
        COPY ghost_trips TO 'audit_log.parquet'
        (FORMAT PARQUET);
    """)

    con.execute("""
        CREATE OR REPLACE TABLE clean_trips AS
        SELECT *
        FROM trips_with_metrics
        WHERE NOT (
            avg_speed_mph > 65
            OR (duration_minutes < 1 AND fare > 20)
            OR (trip_distance = 0 AND fare > 0)
        );
    """)

    con.execute("""
        COPY clean_trips TO 'clean_trips.parquet'
        (FORMAT PARQUET);
    """)

    print("Ghost trip filtering completed.")

    #-----Build Congestion Zone------
def build_congestion_zone_reference():
    """
    Identify congestion zone LocationIDs.
    """

    lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    lookup_path = os.path.join(DATA_FOLDER, "taxi_zone_lookup.csv")

    # Download lookup if missing
    if not os.path.exists(lookup_path):
        print("Downloading taxi zone lookup...")
        data = requests.get(lookup_url).content
        with open(lookup_path, "wb") as f:
            f.write(data)

    con = duckdb.connect()

    # Load lookup data
    con.execute(f"""
        CREATE OR REPLACE TABLE taxi_zones AS
        SELECT *
        FROM read_csv_auto('{lookup_path}');
    """)

    # Manhattan zones approximation
    con.execute("""
       CREATE OR REPLACE TABLE congestion_zone AS
SELECT LocationID
FROM taxi_zones
WHERE Borough = 'Manhattan'
AND Zone NOT LIKE '%Harlem%'
AND Zone NOT LIKE '%Inwood%'
AND Zone NOT LIKE '%Washington Heights%';


    """)

    # Save reference
    con.execute("""
        COPY congestion_zone
        TO 'congestion_zone.parquet'
        (FORMAT PARQUET);
    """)

    print("Congestion zone mapping created.")

#----Leakage Audit---
def congestion_leakage_audit():
    """
    Compute surcharge compliance and leakage.
    """

    con = duckdb.connect()

    # Load clean trips
    con.execute("""
        CREATE OR REPLACE TABLE trips AS
        SELECT *
        FROM read_parquet('clean_trips.parquet');
    """)

    # Load congestion zone IDs
    con.execute("""
        CREATE OR REPLACE TABLE zones AS
        SELECT *
        FROM read_parquet('congestion_zone.parquet');
    """)

    # Trips entering congestion zone
    con.execute("""
        CREATE OR REPLACE TABLE entering_zone AS
        SELECT t.*
        FROM trips t
        LEFT JOIN zones p
            ON t.pickup_loc = p.LocationID
        LEFT JOIN zones d
            ON t.dropoff_loc = d.LocationID
        WHERE
            p.LocationID IS NULL
            AND d.LocationID IS NOT NULL
            AND EXTRACT(YEAR FROM pickup_time) >= 2024;

    """)

    # Compliance stats
    con.execute("""
        CREATE OR REPLACE TABLE compliance_stats AS
        SELECT
            COUNT(*) AS total_entering,
            SUM(
                CASE
                    WHEN congestion_surcharge > 0 THEN 1
                    ELSE 0
                END
            ) AS compliant_trips
        FROM entering_zone;
    """)

    # Leakage trips
    con.execute("""
        CREATE OR REPLACE TABLE leakage_trips AS
        SELECT *
        FROM entering_zone
        WHERE congestion_surcharge IS NULL
           OR congestion_surcharge <= 0;
    """)

    # Top leakage pickup zones
    con.execute("""
        CREATE OR REPLACE TABLE top_leakage_pickups AS
        SELECT
            pickup_loc,
            COUNT(*) AS leakage_count
        FROM leakage_trips
        GROUP BY pickup_loc
        ORDER BY leakage_count DESC
        LIMIT 3;
    """)

    # Save outputs
    con.execute("""
        COPY leakage_trips
        TO 'leakage_trips.parquet'
        (FORMAT PARQUET);
    """)

    con.execute("""
        COPY top_leakage_pickups
        TO 'top_leakage_pickups.parquet'
        (FORMAT PARQUET);
    """)

    con.execute("""
        COPY compliance_stats
        TO 'compliance_stats.parquet'
        (FORMAT PARQUET);
    """)

    print("Correct leakage audit completed.")


#----KPI Comuptation----
def compute_kpis():
    """
    Compute aggregated KPIs for analysis and dashboard.
    """

    con = duckdb.connect()

    # Load clean trips
    con.execute("""
        CREATE OR REPLACE TABLE trips AS
        SELECT *
        FROM read_parquet('clean_trips.parquet');
    """)

    # Monthly aggregation
    con.execute("""
        CREATE OR REPLACE TABLE monthly_kpis AS
        SELECT
            DATE_TRUNC('month', pickup_time) AS month,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            SUM(congestion_surcharge) AS congestion_revenue,
            AVG(trip_distance) AS avg_distance,
            AVG(
                EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 60
            ) AS avg_duration_minutes
        FROM trips
        GROUP BY 1
        ORDER BY 1;
    """)

    # Save aggregated results
    con.execute("""
        COPY monthly_kpis
        TO 'monthly_kpis.parquet'
        (FORMAT PARQUET);
    """)

    print("KPI aggregation completed.")

#---DashBoard Dataset-----
def prepare_dashboard_datasets():
    """
    Prepare aggregated datasets for dashboards.
    """

    con = duckdb.connect()

    # Load clean trips
    con.execute("""
        CREATE OR REPLACE TABLE trips AS
        SELECT *
        FROM read_parquet('clean_trips.parquet');
    """)

    # Trips per pickup zone
    con.execute("""
        CREATE OR REPLACE TABLE zone_trip_counts AS
        SELECT
            pickup_loc,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS revenue
        FROM trips
        GROUP BY pickup_loc
        ORDER BY trip_count DESC;
    """)

    # Leakage summary per month
    con.execute("""
        CREATE OR REPLACE TABLE leakage AS
        SELECT *
        FROM read_parquet('leakage_trips.parquet');
    """)

    con.execute("""
        CREATE OR REPLACE TABLE monthly_leakage AS
        SELECT
            DATE_TRUNC('month', pickup_time) AS month,
            COUNT(*) AS leakage_trips,
            SUM(total_amount) AS leakage_revenue
        FROM leakage
        GROUP BY 1
        ORDER BY 1;
    """)

    # Save dashboard datasets
    con.execute("""
        COPY zone_trip_counts
        TO 'dashboard_zone_counts.parquet'
        (FORMAT PARQUET);
    """)

    con.execute("""
        COPY monthly_leakage
        TO 'dashboard_leakage.parquet'
        (FORMAT PARQUET);
    """)

    print("Dashboard datasets prepared.")

#----Yellow Green Decline------
def yellow_green_decline():
    """
    Compare Q1 2024 vs Q1 2025 zone entry volumes.
    """

    con = duckdb.connect()

    # Load unified data
    con.execute("""
        CREATE OR REPLACE TABLE trips AS
        SELECT *
        FROM read_parquet('unified_trips.parquet');
    """)

    # Load congestion zones
    con.execute("""
        CREATE OR REPLACE TABLE zones AS
        SELECT *
        FROM read_parquet('congestion_zone.parquet');
    """)

    # Trips entering zone
    con.execute("""
        CREATE OR REPLACE TABLE entering_zone AS
        SELECT t.*
        FROM trips t
        LEFT JOIN zones p
            ON t.pickup_loc = p.LocationID
        LEFT JOIN zones d
            ON t.dropoff_loc = d.LocationID
        WHERE
            p.LocationID IS NULL
            AND d.LocationID IS NOT NULL;
    """)

    # Filter Q1
    con.execute("""
        CREATE OR REPLACE TABLE q1_data AS
        SELECT
            taxi_type,
            EXTRACT(YEAR FROM pickup_time) AS year
        FROM entering_zone
        WHERE EXTRACT(MONTH FROM pickup_time) IN (1,2,3)
          AND EXTRACT(YEAR FROM pickup_time) IN (2024, 2025);
    """)

    # Aggregate
    con.execute("""
        CREATE OR REPLACE TABLE q1_comparison AS
        SELECT
            taxi_type,
            year,
            COUNT(*) AS trips
        FROM q1_data
        GROUP BY taxi_type, year;
    """)

    # Save result
    con.execute("""
        COPY q1_comparison
        TO 'q1_yellow_green.parquet'
        (FORMAT PARQUET);
    """)

    print("Yellow vs Green decline analysis completed.")

#--- Border Effect Choropleth----    
def border_effect_analysis():
    """
    Compute percent change in dropoffs outside congestion zone.
    """

    con = duckdb.connect()

    # Load clean trips
    con.execute("""
        CREATE OR REPLACE TABLE trips AS
        SELECT *
        FROM read_parquet('clean_trips.parquet');
    """)

    # Load congestion zones
    con.execute("""
        CREATE OR REPLACE TABLE zones AS
        SELECT *
        FROM read_parquet('congestion_zone.parquet');
    """)

    # Load zone lookup
    con.execute(f"""
        CREATE OR REPLACE TABLE lookup AS
        SELECT *
        FROM read_csv_auto('{DATA_FOLDER}/taxi_zone_lookup.csv');
    """)

    # Manhattan zones outside congestion zone
    con.execute("""
        CREATE OR REPLACE TABLE border_zones AS
SELECT l.LocationID
FROM lookup l
LEFT JOIN zones z
    ON l.LocationID = z.LocationID
WHERE l.Borough = 'Manhattan'
AND z.LocationID IS NULL;


    """)

    # Dropoffs in border zones
    con.execute("""
        CREATE OR REPLACE TABLE border_dropoffs AS
        SELECT
            dropoff_loc,
            EXTRACT(YEAR FROM pickup_time) AS year
        FROM trips t
        JOIN border_zones b
           ON CAST(t.dropoff_loc AS INTEGER) = CAST(b.LocationID AS INTEGER)

       WHERE EXTRACT(YEAR FROM pickup_time) IN (2024, 2025);

    """)

    # Aggregate counts
    con.execute("""
        CREATE OR REPLACE TABLE border_counts AS
        SELECT
            dropoff_loc,
            year,
            COUNT(*) AS trips
        FROM border_dropoffs
        GROUP BY dropoff_loc, year;
    """)

    # Compute percent change
    con.execute("""
        CREATE OR REPLACE TABLE border_change AS
SELECT
    COALESCE(a.dropoff_loc, b.dropoff_loc) AS dropoff_loc,
    COALESCE(a.trips, 0) AS trips_2024,
    COALESCE(b.trips, 0) AS trips_2025,
    CASE
        WHEN COALESCE(a.trips,0) > 0
        THEN 100.0 * (COALESCE(b.trips,0) - a.trips) / a.trips
        ELSE NULL
    END AS percent_change
FROM
    (SELECT * FROM border_counts WHERE year = 2024) a
FULL OUTER JOIN
    (SELECT * FROM border_counts WHERE year = 2025) b
ON a.dropoff_loc = b.dropoff_loc;


    """)

    # Save result
    con.execute("""
        COPY border_change
        TO 'border_effect.parquet'
        (FORMAT PARQUET);
    """)

    print("Border effect analysis completed.")

#---Velocity Heatmap----
def congestion_velocity_heatmap():
    """
    Compute average speed heatmap for congestion zone.
    """

    con = duckdb.connect()

    # Load trips
    con.execute("""
        CREATE OR REPLACE TABLE trips AS
        SELECT *
        FROM read_parquet('clean_trips.parquet');
    """)

    # Load congestion zone IDs
    con.execute("""
        CREATE OR REPLACE TABLE zones AS
        SELECT *
        FROM read_parquet('congestion_zone.parquet');
    """)

    # Trips occurring inside congestion zone
    con.execute("""
        CREATE OR REPLACE TABLE zone_trips AS
        SELECT t.*
        FROM trips t
        JOIN zones z
            ON t.pickup_loc = z.LocationID;
    """)

    # Compute speed and extract time features
    con.execute("""
        CREATE OR REPLACE TABLE speed_metrics AS
        SELECT
            EXTRACT(HOUR FROM pickup_time) AS hour,
            EXTRACT(DOW FROM pickup_time) AS weekday,
            CASE
                WHEN EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) > 0
                THEN trip_distance /
                     (EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 3600)
                ELSE NULL
            END AS speed_mph
        FROM zone_trips;
    """)

    # Aggregate speeds
    con.execute("""
        CREATE OR REPLACE TABLE heatmap_data AS
        SELECT
            weekday,
            hour,
            AVG(speed_mph) AS avg_speed
        FROM speed_metrics
        WHERE speed_mph IS NOT NULL
        GROUP BY weekday, hour;
    """)

    # Save result
    con.execute("""
        COPY heatmap_data
        TO 'velocity_heatmap.parquet'
        (FORMAT PARQUET);
    """)

    print("Velocity heatmap data prepared.")

#----Crowding Out Analysis----
def crowding_out_analysis():
    """
    Analyze impact of congestion surcharge on tipping.
    """

    con = duckdb.connect()

    # Load clean trips
    con.execute("""
        CREATE OR REPLACE TABLE trips AS
        SELECT *
        FROM read_parquet('clean_trips.parquet');
    """)

    # Compute tipping metrics
    con.execute("""
        CREATE OR REPLACE TABLE tipping_data AS
        SELECT
            DATE_TRUNC('month', pickup_time) AS month,
            congestion_surcharge,
            CASE
                WHEN fare > 0
                THEN tip_amount / fare
                ELSE NULL
            END AS tip_ratio
        FROM trips;
    """)

    # Aggregate monthly
    con.execute("""
        CREATE OR REPLACE TABLE tipping_summary AS
        SELECT
            month,
            AVG(congestion_surcharge) AS avg_surcharge,
            AVG(tip_ratio) AS avg_tip_ratio
        FROM tipping_data
        WHERE tip_ratio IS NOT NULL
        GROUP BY month
        ORDER BY month;
    """)

    # Save output
    con.execute("""
        COPY tipping_summary
        TO 'crowding_out.parquet'
        (FORMAT PARQUET);
    """)

    print("Crowding out analysis completed.")

#---Rain tax Analysis----
def rain_tax_analysis():
    """
    Analyze effect of rain on taxi demand using NYC weather.
    """

    import pandas as pd

    # Central Park, NYC coordinates
    weather_url = (
        "https://archive-api.open-meteo.com/v1/archive"
        "?latitude=40.7812"
        "&longitude=-73.9665"
        "&start_date=2024-01-01"
        "&end_date=2025-12-31"
        "&daily=precipitation_sum"
        "&timezone=America/New_York"
    )

    weather_path = os.path.join(DATA_FOLDER, "ny_weather.parquet")

    # Download weather if missing
    if not os.path.exists(weather_path):
        print("Downloading NYC weather data...")
        weather_json = requests.get(weather_url).json()

        weather_df = pd.DataFrame({
            "trip_date": weather_json["daily"]["time"],
            "precipitation": weather_json["daily"]["precipitation_sum"]
        })

        weather_df["trip_date"] = pd.to_datetime(
            weather_df["trip_date"]
        ).dt.date

        weather_df.to_parquet(weather_path)

    con = duckdb.connect()

    # Load taxi trips
    con.execute("""
        CREATE OR REPLACE TABLE trips AS
        SELECT *
        FROM read_parquet('clean_trips.parquet');
    """)

    # Daily trip counts
    con.execute("""
        CREATE OR REPLACE TABLE daily_trips AS
        SELECT
            DATE(pickup_time) AS trip_date,
            COUNT(*) AS trip_count
        FROM trips
        GROUP BY trip_date;
    """)

    # Load NYC weather
    con.execute(f"""
        CREATE OR REPLACE TABLE weather AS
        SELECT *
        FROM read_parquet('{weather_path}');
    """)

    # Join weather and trips
    con.execute("""
        CREATE OR REPLACE TABLE rain_analysis AS
        SELECT
            d.trip_date,
            d.trip_count,
            CASE
                WHEN COALESCE(w.precipitation, 0) > 0 THEN 1
                ELSE 0
            END AS rainy
        FROM daily_trips d
        LEFT JOIN weather w
            ON d.trip_date = w.trip_date;
    """)

    # Aggregate rain impact
    con.execute("""
        CREATE OR REPLACE TABLE rain_summary AS
        SELECT
            rainy,
            AVG(trip_count) AS avg_trips
        FROM rain_analysis
        GROUP BY rainy;
    """)

    # Save result
    con.execute("""
        COPY rain_summary
        TO 'rain_tax.parquet'
        (FORMAT PARQUET);
    """)

    print("Rain tax analysis completed.")


# ---------- Pipeline Runner ----------
def run_ingestion():

    links = scrape_parquet_links()
    download_files(links)

    impute_december_if_missing()

    create_unified_schema()

    ghost_trip_filter()

    build_congestion_zone_reference()

    congestion_leakage_audit()

    yellow_green_decline()

    compute_kpis()

    border_effect_analysis()

    congestion_velocity_heatmap()

    crowding_out_analysis()

    rain_tax_analysis()
    
    prepare_dashboard_datasets()






if __name__ == "__main__":
    run_ingestion()
