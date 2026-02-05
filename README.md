\# NYC Congestion Pricing Impact Analysis



\## Overview

This project builds an automated data pipeline and dashboard to analyze the impact of congestion pricing in New York City.



The system ingests taxi trip data, cleans it, performs congestion analysis, and produces visual insights.



---



\## Features

\- Automated taxi dataset ingestion

\- Schema unification across taxi types

\- Ghost trip detection

\- Congestion leakage audit

\- Traffic speed analysis

\- Demand analysis

\- Streamlit dashboard visualization



---



\## Pipeline Workflow

1\. Download datasets automatically

2\. Clean and unify schemas

3\. Remove fraudulent trips

4\. Perform congestion analysis

5\. Aggregate KPIs

6\. Prepare dashboard datasets



---



\## How to Run



Install dependencies:

pip install -r requirements.txt


Run pipeline:



python pipeline.py


Launch dashboard:



python -m streamlit run DashBoard/app.py


---

## Dashboard Features
- Monthly trip trends
- Revenue trends
- Congestion leakage analysis
- Pickup zone activity
- Traffic speed patterns

---

## Technologies Used
- Python
- DuckDB
- Streamlit
- Pandas
- Web scraping

---

## Author
Ahmad Fareed




