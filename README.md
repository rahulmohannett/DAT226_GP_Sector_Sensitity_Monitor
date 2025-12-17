The S&P 500 is supposed to be a well-diversified snapshot of the U.S. economy. But here's the thing: a few dominant sectors (looking at you, Tech) drive most of the movement. Traditional dashboards show you sector weights and pretty price charts, but they miss the real story—how sensitive the index is to each sector's behavior.
We call this the "Illusion of Diversification"—and this project is designed to pull back the curtain.
The Problem We're Solving
Most market analytics tools are:

Static: They show you what happened, not what's driving the movement
Fragmented: Scattered scripts, manual updates, inconsistent metrics
Misleading: Sector weights ≠ sector influence

Our Solution
A cloud-native, warehouse-first pipeline that:
1)Automatically ingests daily market data
2) Calculates rolling betas and covariance to measure real sector influence
3) Trains ML models to forecast volatility trends
4) Updates dashboards automatically—no manual work required
5) Runs entirely in Snowflake (no data movement, no headaches)


Architecture
We built this using a modern ELT stack with five core components:
Yahoo Finance → Airflow → Snowflake → dbt → Preset
     ↓            ↓          ↓         ↓       ↓
   Data      Orchestration  Warehouse Transform  Dashboards
Tech Stack
TechnologyPurposeApache AirflowOrchestrates four decoupled DAGs for ingestion, transformation, ML, and servingSnowflakeData warehouse + compute engine (everything runs here)dbtSQL transformations, data quality tests, and lineage trackingSnowflake CortexAutoML forecasting—trains and predicts directly in the warehousePresetReal-time dashboards with heatmaps, regime indicators, and forecast conesDockerContainerized dev environment for team consistency

Data Flow
Bronze → Silver → Gold (Medallion Architecture)

Bronze (RAW): Raw JSON payloads from Yahoo Finance API
Silver (STAGING): Cleaned, deduplicated OHLCV data with log-returns
Gold (MARTS): Rolling betas, risk contributions, forecasts, and analytical views

The Four DAGs
We designed four independent DAGs connected only through sensors (no cascading failures):

DAG 1 - Daily Ingestion: Pulls OHLCV data for 11 sector ETFs + SPY benchmark
DAG 2 - Upstream Transformations: Runs dbt models for cleaning, feature engineering, and volatility metrics
DAG 3 - ML Training & Prediction: Trains Cortex forecasting model on 730 days of rolling betas, predicts 30 days ahead
DAG 4 - Downstream Serving: Merges historical and forecasted data into final dashboard-ready marts


What We Measure
Key Metrics

Log-Returns: Statistically stable returns for proper covariance analysis
Rolling Beta: How much each sector moves relative to the S&P 500 (SPY)
Risk Contribution: Beta × Sector Weight = True influence on index volatility
Market Regime Indicator: Tracks offensive vs. defensive sector dominance
Volatility Forecasts: 30-day predictions with confidence intervals

The Math (If You're Curious)
Rolling Beta Formula:
β(sector, t) = Cov(sector returns, SPY returns) / Var(SPY returns)
Risk Contribution:
Weighted Impact = β(t) × Sector Weight

Dashboard Highlights
Our Preset dashboards give you:

Sector Sensitivity Heatmap: See which sectors are driving volatility right now
Risk-Reward Opportunity Matrix: Identify high-beta/low-return sectors (inefficient risk)
Market Regime Indicator: Are we in an offensive or defensive market?
Forecast Cone: Where is sector sensitivity heading over the next 30 days?


Key Findings
The "Sticky Risk" of Technology
Our forecasting model revealed something important:

Technology (XLK) currently has a rolling beta above 1.5 (50% more volatile than the index)
The 30-day forecast shows sustained high sensitivity—no mean reversion to safety
Translation: The S&P 500 remains structurally dependent on Tech volatility

This confirms our hypothesis: the index feels diversified, but it's actually concentrated in a few high-volatility sectors.

Getting Started
Prerequisites

Docker & Docker Compose
Snowflake account with Cortex enabled
Airflow environment (or use our containerized setup)
Python 3.9+

Setup

Clone the repo

bash   git clone https://github.com/rahulmohannett/DAT226_GP_Sector_Sensitity_Monitor.git
   cd DAT226_GP_Sector_Sensitity_Monitor

Configure environment variables

Copy .env.example to .env
Add your Snowflake credentials


Start Docker containers

bash   docker-compose up -d

Initialize Airflow

bash   docker exec -it airflow-webserver airflow db init

Trigger the pipeline

Access Airflow UI at localhost:8080
Enable and trigger DAG 1 (ingestion runs daily at 02:30 UTC)




Lessons Learned

Static sector definitions work fine: S&P sectors change rarely, so hardcoded ETF symbols simplified the pipeline
dbt snapshots are essential: Prevents historical overwrites during sector reclassifications
Cortex training windows matter: Too long = smoothed patterns, too short = overfitting
Airflow sensors prevent chaos: Ensures downstream models don't run on incomplete data
Stock splits break log-returns: We had to fall back to simple returns after a mid-project split event


Future Enhancements

 Multi-factor models (PCA, macroeconomic indicators)
 Intraday ingestion for higher-frequency analytics
 Regime classification ML (offensive vs. defensive detection)
 Slack/email alerts for volatility spikes
 SCD Type-2 logic for versioned sector weights


👥 Team
San José State University - DATA 226 (Fall 2025)

Disha Rawat - disha.rawat@sjsu.edu
Haritha Gouttumukka - nagasaiharitha.gottumukka@sjsu.edu
Rahul Mohan Devi - rahul.mohandevi@sjsu.edu
Ruchika Chaurasia - ruchika.chaurasia@sjsu.edu

Course: DATA 226 – Data Warehouse and Pipeline
Professor: Dr. Keeyong Han
