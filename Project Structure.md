# Project Root (sp500-sector-monitor)
├── dags/                               
│   ├── market_data_ingest.py           
│   ├── elt_dbt_upstream.py            
│   ├── train_predict.py               
│   ├── elt_dbt_downstream.py           
│   └── __pycache__/                    
│
├── dbt_project/                        
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_market_data.sql
│   │   │   ├── stg_market_forecast.sql
│   │   │   └── _sources.yaml
│   │   ├── intermediate/
│   │   │   └── int_daily_returns.sql
│   │   └── marts/
│   │       ├── mart_sector_volatility.sql
│   │       ├── mart_risk_reward_scatter.sql
│   │       ├── mart_forecast_combined.sql
│   │       └── mart_market_regime.sql
│   ├── seeds/
│   │   ├── sector_metadata.csv
│   │   └── seeds.yaml
│   ├── tests/
│   │   └── test_forecast_dates_future.sql
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml                    
│   ├── logs/                           
│   └── target/                         
│
├── Dockerfile
└── requirements.txt                
│
├── logs/                               
│
├── .gitignore                          
├── docker-compose.yaml                 
└── README.md