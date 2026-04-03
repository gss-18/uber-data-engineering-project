# ⚡ Uber Real-Time Data Engineering

> End-to-end streaming data pipeline with a live analytics dashboard — built on Azure EventHub, Databricks Delta Live Tables, and Streamlit.

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.35+-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-EventHub-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-DLT-FF3621?style=flat-square&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8?style=flat-square&logo=apachespark&logoColor=white)

---

## 📐 Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────────┐
│  FastAPI / Local │────▶│  Azure EventHub  │────▶│  Databricks DLT         │
│  Ride Generator  │     │  (Kafka Protocol)│     │  Delta Live Tables       │
└─────────────────┘     └──────────────────┘     └────────────┬────────────┘
                                                               │
                         ┌─────────────────┐                  ▼
                         │  Azure Data      │     ┌─────────────────────────┐
                         │  Factory (Batch) │────▶│  Medallion Architecture  │
                         └─────────────────┘     │  Bronze → Silver → Gold  │
                                                  └────────────┬────────────┘
                                                               │
                                                               ▼
                                                  ┌─────────────────────────┐
                                                  │  Streamlit Dashboard     │
                                                  │  Mission Control         │
                                                  └─────────────────────────┘
```

---

## 🗂 Data Architecture — Medallion Schema

### 🟫 Bronze (9 tables)
Raw ingestion layer — no transformations applied.

| Table | Description |
|---|---|
| `bulk_rides` | 2,000 historical rides loaded via Azure Data Factory |
| `rides_raw` | Live EventHub stream (Kafka) |
| `streaming_rides_archive` | Persistent backup — survives EventHub delete/recreate cycles |
| `map_cities` | 40 US cities across 4 regions with coordinates |
| `map_cancellation_reasons` | 4 cancellation reason codes |
| `map_payment_methods` | 4 payment types |
| `map_ride_statuses` | 2 ride status codes |
| `map_vehicle_makes` | 7 vehicle manufacturers |
| `map_vehicle_types` | 5 service tiers |

### 🪙 Silver (2 tables)
Cleaned and joined layer.

| Table | Description |
|---|---|
| `stg_rides` | Merged bulk + stream + archive — deduplicated |
| `silver_obt` | One Big Table — all dimensions joined for downstream consumption |

### 🥇 Gold (7 tables — Star Schema)
Analytics-ready layer.

| Table | Type | Description |
|---|---|---|
| `fact` | Fact | Ride measures + foreign keys |
| `dim_passenger` | SCD Type 1 | Passenger profiles |
| `dim_driver` | SCD Type 1 | Driver profiles |
| `dim_vehicle` | SCD Type 1 | Vehicle registry |
| `dim_booking` | SCD Type 1 | Booking details + coordinates |
| `dim_payment` | SCD Type 1 | Payment methods |
| `dim_location` | SCD Type 2 | City dimension — tracks historical city changes |

---

## 🛠 Tech Stack

| Layer | Technology |
|---|---|
| **Streaming Ingestion** | Azure EventHub (Standard, Kafka Protocol) |
| **Batch Ingestion** | Azure Data Factory |
| **Processing** | Databricks Delta Live Tables (DLT) |
| **Storage** | Delta Lake (Unity Catalog) |
| **Secrets** | Azure Key Vault |
| **Infrastructure Auth** | Azure Service Principal |
| **Dashboard** | Streamlit (deployed on Streamlit Cloud) |
| **Data Generation** | Python + Faker |
| **Language** | Python 3.11+ |

---

## 📊 Dashboard — Mission Control

Two-tab Streamlit app deployed at Streamlit Cloud.

**Tab 01 — Analytics & Live Data**
- KPI cards: total rides, revenue, avg fare, cancellation rate, avg rating
- Rides by city (bar chart)
- Vehicle type distribution (donut)
- Payment method breakdown
- Surge multiplier distribution
- Top drivers leaderboard
- Live ride feed (auto-refreshes every 10s)
- Pickup heatmap (Folium)
- Regional revenue breakdown

**Tab 02 — Pipeline Control** *(password protected)*
- Start / Stop Azure EventHub (provisions/deletes namespace via Azure SDK)
- Trigger Databricks DLT pipeline update
- Full pipeline refresh
- Real-time DLT pipeline status bar
- Medallion schema overview

---

## 🔑 Key Engineering Decisions

**Archive pattern for streaming continuity**
EventHub is deleted between sessions to avoid cost. A `streaming_rides_archive` Bronze table persists all streamed rides to Delta Lake before each delete, so ride counts accumulate across sessions without data loss.

**Azure Key Vault as secrets source of truth**
`CONNECTION_STRING` and `LISTENER_CONNECTION_STRING` are written to Key Vault after each EventHub provisioning. The Streamlit app fetches them at runtime — no manual secrets update needed after start/stop.

**Azure SDK replacing az CLI**
All EventHub management (create namespace, create topic, create policies, fetch keys, delete namespace) uses the `azure-mgmt-eventhub` Python SDK instead of subprocess `az` CLI calls, making the control panel buttons work on Streamlit Cloud.

**DLT pipeline optimization**
Pipeline execution reduced from ~6–8 minutes to ~75 seconds by: removing unnecessary watermark on `silver_obt`, setting shuffle partitions to 4, introducing a `gold_base` cache table to prevent multiple stream readers on `silver_obt`, and correcting the fact table CDC key to `ride_id` only.

**SCD Type 2 on `dim_location`**
Retained for portfolio demonstration value — tracks historical city-level changes even though the current dataset doesn't strictly require it.

---

## 🚀 Local Setup

### Prerequisites
- Python 3.11+
- Azure CLI (`az`) installed and logged in
- Databricks workspace (Community Edition or higher)
- Azure subscription with EventHub + Key Vault access

### 1. Clone and install
```bash
git clone https://github.com/YOUR_USERNAME/uber-data-engineering-project.git
cd uber-data-engineering-project
python -m venv .uber_de
.uber_de\Scripts\activate       # Windows
pip install -r requirements.txt
```

### 2. Configure `.env`
```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token
PIPELINE_ID=your-dlt-pipeline-id
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id

ADMIN_USERNAME=your-username
ADMIN_PASSWORD=your-password
SECRET_KEY=your-secret-key

AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
AZURE_SUBSCRIPTION_ID=your-subscription-id
```

### 3. Start a session
```bash
# Provision EventHub + write credentials to Key Vault
python start_eventhub.py

# Stream rides to EventHub
python ingest.py

# Run dashboard locally
streamlit run streamlit_app/main.py
```

### 4. End a session
```bash
# Delete EventHub + clear Key Vault — stops billing
python stop_eventhub.py
```

---

## ☁️ Streamlit Cloud Deployment

### One-time secrets setup
Add these to **App ⋮ → Settings → Secrets** in Streamlit Cloud:

```toml
DATABRICKS_HOST = "https://your-workspace.cloud.databricks.com"
DATABRICKS_TOKEN = "your-token"
PIPELINE_ID = "your-pipeline-id"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/your-warehouse-id"

ADMIN_USERNAME = "your-username"
ADMIN_PASSWORD = "your-password"
SECRET_KEY = "your-secret-key"

AZURE_TENANT_ID = "your-tenant-id"
AZURE_CLIENT_ID = "your-client-id"
AZURE_CLIENT_SECRET = "your-client-secret"
AZURE_SUBSCRIPTION_ID = "your-subscription-id"
```

`CONNECTION_STRING` and `LISTENER_CONNECTION_STRING` do **not** need to be set manually — they are fetched from Azure Key Vault at runtime after running `start_eventhub.py` locally or clicking **Start EventHub** in the control panel.

---

## 💰 Cost Model

| Resource | Cost |
|---|---|
| Azure EventHub Standard | ~$0.015/TU/hr — deleted between sessions |
| Azure Key Vault | ~$0.00/month at portfolio scale |
| Azure Data Factory | Minimal — batch load only |
| Databricks Community Edition | Free |
| Streamlit Cloud | Free |

---

## 📁 Project Structure

```
uber-data-engineering-project/
├── streamlit_app/
│   ├── main.py                    # Entry point — page config, top bar, tabs
│   └── components/
│       ├── analytics.py           # Tab 01 — charts, KPIs, live feed, map
│       ├── control.py             # Tab 02 — pipeline control panel
│       └── pipeline_status.py     # DLT pipeline status bar component
├── Code_Files/
│   ├── archive.py                 # DLT notebook — streaming archive
│   ├── ingest.py                  # DLT notebook — bronze ingestion
│   ├── model.py                   # DLT notebook — gold star schema
│   ├── silver.py                  # DLT notebook — silver stg_rides
│   └── silver_obt.sql             # DLT notebook — silver OBT
├── Data/
│   ├── bulk_rides.json            # 2,000 historical rides
│   └── map_*.json                 # Dimension lookup files
├── eventhub_manager.py            # Azure SDK — EventHub + Key Vault operations
├── connection.py                  # EventHub producer — sends ride events
├── db.py                          # Databricks SQL connector + all queries
├── data.py                        # Synthetic ride data generator
├── start_eventhub.py              # Local script — provision EventHub
├── stop_eventhub.py               # Local script — delete EventHub
├── auth.py                        # JWT auth helpers
├── requirements.txt
└── .gitignore
```

---

## 🔒 Security Notes

- Credentials are never hardcoded — read from `.env` locally and `st.secrets` / Azure Key Vault on Streamlit Cloud
- `.env` and `.streamlit/secrets.toml` are gitignored
- Azure Service Principal scoped to `rg-uber` resource group only
- Pipeline Control tab is password-protected

---

*Built as a portfolio project demonstrating real-time data engineering with Azure, Databricks, and Streamlit.*
