# COMP4442 Group Project: Driving Behavior Analysis System

An end-to-end, cloud-native big data application designed to analyze and monitor driver behavior. The system processes extensive driving records to aggregate vital statistics (overspeeding, fatigue driving, neutral sliding) and provides an interactive, real-time dashboard for continuous monitoring.

## 🏗️ System Architecture & Cloud Infrastructure

This project leverages **Amazon Web Services (AWS)** for robust, scalable cloud deployment:

- **AWS RDS (MySQL)**: A relational database storing aggregated driver summary statistics dynamically served to the dashboard.
- **AWS S3**: Acts as our Data Lake (storing huge `cleaned_data.csv` files) and seamlessly hosts the Frontend as a Serverless Static Website.
- **AWS EMR**: Runs **Apache Spark** jobs (`spark_analyzer_S3.py`) to process millions of driving records in a distributed manner and directly injects the results into RDS via JDBC.
- **AWS Elastic Beanstalk**: Hosts the **FastAPI** backend within a Docker container, making the API layer highly available.

## 📂 Project Structure

```text
COMP4442GroupProject/
├── backend/              # FastAPI server, Database Clients, Replay Engine, Docker Deployment config
├── data_processor/       # Pandas cleaning scripts, Spark Analyzer, S3 Uploader
└── frontend/             # HTML, CSS, Vanilla JS, Chart.js for the dashboard
```

## ✨ Key Features

1. **Comprehensive Driver Summary**: View aggregated data such as total overspeed counts, fatigue alerts, and total overspeed/neutral-sliding times for the selected timeframe.
2. **Real-Time Speed Monitoring**: Simulates a live data stream of drivers' speeds using a global clock replay engine operating at a 10x real-world speed multiplier.
3. **Live Alerts**: The dashboard (updating every 30 seconds via polling) triggers visual high-speed warnings whenever drivers exceed 100 km/h or flag an overspeeding event.

## 🚀 Local Development Setup

### 1. Data Preprocessing
Navigate to `data_processor/` and run the Pandas script to merge the raw daily logs into a standard format:
```bash
pip install -r requirements.txt
python data_preprocessing.py
```

### 2. Spark Aggregation (Local DB)
Configure your local MySQL credentials in `data_processor/.env`, then run the Spark analyzer to compute and insert the driver summaries:
```bash
python spark_analyzer.py
```

### 3. Start the Backend API
Navigate to `backend/`, setup local database & cache modes in `.env`, and start the Uvicorn server:
```bash
pip install -r requirements.txt
python run.py
```

### 4. Launch the Dashboard
Open `frontend/index.html` in your browser (preferably via VS Code Live Server) to see the dashboard in action.

## ☁️ AWS Deployment Workflow

1. **Data Upload**: Create an S3 bucket and use `s3_uploader.py` to upload the cleaned dataset.
2. **Analysis on EMR**: Submit `spark_analyzer_S3.py` as a Spark Application step to an AWS EMR cluster. Supply your RDS credentials and S3 paths as arguments to aggregate the data into AWS RDS.
3. **Backend Deployment**: Ensure `.env` is updated (or use EB Environment Properties), zip the inner contents of the `backend/` directory, and deploy it to a Docker platform on **AWS Elastic Beanstalk**.
4. **Frontend Hosting**: Update the `API_BASE_URL` in `frontend/js/api_service.js` to match the Elastic Beanstalk endpoint. Upload the `frontend/` contents to an S3 bucket configured for **Static Website Hosting**.
   > *Note: Access the frontend exclusively via the S3 `http://...s3-website...` endpoint to avoid HTTP/HTTPS Mixed Content errors.*

## 🛠️ Technologies Used
- **Frontend**: HTML5, CSS3 (Bootstrap 5), JavaScript (Fetch API), Chart.js
- **Backend**: Python 3.9, FastAPI, Uvicorn, SQLAlchemy, PyMySQL
- **Data Engineering**: Pandas, Apache Spark (PySpark)
- **AWS**: S3, RDS, EMR, Elastic Beanstalk
