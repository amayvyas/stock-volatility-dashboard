# 📊 Real-Time Stock Data Pipeline & Dashboard

A complete end-to-end data engineering project that ingests, transforms, stores, and visualizes real-time stock data using Python, PostgreSQL, and Streamlit.

---

## 🚀 Project Highlights

- **Real-Time Data Transformation**: Cleans raw stock data, calculates key indicators (volatility, price ranges, trends).
- **PostgreSQL Data Warehouse**: Stores structured stock data with timestamps, change metrics, and zones.
- **Streamlit Dashboard**: Interactive UI to visualize trends, volatility zones, and OHLC data.

---

## 🛠️ Tech Stack

| Layer           | Tools Used                        |
|----------------|-----------------------------------|
| Ingestion       | Python, JSON                      |
| Transformation  | Pandas, Custom Feature Engineering|
| Storage         | PostgreSQL                        |
| Visualization   | Streamlit, Plotly                 |
| Deployment      | Local / Cloud-ready               |

---


## 📉 Dashboard Preview
   🚀 [Click here to view the dashboard](https://your-username-your-repo-name.streamlit.app/) 
> - Line charts for price movements
> - Pie chart for volatility zones
> - Trend indicators and table view


---

## 🔐 Environment Setup

1. Clone this repo:
   ```bash
   git clone https://github.com/yourusername/stock-dashboard.git
   cd stock-dashboard
   ```

2. Create .env file
    ```
    PG_HOST=localhost
    PG_PORT=5432
    PG_DATABASE=your_db
    PG_USER=your_user
    PG_PASSWORD=your_password
    ```
3. Install dependencies:
    ``` 
    pip install -r requirements.txt
    ```
4. Run:
    ```
    streamlit run dashboard.py
    ```

# 🧠 Skills Demonstrated
- SQL schema design & data integrity
- Feature engineering with Pandas
- PostgreSQL integration with psycopg2
- Clean code structure for ETL
- Streamlit + Plotly for interactive dashboards

# 💼 Why This Project Matters
This project is designed to demonstrate core data engineering principles, including real-world ingestion, transformation, and serving of data through a user-friendly interface — exactly the kind of system modern data teams need.

# 📫 Let's Connect
If you're a recruiter or a hiring manager, feel free to reach out via LinkedIn or email for a deeper walkthrough or collaboration.
- 📧[Email](mailto:amay22vyas@gmail.com)
- 💼 [LinkedIn](https://www.linkedin.com/in/amay-v)