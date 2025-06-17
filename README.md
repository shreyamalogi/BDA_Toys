# 🧠 Amazon Verified Review Classifier (Toys & Games)

This project builds a scalable Big Data pipeline to analyze Amazon product reviews and classify whether they originate from **verified purchases**. It focuses on reviews in the **Toys & Games** category and uses Apache Spark, Hive, and a Random Forest model implemented via **PySpark MLlib**, all deployed on **Google Cloud Platform (GCP)**.

---

## 📌 Objective

To improve trust in online reviews by predicting whether a review is **verified**, using helpfulness votes and rating data. Applications include:

- Detecting suspicious/unverified reviews
- Enhancing search result rankings
- Supporting recommender systems with verified trust signals

---


---

## 🧰 Technologies Used

| Tool / Platform        | Purpose                                    |
|------------------------|--------------------------------------------|
| Apache Spark (PySpark) | Distributed data processing and ML         |
| Apache Hive            | SQL-style analysis and aggregation         |
| Google Cloud Dataproc  | Cloud-native Spark/Hadoop cluster          |
| Google Cloud Storage   | Input/output dataset handling              |
| Python 3.10+           | Language for pipeline and ML logic         |
| Matplotlib (optional)  | Visualizing feature importance             |

---

## 📥 Dataset

- **Source**: Amazon Product Review Dataset (JSONL format)
- **Category**: Toys and Games
- **Volume**: ~8GB 
- **Location**: `gs://toys555/raw_data/Toys_and_Games.js`

---









