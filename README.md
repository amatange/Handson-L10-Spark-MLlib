# Handson-L10-Spark-Streaming-MachineLearning-MLlib


## Overview

This project demonstrates the integration of **PySpark Structured Streaming** and **PySpark MLlib** to perform both individual-level and aggregate-level real-time predictions. The system is split into two logical phases:

1. **Offline Training Phase:** Historical data is processed, features are engineered, and a Linear Regression model is trained and persisted to disk.
2. **Online Inference Phase:** Real-time data is ingested via a network socket, transformed through the same feature pipeline, and evaluated by the saved model to output live predictions.

## Task 4: Real-Time Individual Fare Prediction

### Objective

To build a machine learning and streaming pipeline that predicts the exact total `fare_amount` of an individual taxi trip in real time based solely on the trip's distance (`distance_km`).

### Engineering Approach

#### 1. Machine Learning Pipeline (Offline)

* **Data Ingestion & Type Casting:** The historical training data (`training-dataset.csv`) is read into a DataFrame. Because MLlib's algorithms require numerical features to be in a floating-point representation, the `distance_km` and `fare_amount` columns are explicitly cast to `DoubleType`.
* **Feature Vectorization:** PySpark MLlib expects all input features to be contained within a single vector column. A `VectorAssembler` is configured with `inputCols=["distance_km"]` and `outputCol="features"` to transform the scalar distance into a dense feature vector.
* **Model Training & Persistence:** A `LinearRegression` estimator is initialized, targeting the `fare_amount` label. The model is fitted to the vectorized training data and saved to disk (`models/fare_model`) using `.write().overwrite().save()`. This allows the inference phase to remain fast and decoupling it from training overhead.

#### 2. Structured Streaming Inference (Online)

* **Socket Ingestion:** The streaming application sets up a connection to a local network socket (`localhost:9999`) to receive incoming JSON payloads containing trip events.
* **JSON Parsing:** Using a predefined schema matching the stream, the incoming raw string data is parsed with `from_json()` to unpack the nested fields (`trip_id`, `driver_id`, etc.).
* **Streaming Vectorization:** To ensure mathematical consistency, an identical `VectorAssembler` is used to structure the incoming `distance_km` values into a `features` vector.
* **Inference & Deviation Analysis:** The pre-trained `LinearRegressionModel` is loaded from disk. It runs `.transform()` on the live stream to append a `prediction` column. Finally, a custom metric called `deviation` is calculated by taking the absolute difference (`fare_amount` - `prediction`) to monitor how closely live data matches the trained trends.

### Output

<img width="891" height="1138" alt="image" src="https://github.com/user-attachments/assets/d21b3859-7741-4c30-a608-38570b204d71" />



## Task 5: Time-Windowed Fare Trend Prediction

### Objective

To build an advanced streaming pipeline that shifts focus from individual trips to macro trends. The goal is to aggregate incoming data into time-based windows, extract temporal features, and predict the *average fare* trend (`avg_fare`) for the upcoming window.

### Engineering Approach

#### 1. Windowed Feature Engineering (Offline)

* **Timestamp Handling:** The historical dataset's string-based timestamps are cast to `TimestampType` to unlock PySpark's time-awareness.
* **Tumbling Window Aggregation:** Instead of analyzing row-by-row, rows are grouped into fixed **5-minute non-overlapping tumbling windows** via `groupBy(window(col("event_time"), "5 minutes"))`. The target label is calculated by averaging the fares within that window (`avg("fare_amount").alias("avg_fare")`).
* **Temporal Feature Extraction:** To capture daily patterns (like rush hours), the exact start time of the window (`window.start`) is parsed using the `hour()` and `minute()` functions to engineer two new numerical features: `hour_of_day` and `minute_of_hour`.
* **Vectorization & Training:** These two engineered time features are bundled into a single `features` vector via `VectorAssembler` and used to train a `LinearRegression` model that maps time of day to expected average fare trends.

#### 2. Sliding Window Streaming Inference (Online)

* **Watermarking for Late Data:** Real-time data processing is highly susceptible to network delays. A watermark of `1 minute` is established on the `event_time` column. This instructs Spark to maintain state and allow late-arriving data to be included in aggregations, preventing data loss.
* **Sliding Window Aggregation:** Unlike the offline tumbling windows, the streaming pipeline implements a **5-minute window that slides every 1 minute**. This provides a smoother, continuously updated view of changing trends.
* **Feature Alignment & Live Prediction:** The streaming window slices are passed through the exact same feature engineering pipeline—generating `hour_of_day` and `minute_of_hour` from the streaming window bounds. The pre-trained trend model reads these vectors on the fly, emitting a running prediction of what the next window's average fare market rate will look like.

### Output

<img width="684" height="299" alt="image" src="https://github.com/user-attachments/assets/c38d1f22-b0aa-4d3e-934a-a1b0d9bd921e" />
