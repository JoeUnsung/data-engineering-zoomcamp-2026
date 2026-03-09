# Week 6: Batch Processing with Spark

## Setup
- **Spark**: PySpark 4.1.1
- **Java**: OpenJDK 17 (via Homebrew)
- **Data**: [Yellow Taxi Trip Data - November 2025](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## How to Run
```bash
cd week6-batch-spark
python3 -m venv venv
source venv/bin/activate
pip install pyspark
python homework.py
```

## Homework Answers

| Q | Question | Answer |
|---|----------|--------|
| 1 | Spark version | 4.1.1 |
| 2 | Average parquet file size (4 partitions) | **25MB** (24.4MB) |
| 3 | Trips on November 15 | **162,604** |
| 4 | Longest trip duration (hours) | **90.6** |
| 5 | Spark UI default port | **4040** |
| 6 | Least frequent pickup zone | **Governor's Island/Ellis Island/Liberty Island** |
