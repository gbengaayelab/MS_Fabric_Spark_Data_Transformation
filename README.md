# SalesOrder_Spark_Notebook

This repository contains a Spark notebook for exploring and analyzing sales order data. The notebook includes code snippets to perform various data exploration and transformation tasks using PySpark.

## Table of Contents
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Data Exploration](#data-exploration)
- [Data Transformation](#data-transformation)
- [Saving and Loading Data](#saving-and-loading-data)
- [Creating a Delta Lake Table](#creating-a-delta-lake-table)
- [Running SQL Queries](#running-sql-queries)

## Introduction
The SalesOrder_Spark_Notebook repository provides a notebook that demonstrates how to explore and analyze sales order data using PySpark. The notebook includes code snippets and explanations for each step, allowing users to understand and customize the data exploration process.

## Prerequisites
Before running the notebook, ensure that you have the following prerequisites installed:
- Apache Spark
- PySpark
- Jupyter Notebook or JupyterLab

## Getting Started
1. Clone the repository to your local machine.
2. Install the required dependencies (Spark, PySpark, Jupyter Notebook, etc.).
3. Launch Jupyter Notebook or JupyterLab.
4. Open the SalesOrder_Spark_Notebook.ipynb file in the Jupyter interface.

## Data Exploration
The notebook provides code snippets to explore the sales order data. Here are some of the tasks covered in the notebook:

### Loading the sales order data from CSV files
```python
from pyspark.sql.types import *

orderSchema = StructType([
    StructField("SalesOrderID", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("SalesOrderDate", StringType()),
    StructField("CustomerName", StringType()),
    StructField("CustomerEmailAddress", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("TaxAmount", FloatType())
])

df = spark.read.format("csv").schema(orderSchema).load("Files/orders/*.csv")
display(df)
```

### Filtering and selecting specific columns
```python
Customers = df.select("SalesOrderDate", "UnitPrice")
display(Customers)
```

### Counting rows
```python
print(Customers.count())
```

### Getting distinct values of columns
```python
print(Customers.columns)
print(Customers.distinct().count())
display(Customers.distinct())
```

### Grouping and aggregating data
```python
display(df.select("Item", "Quantity").groupBy("Item").sum().orderBy("sum(Quantity)"))
```

## Data Transformation
The notebook includes examples of data transformation operations on the sales order data. Some of the transformations covered are:

### Creating new columns for Year, Month, and Day of Month
```python
from pyspark.sql.functions import *

transformed_df = df.withColumn("Year", year(col("SalesOrderDate"))).withColumn("Month", month(col("SalesOrderDate"))).withColumn("Day", dayofmonth(col("SalesOrderDate")))
```

### Splitting the CustomerName column into FirstName and LastName
```python
transformed_df = transformed_df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))
```

### Reordering and filtering columns
```python
transformed_df = transformed_df["SalesOrderID", "SalesOrderLineNumber", "SalesOrderDate", "Year", "Month", "Day", "FirstName", "LastName", "CustomerEmailAddress", "Item", "Quantity", "UnitPrice", "TaxAmount"]
```
### Display the last five orders
```python
display(transformed_df.tail(5))
```
### Calculating total revenue per sales order
```python
transformed_df = transformed_df.withColumn("TotalRevenue", col("Quantity") * col("UnitPrice"))
```

## Saving and Loading Data
The notebook demonstrates how to save and load data using different file formats. Here are some examples:

### Saving data as Parquet files
```python
transformed_df.write.format("parquet").mode("overwrite").save("output/transformed_data.parquet")
```

### Loading data from Parquet files
```python
loaded_df = spark.read.format("parquet").load("output/transformed_data.parquet")
```

### Saving data as CSV files
```python
transformed_df.write.format("csv").mode("overwrite").save("output/transformed_data.csv")
```

### Loading data from CSV files
```python
loaded_df = spark.read.format("csv").schema(orderSchema).load("output/transformed_data.csv")
```

## Creating a Delta Lake Table
The notebook shows how to create a Delta Lake table for efficient querying and versioning of data:

### Creating a Delta Lake table from transformed data
```python
transformed_df.write.format("delta").mode("overwrite").save("output/delta_table")
```

### Loading data from a Delta Lake table
```python
loaded_df = spark.read.format("delta").load("output/delta_table")
```

## Running SQL Queries
The notebook allows you to run SQL queries on the sales order data using Spark SQL:

### Registering the DataFrame as a temporary table
```python
transformed_df.createOrReplaceTempView("sales_orders")
```

### Running SQL queries
```python
result = spark.sql("SELECT CustomerName, SUM(TotalRevenue) AS TotalRevenue FROM sales_orders GROUP BY CustomerName ORDER BY TotalRevenue DESC")
display(result)
```

Feel free to refer to the notebook for more code snippets and detailed explanations of each step. Happy exploring and analyzing your sales order data!