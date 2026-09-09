from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("CDCAudit&DataLineagePipeline")
    .master("local[*]")
    .getOrCreate()
)


# ==== EXTRACT ====


# Read the Dataset
orders_existing_df = (
    spark.read.csv(
        "data/orders_existing.csv",
        header=True,
        inferSchema=True
    )
)

orders_cdc_df = (
    spark.read.csv(
        "data/orders_cdc.csv",
        header=True,
        inferSchema=True
    )
)

# Display the Dataset
print("\n--- Orders Existing ---")
orders_existing_df.show()

print("\n--- Orders CDC ---")
orders_cdc_df.show()

# Display Dataset Schema
print("\n--- Orders Existing Schema ---")
orders_existing_df.printSchema()

print("\n--- Orders CDC Schema ---")
orders_cdc_df.printSchema()


# ==== TRANSFORM ====


# Validate CDC Records
valid_operation = ["UPDATE", "INSERT", "DELETE"]

existing_keys = (
    orders_existing_df
    .select(
        col("order_id").alias("existing_order_id")
    )
)

tagged_cdc_df = (
    orders_cdc_df
    .join(
        existing_keys, 
        on=orders_cdc_df.order_id == existing_keys.existing_order_id, 
        how="left"
    )
    .withColumn(
        "invalid_reason",
        when(
            ~col("operation").isin(valid_operation), "INVALID_OPERATION"
        )
        .when(
            col("order_id").isNull(), "MISSING_ORDER_ID"
        )
        .when(
            (col("operation").isin("INSERT", "UPDATE")) & (col("amount").isNull() | (col("amount") <= 0)) , "INVALID_AMOUNT"
        )
        .when(
            (col("customer_id").isNull()) & (col("operation") != "DELETE"), "MISSING_CUSTOMER_ID"
        )
        .when(
            (col("amount").isNull()) & (col("operation") != "DELETE"), "MISSING_AMOUNT"
        )
        .when(
            (col("operation").isin("UPDATE", "DELETE")) & (col("existing_order_id").isNull()), "ORDER_NOT_FOUND"
        )
        .when(
            (col("operation") == "INSERT") & (col("existing_order_id").isNotNull()), "ORDER_ALREADY_EXISTS"
        )
        .otherwise(None)
    )
    .drop(
        "existing_order_id"
    )
)

# Create Audit Log
audit_log_df = (
    tagged_cdc_df
    .withColumn(
        "processing_status", 
        when(
            col("invalid_reason").isNull(), "APPLIED")
        .otherwise("REJECTED"))
    .withColumn(
        "invalid_reason", 
        when(
            col("invalid_reason").isNull(), "PASSED_VALIDATION")
        .otherwise(col("invalid_reason")))
    .withColumn(
        "processed_at", 
        current_timestamp()
    )
    .select(
        "processed_at",
        "order_id",
        "operation",
        "processing_status",
        "invalid_reason",
        "customer_id",
        "amount"
    )
)

print("\n--- CDC Event Audit Log ---")
audit_log_df.show(truncate=False)

# Invalid Orders CDC
invalid_orders_cdc_df = (
    tagged_cdc_df
    .filter(
        col("invalid_reason").isNotNull()
    )
)

invalid_count = invalid_orders_cdc_df.count()

if invalid_count > 0:
    print("\nInvalid CDC records found:")
    invalid_orders_cdc_df.show()

else:
    print("\nCDC data-quality validation passed.")

# Valid Orders CDC
valid_orders_cdc_df = (
    tagged_cdc_df
    .filter(
        col("invalid_reason").isNull()
    )
    .drop(
        "invalid_reason"
    )
)

# Display Valid Orders CDC
print("\n--- Valid Orders CDC ---")
valid_orders_cdc_df.show()

unchanged_orders_df = (
    orders_existing_df
    .join(
        valid_orders_cdc_df,
        on="order_id",
        how="left_anti"
    )
)

# Display Unnchanged Orders
print("\n--- Unchanged Orders ---")
unchanged_orders_df.show()

cdc_changes_df = (
    valid_orders_cdc_df
    .filter(
        col("operation") != "DELETE"
    )
    .drop(
        "operation"
    )
)

# Create Latest Orders
latest_orders_df = (
    unchanged_orders_df
    .unionByName(
        cdc_changes_df
    )
    .orderBy(
        "order_id"
    )
)

# Display Latest Orders
print("\n--- Latest Orders ---")
latest_orders_df.show()


# ==== LOAD ====


latest_orders_df.write \
    .mode("overwrite") \
    .parquet("output/latest_orders/")

print("\nLatest Orders Saved Successfully.")

invalid_orders_cdc_df.write \
    .mode("overwrite") \
    .parquet("output/rejected_orders/")

print("\nRejected Orders Saved Successfully.")

audit_log_df.write \
    .mode("append") \
    .parquet("output/audit_log/")

print("\nAudit Log Saved Successfully.")