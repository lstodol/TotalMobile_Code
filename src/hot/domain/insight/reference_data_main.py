# Databricks notebook source
# MAGIC %md
# MAGIC ### Load Reference Tables

# COMMAND ----------

dbutils.widgets.text("tenant_name", "")
dbutils.widgets.text("product_name", "")

tenant_name = dbutils.widgets.get("tenant_name")
product_name = dbutils.widgets.get("product_name")

# COMMAND ----------

import logging
import fixpath

from modules.unity_logging import LoggerConfiguration
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

logger = logging.getLogger(__name__)
LoggerConfiguration(tenant_name).configure(logger)

# COMMAND ----------

datedimension_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_datedimension"
timedimension_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_timedimension"
kpadimension_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_kpadimension"
kpidimension_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_kpidimension"
kpitargetconfig_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_kpitargetconfig"
tenantconfig_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_tenantconfig"
shiftexclusionreason_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_shiftexclusionreasondimension"
activitytypedimension_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_activitytypedimension"
timelinecontexttypedimension_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_timelinecontexttypedimension"
taskstatusdimension_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_taskstatusdimension"
taskstatusgroupdimension_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_taskstatusgroupdimension"
pbitimeintervals_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_pbitimeintervals"
pbidateparameter_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_pbidateparameter"
pbitimeparameter_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_pbitimeparameter"
pbivaluearameter_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_pbivalueparameter"
pbilabels_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_pbilabels"
pbitranslations_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_pbitranslations"
pbiworkershiftsheader_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_pbiworkershiftsheader"
pbipertchartrelationships_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_pbipertchartrelationships"
pbikpidimensionoperational_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_pbikpidimensionoperational"
pbitranslatedlocalizedlabels_table = f"/mnt/{tenant_name}/lakehouse/gold/{tenant_name}.gold_{product_name}_pbitranslatedlocalizedlabels"

# COMMAND ----------

# DBTITLE 1,Load Date Tables  
max_load_date = spark.sql(
    f"SELECT CAST(MAX(load_date) AS DATE) AS max_load_date FROM delta.`{datedimension_table}`"
)
today = spark.sql("SELECT CURRENT_DATE() AS today")

max_load_date_result = max_load_date.first().max_load_date
today_result = today.first().today

if max_load_date_result is None or max_load_date_result < today_result:

    date_dimension_df = spark.sql(
        """
        SELECT
            YEAR(currentDate) * 10000 + MONTH(currentDate) * 100 + DAY(currentDate) AS id,
            currentDate AS date,
            DAY(currentDate) AS day_of_month,
            DATE_FORMAT(currentDate, 'EEEE') AS day_name,
            WEEKOFYEAR(currentDate) AS week_of_year,
            MONTH(currentDate) AS month,
            DATE_FORMAT(currentDate, 'MMMM') AS month_name,
            UPPER(SUBSTRING(DATE_FORMAT(currentDate, 'MMMM'), 1, 3)) AS month_name_short,
            QUARTER(currentDate) AS quarter,
            YEAR(currentDate) AS year,
            CONCAT(YEAR(currentDate), '/', WEEKOFYEAR(currentDate)) AS year_week,
            CONCAT(YEAR(currentDate), '/', MONTH(currentDate)) AS year_month,
            CASE WHEN currentDate = CURRENT_DATE() THEN "Today" ELSE currentDate END AS is_today,
            currentDate AS date2,
            CURRENT_TIMESTAMP() AS load_date
        FROM (
            SELECT EXPLODE(SEQUENCE(TO_DATE('2023-01-01'), TO_DATE('2024-12-31'), INTERVAL 1 DAY)) AS currentDate
            ) AS dateRange
    """
    )
    date_dimension_df.write.mode("overwrite").save(datedimension_table)

    ninety_days_ago = date_sub(current_date(), 90)
    pbidateparameter_df = (date_dimension_df
            .select("date", "load_date")
            .filter((col("date") > ninety_days_ago) & (col("date") <= current_date()))
            .sort(col("date").desc())
        )
    pbidateparameter_df.write.mode("overwrite").save(pbidateparameter_table)

    print("Data inserted into the date dimensions.")
else:
    print("Table load skipped as the date dimensions have already been refreshed for today.")

# COMMAND ----------

# DBTITLE 1,Load Time Table
timedimension_df_count = spark.read.format("delta").load(timedimension_table).count()

if timedimension_df_count == 0:

    timedimension_query = f"""
        WITH TimeData AS (
        SELECT
            (HOUR(TIMESTAMPADD(SECOND, t.second_of_day, '1970-01-01 00:00:00')) * 3600) +
            (MINUTE(TIMESTAMPADD(SECOND, t.second_of_day, '1970-01-01 00:00:00')) * 60) +
            SECOND(TIMESTAMPADD(SECOND, t.second_of_day, '1970-01-01 00:00:00')) AS id,
            HOUR(TIMESTAMPADD(SECOND, t.second_of_day, '1970-01-01 00:00:00')) AS hour_of_day,
            MINUTE(TIMESTAMPADD(SECOND, t.second_of_day, '1970-01-01 00:00:00')) AS minute_of_hour,
            SECOND(TIMESTAMPADD(SECOND, t.second_of_day, '1970-01-01 00:00:00')) AS second_of_minute,
            DATE_FORMAT(TIMESTAMPADD(SECOND, t.second_of_day, '1970-01-01 00:00:00'), 'HH:mm:ss') AS time_label
        FROM 
            (SELECT EXPLODE(SEQUENCE(0, 86399, 1)) AS second_of_day) AS t
            )

        INSERT INTO delta.`{timedimension_table}`
        SELECT * FROM TimeData;
    """
    spark.sql(timedimension_query)

    logger.info("Data inserted into the time dimension table.")
else:
    logger.info("Table load skipped as the time dimension table is not empty.")

# COMMAND ----------

# DBTITLE 1,Load Time Interval Table
time_interval_df_count = spark.read.format("delta").load(pbitimeintervals_table).count()

if time_interval_df_count == 0:

    spark.sql(
        f"""
        INSERT OVERWRITE TABLE delta.`{pbitimeintervals_table}`
            (interval, interval_description_en_gb, interval_description_fr_fr, interval_description_de_de)
        VALUES
            (0, 'Today', "Aujourd'hui", 'Heute'),
            (7, '7 Days', '7 Jours', '7 Tage'),
            (14, '14 Days', '14 Jours', '14 Tage'),
            (28, '28 Days', '28 Jours', '28 Tage'),
            (90, '90 Days', '90 Jours', '90 Tage');
        """
    )

    logger.info("Data inserted into the time_intervals table.")
else:
    logger.info("Table load skipped as the time_intervals table is not empty.")

# COMMAND ----------

# DBTITLE 1,Load Value Parameter Table
value_parameter_df_count = spark.read.format("delta").load(pbivaluearameter_table).count()

if value_parameter_df_count == 0:

    values = [(i,) for i in range(101)] 
    
    schema = StructType([
        StructField("value", IntegerType(), True),
        StructField("load_date", TimestampType(), True) 
    ])
    
    parameter_table_df = spark.createDataFrame(values, schema=schema[:-1]) 
    parameter_table_with_timestamp_df = parameter_table_df.withColumn("load_date", current_timestamp())
    parameter_table_with_timestamp_df.createOrReplaceTempView("values")

    spark.sql(f"""
        INSERT OVERWRITE TABLE delta.`{pbivaluearameter_table}`
        SELECT value, load_date FROM values ORDER BY value 
    """)
    
    logger.info("Data inserted into the value_parameter table.")
else:
    logger.info("Table load skipped as the value_parameter table is not empty.")

# COMMAND ----------

# DBTITLE 1,Load Time Parameter Table
time_parameter_df_count = spark.read.format("delta").load(pbitimeparameter_table).count()

if time_parameter_df_count == 0:

    # Generate a DataFrame with a single column containing every 15-minute interval in a day
    time_interval_df = spark.range(0, 96).select(
        expr(
            "concat(lpad(floor(id / 4), 2, '0'), ':', lpad((id % 4) * 15, 2, '0')) as time"
        ),
        current_timestamp().alias("load_date"),
    )
    time_interval_df_ordered = time_interval_df.orderBy("time")
    time_interval_df_ordered.write.format("delta").mode("overwrite").save(pbitimeparameter_table)

    logger.info("Data inserted into the time_parameter table.")
else:
    logger.info("Table load skipped as the time_parameter table is not empty.")

# COMMAND ----------

# DBTITLE 1,Load PBI Labels Table
pbi_labels_df_count = spark.read.format("delta").load(pbilabels_table).count()

if pbi_labels_df_count == 0:

    spark.sql(
        f"""
        INSERT OVERWRITE TABLE delta.`{pbilabels_table}`
            (id, label_en_gb, label_fr_fr, label_de_de, color)
        VALUES
            (1, 'Models', 'Des modèles', 'Modelle', '#008000'),
            (2, 'Potentials', 'Potentiels', 'Potenziale', '#ffa500'),
            (3, 'Resistors', 'Résistances', 'Beständigkeit', '#ff0000')
        """
    )
    logger.info("Data inserted into the pbi_labels table.")
else:
    logger.info("Table load skipped as the pbi_labels table is not empty.")

# COMMAND ----------

# DBTITLE 1,Load KPI Dimension Table
kpi_dimension_df = spark.read.format("delta").load(kpidimension_table)

kpi_data = [
    (0, "comp1", "Compliance 1", "Konformität 1", "Conformité 1", 5, 5),
    (1, "comp2", "Compliance 2", "Konformität 2", "Conformité 2", 5, 5),
    (2, "comp3", "Compliance 3", "Konformität 3", "Conformité 3", 5, 5),
    (3, "comp4", "Compliance 4", "Konformität 4", "Conformité 4", 5, 5),
    (4, "comp5", "Compliance 5", "Konformität 5", "Conformité 5", 5, 5),
    (5, "comp6", "Compliance 6", "Konformität 6", "Conformité 6", 5, 5),
    (6, "comp7", "Compliance 7", "Konformität 7", "Conformité 7", 5, 5),
    (7, "comp8", "Compliance 8", "Konformität 8", "Conformité 8", 5, 5),
    (9, "csat1", "C-Sat 1", "Kundenzufriedenheit 1", "Satisfaction Client 1", 6, 6),
    (10, "csat2", "C-Sat 2", "Kundenzufriedenheit 2", "Satisfaction Client 2", 6, 6),
    (11, "effct13", "Effectiveness 13", "Effektivität 13", "Efficacité 13", 4, 4),
    (12, "effct14", "Effectiveness 14", "Effektivität 14", "Efficacité 14", 4, 4),
    (13, "effct15", "Effectiveness 15", "Effektivität 15", "Efficacité 15", 4, 4),
    (14, "effct1", "Effectiveness 1", "Effektivität 1", "Efficacité 1", 4, 4),
    (15, "effct2", "Effectiveness 2", "Effektivität 2", "Efficacité 2", 4, 4),
    (16, "effct3", "Effectiveness 3", "Effektivität 3", "Efficacité 3", 4, 4),
    (17, "effct4", "Effectiveness 4", "Effektivität 4", "Efficacité 4", 4, 4),
    (18, "effct5", "Effectiveness 5", "Effektivität 5", "Efficacité 5", 4, 4),
    (19, "effct6", "Effectiveness 6", "Effektivität 6", "Efficacité 6", 4, 4),
    (20, "effct9", "Effectiveness 9", "Effektivität 9", "Efficience 9", 4, 4),
    (21, "effcy10", "Efficiency 10", "Effizienz 10", "Efficience 10", 3, 3),
    (22, "effcy12", "Efficiency 12", "Effizienz 12", "Efficience 12", 3, 3),
    (23, "effcy13", "Efficiency 13", "Effizienz 13", "Efficience 13", 3, 3),
    (24, "effcy3", "Efficiency 3", "Effizienz 3", "Efficience 3", 3, 3),
    (25, "effcy4", "Efficiency 4", "Effizienz 4", "Efficience 4", 3, 3),
    (26, "effcy5", "Efficiency 5", "Effizienz 5", "Efficience 5", 3, 3),
    (27, "effcy6", "Efficiency 6", "Effizienz 6", "Efficience 6", 3, 3),
    (28, "effcy7", "Efficiency 7", "Effizienz 7", "Efficience 7", 3, 3),
    (29, "effcy8", "Efficiency 8", "Effizienz 8", "Efficience 8", 3, 3),
    (30, "effcy9", "Efficiency 9", "Effizienz 9", "Efficience 9", 3, 3),
    (31, "prod1", "Productivity 1", "Produktivität 1", "Productivité 1", 1, 1),
    (32, "prod2", "Productivity 2", "Produktivität 2", "Productivité 2", 1, 1),
    (33, "prod3", "Productivity 3", "Produktivität 3", "Productivité 3", 1, 1),
    (34, "prod4", "Productivity 4", "Produktivität 4", "Productivité 4", 1, 1),
    (35, "prod5", "Productivity 5", "Produktivität 5", "Productivité 5", 1, 1),
    (36, "prod6", "Productivity 6", "Produktivität 6", "Productivité 6", 1, 1),
    (37, "util1", "Utilisation 1", "Nutzung 1", "Utilisation 1", 2, 2),
    (38, "util2", "Utilisation 2", "Nutzung 2", "Utilisation 2", 2, 2),
    (39, "util3", "Utilisation 3", "Nutzung 3", "Utilisation 3", 2, 2),
    (40, "util4", "Utilisation 4", "Nutzung 4", "Utilisation 4", 2, 2),
    (41, "util5", "Utilisation 5", "Nutzung 5", "Utilisation 5", 2, 2),
    (42, "csat3", "C-Sat 3", "Kundenzufriedenheit 3", "Satisfaction Client 3", 6, 6),
    (43, "csat4", "C-Sat 4", "Kundenzufriedenheit 4", "Satisfaction Client 4", 6, 6),
    (44, "effcy1", "Efficiency 1", "Effizienz 1", "Efficience 1", 3, 3),
    (45, "effcy2", "Efficiency 2", "Effizienz 2", "Efficience 2", 3, 3),
    (46, "effcy11", "Efficiency 11", "Effizienz 11", "Efficience 11", 3, 3),
    (47, "effct10", "Effectiveness 10", "Effektivität 10", "Efficacité 10", 4, 4),
    (48, "effct11", "Effectiveness 11", "Effektivität 11", "Efficacité 11", 4, 4),
    (49, "effct12", "Effectiveness 12", "Effektivität 12", "Efficacité 12", 4, 4),
    (50, "csat5", "C-Sat 5", "Kundenzufriedenheit 5", "Satisfaction Client 5", 6, 6),
    (51, "csat6", "C-Sat 6", "Kundenzufriedenheit 6", "Satisfaction Client 6", 6, 6)
]

source_df = spark.createDataFrame(
    kpi_data,
    ["id", "kpi_code", "kpi_name_en_gb", "kpi_name_de_de", "kpi_name_fr_fr", "kpa_id", "display_order"],
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{kpidimension_table}` as tgt
    USING source_data as src
        ON tgt.id = src.id 
    WHEN MATCHED AND tgt.kpi_code <> src.kpi_code THEN
        UPDATE SET
            tgt.kpi_code = src.kpi_code,
            tgt.kpi_name_en_gb = src.kpi_name_en_gb,
            tgt.kpi_name_de_de = src.kpi_name_de_de,
            tgt.kpi_name_fr_fr = src.kpi_name_fr_fr,
            tgt.kpa_id = src.kpa_id,
            tgt.display_order = src.display_order
    WHEN NOT MATCHED THEN
        INSERT (id, kpi_code, kpi_name_en_gb, kpi_name_de_de, kpi_name_fr_fr, kpa_id, display_order)
        VALUES (src.id, src.kpi_code, src.kpi_name_en_gb, src.kpi_name_de_de, src.kpi_name_fr_fr, src.kpa_id, src.display_order)
"""

spark.sql(merge_sql)

logger.info("Refreshing KPI dimension completed.")

# COMMAND ----------

# DBTITLE 1,Load KPA Dimension Table
kpa_dimension_df = spark.read.format("delta").load(kpadimension_table)

kpa_data = [
    (1, "prod", "Productivity", "Produktivität", "Productivité", 1),
    (2, "util", "Utilisation", "Nutzung", "Utilisation", 2),
    (3, "effcy", "Efficiency", "Effizienz", "Efficience", 3),
    (4, "effct", "Effectiveness", "Effektivität", "", 4),
    (5, "comp", "Compliance", "Konformität", "Conformité", 5),
    (6, "csat", "C-Sat", "Kundenzufriedenheit", "Satisfaction du client", 6)
]

source_df = spark.createDataFrame(
    kpa_data, ["id", "kpa_code", "kpa_name_en_gb", "kpa_name_de_de", "kpa_name_fr_fr", "display_order"]
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{kpadimension_table}` as tgt
    USING source_data as src
        ON tgt.id = src.id
    WHEN MATCHED AND tgt.kpa_code <> src.kpa_code
        THEN UPDATE SET
            tgt.kpa_name_en_gb = src.kpa_name_en_gb,
            tgt.kpa_name_de_de = src.kpa_name_de_de,
            tgt.kpa_name_fr_fr = src.kpa_name_fr_fr,
            tgt.display_order = src.display_order
    WHEN NOT MATCHED THEN
        INSERT (id, kpa_code, kpa_name_en_gb, kpa_name_de_de, kpa_name_fr_fr, display_order)
        VALUES (src.id, src.kpa_code, src.kpa_name_en_gb, src.kpa_name_de_de, src.kpa_name_fr_fr, src.display_order)
"""

spark.sql(merge_sql)

logger.info("Refreshing KPA dimension completed.")

# COMMAND ----------

# DBTITLE 1,Update Tenant Config Table
tenant_config_df = spark.read.format("delta").load(tenantconfig_table)

tenant_config_data = [
    ('expected_shift_start_time', '08:00:00'),
    ('expected_shift_end_time', '16:00:00'),
    ('min_shift_duration_mins', 60),
    ('max_shift_duration_mins', 720),
    ('expected_shift_duration_mins', 480),
    ('expected_shift_start_time', '08:00:00'),
    ('expected_shift_end_time', '16:00:00'),
    ('min_shift_duration_mins', 60),
    ('max_shift_duration_mins', 720),
    ('expected_shift_duration_mins', 480),
    ('expected_shift_start_to_travel_duration_mins', 15),
    ('expected_shift_end_to_last_event_duration_mins', 45)
]

source_df = spark.createDataFrame(
    tenant_config_data, ["name", "value"]
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{tenantconfig_table}` as tgt
    USING source_data as src
        ON tgt.name = src.name 
        AND tgt.value = src.value
    WHEN NOT MATCHED THEN
        INSERT (name, revision_datetime, value)
        VALUES (src.name, current_timestamp(), src.value)
"""

spark.sql(merge_sql)

logger.info("Refreshing tenant config table completed.")

# COMMAND ----------

# DBTITLE 1,Update KPI Target Config Table
kpi_target_config_df = spark.read.format("delta").load(kpitargetconfig_table)

kpi_target_config_data = [
    (0, 100), (1, 100), (2, 100), (3, 100), (4, 100), (5, 100), (6, 100), (7, 100), (8, 100), (9, 100), (10, 100), 
    (11, 100), (12, 100), (13, 100), (14, 100), (15, 100), (16, 100), (17, 100), (18, 100), (19, 100), (20, 100), 
    (21, '1.5'), (22, '1.5'), (23, 8), (24, 8), (25, 8), (26, 100), (27, 100), (28, 100), (29, 100), (30, 100),
    (31, 100), (32, 100), (33, 100), (34, 100), (35, 100), (36, 100), (37, 100), (38, 100), (39, 100), (40, 100),
    (41, 100), (42, 100), (43, 100), (44, 100), (45, 100), (46, 100), (47, 100), (48, 100), (49, 100), (50, 100), (51, 100)
]

source_df = spark.createDataFrame(
    kpi_target_config_data, ["kpi_id", "target_value"]
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{kpitargetconfig_table}` as tgt
    USING source_data as src
        ON tgt.kpi_id = src.kpi_id 
        AND tgt.target_value = src.target_value
    WHEN NOT MATCHED THEN
        INSERT (kpi_id, revision_datetime, target_value)
        VALUES (src.kpi_id, current_timestamp(), src.target_value)
"""

spark.sql(merge_sql)

logger.info("Refreshing KPI target config table completed.")

# COMMAND ----------

# DBTITLE 1,Update Shift Exclusion Reason Dimension
shift_exclusion_reason_df = spark.read.format("delta").load(shiftexclusionreason_table)

shift_exclusion_reason_data = [
    ('ShiftDurationNegative',),
    ('ShiftDurationTooShort',),
    ('ShiftDurationTooLong',)
]

source_df = spark.createDataFrame(
    shift_exclusion_reason_data, ["reason_code"]
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{shiftexclusionreason_table}` as tgt
    USING source_data as src
        ON tgt.reason_code = src.reason_code 
    WHEN NOT MATCHED THEN
        INSERT (reason_code, load_date)
        VALUES (src.reason_code, current_timestamp())
"""

spark.sql(merge_sql)

logger.info("Refreshing shift exclusion reason dictionary table completed.")

# COMMAND ----------

# DBTITLE 1,Update KPI Dimension Operational
kpi_dimension_operational_df_count = spark.read.format("delta").load(pbikpidimensionoperational_table).count()

if kpi_dimension_operational_df_count == 0:

    spark.sql(
        f"""
        INSERT OVERWRITE TABLE delta.`{pbikpidimensionoperational_table}`
            (kpi_id, kpi_code, kpi_name_en, kpi_name_de, display_order, load_date)
        VALUES
            (NULL, 'big1', 'Resource', NULL, 1, CURRENT_TIMESTAMP()),
            (67, 'comp1', 'Geofence Accuracy', NULL, 2, CURRENT_TIMESTAMP()),
            (68, 'comp2', 'GPS Validity', NULL, 3, CURRENT_TIMESTAMP()),
            (NULL, 'empty1', 'empty1', NULL, 4, CURRENT_TIMESTAMP()),
            (26, 'effcy4', 'Travel Duration (within 50%)', NULL, 5, CURRENT_TIMESTAMP()),
            (25, 'effcy3', 'Task Duration (within 50%)', NULL, 6, CURRENT_TIMESTAMP()),
            (NULL, 'big2', 'Plan', NULL, 7, CURRENT_TIMESTAMP()),
            (0, 'prod1', 'Productivity (Inc Travel)', NULL, 8, CURRENT_TIMESTAMP()),
            (NULL, 'empty2', 'empty2', NULL, 9, CURRENT_TIMESTAMP()),
            (NULL, 'empty3', 'empty3', NULL, 10, CURRENT_TIMESTAMP()),
            (NULL, 'empty4', 'empty4', NULL, 11, CURRENT_TIMESTAMP()),
            (NULL, 'empty5', 'empty5', NULL, 12, CURRENT_TIMESTAMP()),
            (NULL, 'empty6', 'empty6', NULL, 13, CURRENT_TIMESTAMP()),
            (27, 'effcy5', 'Velocity (Tasks per hour)', NULL, 14, CURRENT_TIMESTAMP()),
            (11, 'util1', 'Utilisation (Inc Travel)', NULL, 15, CURRENT_TIMESTAMP()),
            (NULL, 'empty7', 'empty7', NULL, 16, CURRENT_TIMESTAMP()),
            (NULL, 'empty8', 'empty8', NULL, 17, CURRENT_TIMESTAMP()),
            (NULL, 'empty9', 'empty9', NULL, 18, CURRENT_TIMESTAMP()),
            (NULL, 'empty10', 'empty10', NULL, 19, CURRENT_TIMESTAMP()),
            (NULL, 'empty11', 'empty10', NULL, 20, CURRENT_TIMESTAMP()),
            (28, 'effcy6', 'Velocity (Paid Shift)', NULL, 21, CURRENT_TIMESTAMP()),
            (87, 'csat3', 'Response Rate', NULL, 22, CURRENT_TIMESTAMP()),
            (NULL, 'empty12', 'empty12', NULL, 23, CURRENT_TIMESTAMP()),
            (NULL, 'empty13', 'empty13', NULL, 24, CURRENT_TIMESTAMP()),
            (NULL, 'empty14', 'empty14', NULL, 25, CURRENT_TIMESTAMP()),
            (NULL, 'empty15', 'empty15', NULL, 26, CURRENT_TIMESTAMP()),
            (NULL, 'empty16', 'empty16', NULL, 27, CURRENT_TIMESTAMP()),
            (52, 'effct10', 'No Access', NULL, 28, CURRENT_TIMESTAMP()),
            (47, 'effct5', 'SLA Pass', NULL, 29, CURRENT_TIMESTAMP()),
            (NULL, 'empty17', 'empty17', NULL, 30, CURRENT_TIMESTAMP()),
            (NULL, 'empty18', 'empty18', NULL, 31, CURRENT_TIMESTAMP()),
            (NULL, 'empty19', 'empty19', NULL, 32, CURRENT_TIMESTAMP()),
            (NULL, 'empty20', 'empty20', NULL, 33, CURRENT_TIMESTAMP()),
            (NULL, 'empty21', 'empty21', NULL, 34, CURRENT_TIMESTAMP()),
            (53, 'effct11', 'Cancelled Visits', NULL, 35, CURRENT_TIMESTAMP()),
            (NULL, 'big3', 'Customer', NULL, 36, CURRENT_TIMESTAMP()),
            (85, 'csat1', 'Net Promoter Score', NULL, 37, CURRENT_TIMESTAMP()),
            (86, 'csat2', 'NPS Capture Rate', NULL, 38, CURRENT_TIMESTAMP()),
            (NULL, 'empty22', 'empty22', NULL, 39, CURRENT_TIMESTAMP()),
            (49, 'effct7', 'Revisit Rate', NULL, 40, CURRENT_TIMESTAMP()),
            (43, 'effct1', 'FTF Rate', NULL, 41, CURRENT_TIMESTAMP()),
            (NULL, 'big4', 'Task', NULL, 42, CURRENT_TIMESTAMP())
        """
    )

    logger.info("Data inserted into the kpi_dimension_operational table.")
else:
    logger.info("Table load skipped as the kpi_dimension_operational table is not empty.")


# COMMAND ----------

# DBTITLE 1,Update Activity Type Dimension
activitytype_df = spark.read.format("delta").load(activitytypedimension_table)

activitytype_data = [
    (0, 'SHIFT', 'SHIFT', 'PRODUCTIVE','PRODUCTIVE'),
    (1, 'TRAVEL_TASK', 'TRAVEL_TASK', 'PRODUCTIVE-TRAVEL', 'PRODUCTIVE'),
    (2, 'TRANSITION', 'ADMIN', 'PRODUCTIVE', 'PRODUCTIVE'),
    (3, 'WORK', 'WORK', 'PRODUCTIVE', 'PRODUCTIVE'),
    (4, 'TRAVEL_HOME', 'TRAVEL_HOME', 'PRODUCTIVE-TRAVEL', 'PRODUCTIVE'),
    (5, 'VEHICLE_CHECK', 'VEHICLE_CHECK', 'PRODUCTIVE', 'PRODUCTIVE'),
    (6, 'DUTY', 'DUTY', 'NON-PRODUCTIVE', 'NON-PRODUCTIVE'),
    (7, 'TRAVEL_DUTY', 'TRAVEL_DUTY', 'NON-PRODUCTIVE', 'NON-PRODUCTIVE')
]

source_df = spark.createDataFrame(
    activitytype_data,
    ["id", "activity_type", "timeline_type", "productivity_type", "productivity_type_group"],
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{activitytypedimension_table}` as tgt
    USING source_data as src
        ON tgt.id = src.id 
    WHEN MATCHED AND tgt.activity_type <> src.activity_type THEN
        UPDATE SET
            tgt.activity_type = src.activity_type,
            tgt.timeline_type = src.timeline_type,
            tgt.productivity_type = src.productivity_type,
            tgt.productivity_type_group = src.productivity_type_group,
            tgt.load_date = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (id, activity_type, timeline_type, productivity_type, productivity_type_group, load_date)
        VALUES (src.id, src.activity_type, src.timeline_type, src.productivity_type, productivity_type_group, CURRENT_TIMESTAMP())
"""

spark.sql(merge_sql)

logger.info("Refreshing Activity Type Dimension completed.")

# COMMAND ----------

# DBTITLE 1,Update Translations Table
translations_df = spark.read.format("delta").load(pbitranslations_table)

translations_data = [
    ("en", "English", "en-GB", 1),
    ("fr", "French", "fr-FR", 2),
    ("de", "German", "de-DE", 3)
]

source_df = spark.createDataFrame(
    translations_data,
    ["language_id", "language", "user_culture", "display_order"],
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{pbitranslations_table}` as tgt
    USING source_data as src
        ON tgt.language_id = src.language_id 
    WHEN MATCHED THEN
        UPDATE SET
            tgt.language = src.language,
            tgt.user_culture = src.user_culture,
            tgt.display_order = src.display_order
    WHEN NOT MATCHED THEN
        INSERT (language_id, language, user_culture, display_order)
        VALUES (src.language_id, src.language, src.user_culture, src.display_order) 
"""

spark.sql(merge_sql)

logger.info("Refreshing Translations Table completed.")

# COMMAND ----------

# DBTITLE 1,Update Timeline Context Type Dimension
timelinecontexttypedimension_df = spark.read.format("delta").load(timelinecontexttypedimension_table)

current_dt = datetime.now()

timelinecontexttypedimension_data = [
    (1, "Planned Work (Static)", 1, current_dt),
    (2, "Planned Work (Dynamic)", 2, current_dt),
    (3, "Actual Work", 3, current_dt)
]

source_df = spark.createDataFrame(
    timelinecontexttypedimension_data,
    ["id", "timeline_context_type", "display_order", "load_date"],
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{timelinecontexttypedimension_table}` as tgt
    USING source_data as src
        ON tgt.id = src.id 
    WHEN MATCHED 
        AND tgt.timeline_context_type != src.timeline_context_type OR tgt.display_order != src.display_order
        THEN UPDATE SET
            tgt.timeline_context_type = src.timeline_context_type,
            tgt.display_order = src.display_order,
            tgt.load_date = src.load_date
    WHEN NOT MATCHED THEN
        INSERT (id, timeline_context_type, display_order, load_date)
        VALUES (src.id, src.timeline_context_type, src.display_order, CURRENT_TIMESTAMP()) 
"""

spark.sql(merge_sql)

logger.info("Refreshing Timeline Context Type Dimension completed.")

# COMMAND ----------

# DBTITLE 1,Update Task Status Dimension Table
current_dt = datetime.now()

taskstatus_data = [
    (1, None, "RAISE", "Raise", 99, False, current_dt),
    (2, "INITIAL", "INITIAL", "Initial", 99, False, current_dt),
    (3, "INITIAL", "PLANNING", "Planning", 99, False, current_dt),
    (4, "INITIAL", "OUT_OF_PLAN", "Out of Plan", 99, False, current_dt),
    (5, "COMMITTED", "COMMIT", "Commit", 1, True, current_dt),
    (6, "COMMITTED", "START_TRAVEL", "Started Travel", 4, True, current_dt),
    (7, "COMMITTED", "ARRIVED", "Arrived", 5, True, current_dt),
    (8, "COMMITTED", "START_WORK", "Started Work", 6, True, current_dt),
    (9, "CLOSED", "FIXED", "Fixed", 7, True, current_dt),
    (10, "CLOSED", "FIXED_NOT_FIRST_TIME", "Fixed Not First Time", 99, False, current_dt),
    (11, "CLOSED", "FURTHER_VISIT_REQUIRED", "Further Visit Required", 10, True, current_dt),
    (12, "CLOSED", "FIXED_FIRST_TIME", "Fixed First Time", 8, True, current_dt),
    (13, "CLOSED", "CLOSED_PENDING_OUTCOME", "Closed Pending Outcome", 99, False, current_dt),
    (14, "CLOSED", "CLOSED", "Closed", 99, False, current_dt),
    (15, "CLOSED", "ABORT", "Aborted", 12, True, current_dt),
    (16, "COMMITTED", "ACKNOWLEDGED", "Acknowledged", 2, True, current_dt),
    (17, "CLOSED", "BROKE_APPOINTMENT", "Broke Appointment", 99, False, current_dt),
    (18, "CLOSED", "CANCEL", "Cancelled", 14, True, current_dt),
    (19, "COMMITTED", "CONTACTED", "Contacted", 3, True, current_dt),
    (20, "CLOSED", "NO_ACCESS", "No Access", 11, True, current_dt),
    (21, None, "SUCCESS", "Success", 99, False, current_dt),
    (22, "RAISED", "UNCOMMIT", "Uncommit", 99, False, current_dt),
    (23, "COMMITTED", "SUSPEND", "Suspended", 99, False, current_dt),
    (24, "CLOSED", "ATTENDED", "Attended", 13, True, current_dt),
]

source_df = spark.createDataFrame(
    taskstatus_data,
    ["id", "status", "detailed_status", "detailed_status_en_gb", "display_order", "include_in_report", "load_date"],
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{taskstatusdimension_table}` as tgt
    USING source_data as src
        ON tgt.id = src.id
    WHEN MATCHED 
        AND (
            tgt.status != src.status OR
            tgt.detailed_status != src.detailed_status OR
            tgt.detailed_status_en_gb != src.detailed_status_en_gb OR
            tgt.display_order != src.display_order OR
            tgt.include_in_report != src.include_in_report
        )
    THEN UPDATE SET
        tgt.status = src.status,
        tgt.detailed_status = src.detailed_status,
        tgt.detailed_status_en_gb = src.detailed_status_en_gb,
        tgt.display_order = src.display_order,
        tgt.include_in_report = src.include_in_report,
        tgt.load_date = src.load_date
    WHEN NOT MATCHED THEN
        INSERT (id, status, detailed_status, detailed_status_en_gb, display_order, include_in_report, load_date)
        VALUES (src.id, src.status, src.detailed_status, src.detailed_status_en_gb, src.display_order, src.include_in_report, CURRENT_TIMESTAMP())
"""

spark.sql(merge_sql)

logger.info("Refreshing Task Status Dimension table completed.")

# COMMAND ----------

# DBTITLE 1,Update Task Status Group Dimension Table

merge_sql = f"""
    MERGE INTO delta.`{taskstatusgroupdimension_table}` AS tgt
    USING (
        SELECT 
            id,
            status,
            detailed_status,
            detailed_status_en_gb,
            CASE 
                WHEN status = 'CLOSED' AND detailed_status != 'CANCEL' THEN 'CLOSED'
                WHEN status = 'CLOSED' AND detailed_status = 'CANCEL' THEN 'CANCELLED'
                ELSE 'OPEN'
            END AS status_group,
            CASE 
                WHEN status = 'CLOSED' AND detailed_status != 'CANCEL' THEN 'Closed Tasks'
                WHEN status = 'CLOSED' AND detailed_status = 'CANCEL' THEN 'Cancelled Tasks'
                ELSE 'Open Tasks'
            END AS status_group_en_gb,
            include_in_report,
            display_order,
            CURRENT_TIMESTAMP() AS load_date
        FROM 
            delta.`{taskstatusdimension_table}`
        
        UNION ALL
        
        SELECT 
            (SELECT MAX(id) FROM delta.`{taskstatusdimension_table}`) + 1, 
            'WORKER_LOCATION', 
            'WORKER_LOCATION', 
            'Worker Location',
            'WORKER_LOCATION',
            'Worker',
            true, 
            (SELECT MAX(display_order) FROM delta.`{taskstatusdimension_table}`) + 1, 
            CURRENT_TIMESTAMP()
    ) AS src
    ON tgt.id = src.id
    WHEN MATCHED 
        AND (
            tgt.status != src.status OR
            tgt.detailed_status != src.detailed_status OR
            tgt.detailed_status_en_gb != src.detailed_status_en_gb OR
            tgt.status_group != src.status_group OR 
            tgt.status_group_en_gb != src.status_group_en_gb OR 
            tgt.display_order != src.display_order OR
            tgt.include_in_report != src.include_in_report 
        )
    THEN 
        UPDATE SET
            tgt.status = src.status,
            tgt.detailed_status = src.detailed_status,
            tgt.detailed_status_en_gb = src.detailed_status_en_gb,
            tgt.status_group = src.status_group,
            tgt.status_group_en_gb = src.status_group_en_gb,
            tgt.display_order = src.display_order,
            tgt.include_in_report = src.include_in_report,
            tgt.load_date = src.load_date
    WHEN NOT MATCHED THEN
        INSERT (id, status, detailed_status, detailed_status_en_gb, status_group, status_group_en_gb, display_order, include_in_report, load_date)
        VALUES (src.id, src.status, src.detailed_status, src.detailed_status_en_gb, src.status_group, src.status_group_en_gb, src.display_order, src.include_in_report, src.load_date)
"""

spark.sql(merge_sql)

logger.info("Refreshing Task Status Group Dimension table completed.")


# COMMAND ----------


# DBTITLE 1,Update PBI Worker Shift Header Table
current_dt = datetime.now()

pbiworkershiftsheader_data = [
    ('Shift Start', 'Overall', 1, current_dt),
    ('Shift End', 'Overall', 2, current_dt),
    ('Shift Duration', 'Overall', 3, current_dt),
    ('Overall', 'Overall', 4, current_dt),
    ('prod', 'Overall', 5, current_dt),
    ('util', 'Overall', 6, current_dt),
    ('effcy', 'Overall', 7, current_dt),
    ('effct', 'Overall', 8, current_dt),
    ('comp', 'Overall', 9, current_dt),
    ('csat', 'Overall', 10, current_dt),
    ('Shift Exclusion', 'Overall', 11, current_dt),
    ('Shift Start', 'prod', 12, current_dt),
    ('Shift End', 'prod', 13, current_dt),
    ('Shift Duration', 'prod', 14, current_dt),
    ('Productivity', 'prod', 15, current_dt),
    ('prod1', 'prod', 16, current_dt),
    ('prod2', 'prod', 17, current_dt),
    ('prod3', 'prod', 18, current_dt),
    ('Shift Exclusion', 'prod', 19, current_dt),
    ('Shift Start', 'util', 20, current_dt),
    ('Shift End', 'util', 21, current_dt),
    ('Shift Duration', 'util', 22, current_dt),
    ('Utilisation', 'util', 23, current_dt),
    ('util2', 'util', 24, current_dt),
    ('util4', 'util', 25, current_dt),
    ('Shift Exclusion', 'util', 26, current_dt)
]

source_df = spark.createDataFrame(
    pbiworkershiftsheader_data,
    ["name", "group", "display_order", "load_date"]
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{pbiworkershiftsheader_table}` as tgt
    USING source_data as src
        ON tgt.name = src.name
        AND tgt.group = src.group
    WHEN MATCHED 
        AND tgt.display_order != src.display_order
    THEN UPDATE SET
        tgt.display_order = src.display_order
    WHEN NOT MATCHED THEN
        INSERT (name, group, display_order, load_date)
        VALUES (src.name, src.group, src.display_order, CURRENT_TIMESTAMP())
"""

spark.sql(merge_sql)

logger.info("Refreshing PBI Worker Shift Header table completed.")

# COMMAND ----------

# DBTITLE 1,Update PBI PERT Chart Relationships Table

pbipertchartrelationships_data = [
    (1, 'shelved', None, None, None, None),
    (2, 'shelved', 'committed', 'dashed', '#cbcdd1', 2),
    (3, 'committed', None, None, None, None),
    (4, 'committed', 'shelved', 'dashed', '#cbcdd1', 2),
    (5, 'committed', 'acknowledged', 'solid', '#cbcdd1', 2),
    (6, 'committed', 'cancelled', 'dashed', '#cbcdd1', 2),
    (7, 'acknowledged', None, None, None, None),
    (8, 'acknowledged', 'contacted', 'solid', '#cbcdd1', 2),
    (9, 'acknowledged', 'cancelled', 'dashed', '#cbcdd1', 2),
    (10, 'contacted', None, None, None, None),
    (11, 'contacted', 'started travel', 'solid', '#cbcdd1', 2),
    (12, 'contacted', 'cancelled', 'dashed', '#cbcdd1', 2),
    (13, 'cancelled', None, None, None, None),
    (14, 'started travel', None, None, None, None),
    (15, 'started travel', 'arrived', 'solid', '#cbcdd1', 2),
    (16, 'started travel', 'aborted', 'dashed', '#cbcdd1', 2),
    (17, 'aborted', None, None, None, None),
    (18, 'suspended', None, None, None, None),
    (19, 'suspended', 'started travel', 'dashed', '#cbcdd1', 2),
    (20, 'no access', None, None, None, None),
    (21, 'arrived', None, None, None, None),
    (22, 'arrived', 'started work', 'solid', '#cbcdd1', 2),
    (23, 'arrived', 'no access', 'dashed', '#cbcdd1', 2),
    (24, 'started work', None, None, None, None),
    (25, 'started work', 'suspended', 'dashed', '#cbcdd1', 2),
    (26, 'started work', 'further visit required', 'dashed', '#cbcdd1', 2),
    (27, 'started work', 'fixed first time', 'solid', '#cbcdd1', 2),
    (28, 'started work', 'fixed further visit', 'dashed', '#cbcdd1', 2),
    (29, 'started work', 'transitioning out', 'solid', '#cbcdd1', 2),
    (30, 'further visit required', None, None, None, None),
    (31, 'working', None, None, None, None),
    (32, 'available', None, None, None, None),
    (33, 'fixed first time', None, None, None, None),
    (34, 'fixed further visit', None, None, None, None),
    (35, 'transitioning in', None, None, None, None),
    (36, 'transitioning in', 'started work', 'solid', '#cbcdd1', 2),
    (37, 'transitioning out', None, None, None, None),
    (38, 'transitioning out', 'menu', 'solid', '#cbcdd1', 2),
    (39, 'travelling', None, None, None, None),
    (40, 'travelling', 'transitioning in', 'solid', '#cbcdd1', 2),
    (41, 'menu', None, None, None, None),
    (42, 'menu', 'travelling', 'solid', '#cbcdd1', 2),
    (43, 'menu', 'idle', 'dashed', '#cbcdd1', 2),
    (44, 'menu', 'travelling home', 'solid', '#cbcdd1', 2),
    (45, 'menu', 'vehicle check', 'dashed', '#cbcdd1', 2),
    (46, 'menu', 'travelling duty', 'dashed', '#cbcdd1', 2),
    (47, 'idle', None, None, None, None),
    (48, 'idle', 'menu', 'dashed', '#cbcdd1', 2),
    (49, 'vehicle check', None, None, None, None),
    (50, 'vehicle check', 'menu', 'dashed', '#cbcdd1', 2),
    (51, 'travelling home', None, None, None, None),
    (52, 'travelling home', 'closed shifts', 'solid', '#cbcdd1', 2),
    (53, 'closed shifts', None, None, None, None),
    (54, 'travelling duty', None, None, None, None),
    (55, 'travelling duty', 'duty', 'dashed', '#cbcdd1', 2),
    (56, 'duty', None, None, None, None),
    (57, 'expected', None, None, None, None),
    (58, 'expected', 'menu', 'solid', '#cbcdd1', 2),
    (59, 'not required', None, None, None, None),
    (60, 'not required', 'expected', 'solid', '#cbcdd1', 2),
    (61, 'excluded shifts', None, None, None, None)
]

source_df = spark.createDataFrame(
    pbipertchartrelationships_data,
    ["id", "source", "target", "style", "colour", "thickness"]
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{pbipertchartrelationships_table}` as tgt
    USING source_data as src
        ON tgt.id = src.id
    WHEN NOT MATCHED THEN
        INSERT (id, source, target, style, colour, thickness, load_date)
        VALUES (src.id, src.source, src.target, src.style, src.colour, src.thickness, CURRENT_TIMESTAMP())
"""

spark.sql(merge_sql)

logger.info("Refreshing PBI PERT Chart Relationships table completed.")

# COMMAND ----------

# DBTITLE 1,Update PBI Translated Localized Labels Table

current_dt = datetime.now()

translatedlocalizedlabels_data = [
    (1, 'en-GB', 'Overall Score & Variance (by week)', 'bar_chart', current_dt),
    (2, 'en-GB', 'Overall Score', 'overall_card', current_dt),
    (3, 'en-GB', 'Variance', 'variance_card', current_dt),
    (4, 'en-GB', 'League Table', 'league_table', current_dt),
    (5, 'en-GB', 'Performance Distribution', 'distribution_chart', current_dt),
    (6, 'en-GB', 'League Table', 'title', current_dt),
    (7, 'en-GB', 'Productivity', 'overall_card_productivity', current_dt),
    (8, 'en-GB', 'Utilisation', 'overall_card_utilisation', current_dt),
    (9, 'en-GB', 'Efficiency', 'overall_card_efficiency', current_dt),
    (10, 'en-GB', 'Effectiveness', 'overall_card_effectiveness', current_dt),
    (11, 'en-GB', 'Compliance', 'overall_card_compliance', current_dt),
    (12, 'en-GB', 'Customer Satisfaction', 'overall_card_csat', current_dt),
    (13, 'en-GB', 'Today', 'time_interval_selection_0_days', current_dt),
    (14, 'en-GB', '7 Days', 'time_interval_selection_7_days', current_dt),
    (15, 'en-GB', '14 Days', 'time_interval_selection_14_days', current_dt),
    (16, 'en-GB', '28 Days', 'time_interval_selection_28_days', current_dt),
    (17, 'en-GB', '90 Days', 'time_interval_selection_90_days', current_dt),
    (18, 'en-GB', 'by week', 'week_label', current_dt),
    (19, 'en-GB', 'Overall Score', 'overall_button', current_dt),
    (20, 'en-GB', 'Productivity', 'productivity_button', current_dt),
    (21, 'en-GB', 'Utilisation', 'utilisation_button', current_dt),
    (22, 'en-GB', 'Efficiency', 'efficiency_button', current_dt),
    (23, 'en-GB', 'Effectiveness', 'effectiveness_button', current_dt),
    (24, 'en-GB', 'Compliance', 'compliance_button', current_dt),
    (25, 'en-GB', 'C-Sat', 'customer_satisfaction_button', current_dt),
    (26, 'de-DE', 'Gesamtbewertung und Varianz (pro Woche)', 'bar_chart', current_dt),
    (27, 'de-DE', 'Gesamtbewertung', 'overall_card', current_dt),
    (28, 'de-DE', 'Varianz', 'variance_card', current_dt),
    (29, 'de-DE', 'Ligatabelle', 'league_table', current_dt),
    (30, 'de-DE', 'Leistungs Verteilung', 'distribution_chart', current_dt),
    (31, 'de-DE', 'Ligatabelle', 'title', current_dt),
    (32, 'de-DE', 'Produktivität', 'overall_card_productivity', current_dt),
    (33, 'de-DE', 'Nutzung', 'overall_card_utilisation', current_dt),
    (34, 'de-DE', 'Effizienz', 'overall_card_efficiency', current_dt),
    (35, 'de-DE', 'Effektivität', 'overall_card_effectiveness', current_dt),
    (36, 'de-DE', 'Konformität', 'overall_card_compliance', current_dt),
    (37, 'de-DE', 'Kundenzufriedenheit', 'overall_card_csat', current_dt),
    (38, 'de-DE', 'Heute', 'time_interval_selection_0_days', current_dt),
    (39, 'de-DE', '7 Tage', 'time_interval_selection_7_days', current_dt),
    (40, 'de-DE', '14 Tage', 'time_interval_selection_14_days', current_dt),
    (41, 'de-DE', '28 Tage', 'time_interval_selection_28_days', current_dt),
    (42, 'de-DE', '90 Tage', 'time_interval_selection_90_days', current_dt),
    (43, 'de-DE', 'pro Woche', 'week_label', current_dt),
    (44, 'de-DE', 'Gesamtbewertung', 'overall_button', current_dt),
    (45, 'de-DE', 'Produktivität', 'productivity_button', current_dt),
    (46, 'de-DE', 'Nutzung', 'utilisation_button', current_dt),
    (47, 'de-DE', 'Effizienz', 'efficiency_button', current_dt),
    (48, 'de-DE', 'Effektivität', 'effectiveness_button', current_dt),
    (49, 'de-DE', 'Konformität', 'compliance_button', current_dt),
    (50, 'de-DE', 'Kundenzufriedenheit', 'customer_satisfaction_button', current_dt),
    (51, 'fr-FR', 'Score global et variance (par semaine)', 'bar_chart', current_dt),
    (52, 'fr-FR', 'Score Global', 'overall_card', current_dt),
    (53, 'fr-FR', 'Variance', 'variance_card', current_dt),
    (54, 'fr-FR', 'Tableau de la ligue', 'league_table', current_dt),
    (55, 'fr-FR', 'Répartition des performances', 'distribution_chart', current_dt),
    (56, 'fr-FR', 'Tableau de la ligue', 'title', current_dt),
    (57, 'fr-FR', 'Productivité', 'overall_card_productivity', current_dt),
    (58, 'fr-FR', 'Utilisation', 'overall_card_utilisation', current_dt),
    (59, 'fr-FR', 'Efficience', 'overall_card_efficiency', current_dt),
    (60, 'fr-FR', 'Efficacité', 'overall_card_effectiveness', current_dt),
    (61, 'fr-FR', 'Conformité', 'overall_card_compliance', current_dt),
    (62, 'fr-FR', 'Satisfaction du client', 'overall_card_csat', current_dt),
    (63, 'fr-FR', 'Aujourd''hui', 'time_interval_selection_0_days', current_dt),
    (64, 'fr-FR', '7 Jours', 'time_interval_selection_7_days', current_dt),
    (65, 'fr-FR', '14 Jours', 'time_interval_selection_14_days', current_dt),
    (66, 'fr-FR', '28 Jours', 'time_interval_selection_28_days', current_dt),
    (67, 'fr-FR', '90 Jours', 'time_interval_selection_90_days', current_dt),
    (68, 'fr-FR', 'par semaine', 'week_label', current_dt),
    (69, 'fr-FR', 'Score Global', 'overall_button', current_dt),
    (70, 'fr-FR', 'Productivité', 'productivity_button', current_dt),
    (71, 'fr-FR', 'Utilisation', 'utilisation_button', current_dt),
    (72, 'fr-FR', 'Efficience', 'efficiency_button', current_dt),
    (73, 'fr-FR', 'Efficacité', 'effectiveness_button', current_dt),
    (74, 'fr-FR', 'Conformité', 'compliance_button', current_dt),
    (75, 'fr-FR', 'Satisfaction du client', 'customer_satisfaction_button', current_dt)
]

source_df = spark.createDataFrame(
    translatedlocalizedlabels_data,
    ["id", "user_culture", "translation", "used_for", "load_date"]
)
source_df.createOrReplaceTempView("source_data")

merge_sql = f"""
    MERGE INTO delta.`{pbitranslatedlocalizedlabels_table}` as tgt
    USING source_data as src
        ON tgt.id = src.id
    WHEN MATCHED AND 
        (tgt.user_culture != src.user_culture OR
        tgt.translation != src.translation OR
        tgt.used_for != src.used_for)
    THEN 
        UPDATE SET 
            tgt.user_culture = src.user_culture,
            tgt.translation = src.translation,
            tgt.used_for = src.used_for,
            tgt.load_date = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED 
    THEN 
        INSERT (id, user_culture, translation, used_for, load_date)
        VALUES (src.id, src.user_culture, src.translation, src.used_for, CURRENT_TIMESTAMP());
"""

spark.sql(merge_sql)

logger.info("Refreshing PBI Translated Localized Labels table completed.")
# COMMAND ----------

dbutils.notebook.exit("Reference tables loaded correctly")
