import sys
import io
import psycopg2
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# ──────────────────────────────────────────────────────────────
# INIT
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'year', 'month', 'day'])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Spark Configs ─────────────────────────────────────────────
spark.conf.set("spark.sql.shuffle.partitions",            "20")
spark.conf.set("spark.sql.parquet.mergeSchema",           "false")
spark.conf.set("spark.sql.parquet.filterPushdown",        "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",    "209715200")  # 200MB
spark.conf.set("spark.sql.broadcastTimeout",              "600")

# ── Parameters ────────────────────────────────────────────────
year  = args['year']
month = args['month']
day   = args['day']

# Padded for safe date construction
month_p = month.zfill(2)
day_p   = day.zfill(2)
log_date_str = f"{year}-{month_p}-{day_p}"

print(f"=" * 60)
print(f"ETL Start: {log_date_str}")
print(f"=" * 60)

# ──────────────────────────────────────────────────────────────
# POSTGRESQL CONFIG
# ──────────────────────────────────────────────────────────────
PG_HOST = "10.88.32.172"
PG_PORT = "5432"
PG_DB   = "networklogs"
PG_USER = "glue_user"
PG_PASS = "user"

# ──────────────────────────────────────────────────────────────
# S3 PATHS
# ──────────────────────────────────────────────────────────────
VPC_BASE  = "s3://ist-network-intelligence-vpcflowlogs-curated/vpcflowlogs/"
VPC_PATH  = f"{VPC_BASE}*/*/year={year}/month={month}/day={day}/"

R53_BASE  = "s3://ist-network-intelligence-vpcflowlogs-curated/vpcdnsquerylogs-curated/"
R53_PATH  = f"{R53_BASE}*/*/year={year}/month={month}/day={day}/"

META_BASE = "s3://ist-network-intelligence-vpcflowlogs-curated/meta_data_tables/"

# ──────────────────────────────────────────────────────────────
# WRITE HELPER — chunked copy_expert (avoids OOM)
# ──────────────────────────────────────────────────────────────
def write_to_postgres(df, table_name, columns, chunk_size=50000):
    """Write DataFrame to PostgreSQL using COPY with chunked buffering."""

    col_str   = ", ".join(columns)
    _host     = PG_HOST
    _port     = PG_PORT
    _db       = PG_DB
    _user     = PG_USER
    _pass     = PG_PASS
    _chunk    = chunk_size

    def write_partition(partition):
        conn = psycopg2.connect(
            host=_host, port=_port,
            dbname=_db, user=_user, password=_pass
        )
        cur    = conn.cursor()
        buffer = io.StringIO()
        count  = 0

        for row in partition:
            buffer.write("\t".join([
                "" if v is None else str(v)
                for v in row
            ]) + "\n")
            count += 1

            # Flush every chunk_size rows to avoid OOM
            if count >= _chunk:
                buffer.seek(0)
                cur.copy_expert(
                    f"COPY public.{table_name} ({col_str}) FROM STDIN WITH (FORMAT TEXT, NULL '')",
                    buffer
                )
                conn.commit()
                buffer = io.StringIO()
                count  = 0

        # Write remaining rows
        if count > 0:
            buffer.seek(0)
            cur.copy_expert(
                f"COPY public.{table_name} ({col_str}) FROM STDIN WITH (FORMAT TEXT, NULL '')",
                buffer
            )
            conn.commit()

        cur.close()
        conn.close()

    df.select(columns).coalesce(10).rdd.foreachPartition(write_partition)
    print(f"  ✅ Written to: {table_name}")


# ──────────────────────────────────────────────────────────────
# STEP 1: READ ALL SOURCES
# ──────────────────────────────────────────────────────────────
print("\n[1/3] Reading S3 sources...")

# VPC Flow Logs
vpc_df = spark.read \
    .option("mergeSchema", "false") \
    .option("basePath",    VPC_BASE) \
    .parquet(VPC_PATH)

# Add log_date + IP integer columns
vpc_df = vpc_df \
    .withColumn("log_date",
        F.to_date(F.lit(log_date_str), "yyyy-MM-dd")
    ) \
    .withColumn("src_ip_int",
        F.conv(
            F.concat(
                F.lpad(F.hex(F.split(F.col("srcaddr"), r"\.")[0].cast("int")), 2, "0"),
                F.lpad(F.hex(F.split(F.col("srcaddr"), r"\.")[1].cast("int")), 2, "0"),
                F.lpad(F.hex(F.split(F.col("srcaddr"), r"\.")[2].cast("int")), 2, "0"),
                F.lpad(F.hex(F.split(F.col("srcaddr"), r"\.")[3].cast("int")), 2, "0")
            ), 16, 10
        ).cast("long")
    ) \
    .withColumn("dst_ip_int",
        F.conv(
            F.concat(
                F.lpad(F.hex(F.split(F.col("dstaddr"), r"\.")[0].cast("int")), 2, "0"),
                F.lpad(F.hex(F.split(F.col("dstaddr"), r"\.")[1].cast("int")), 2, "0"),
                F.lpad(F.hex(F.split(F.col("dstaddr"), r"\.")[2].cast("int")), 2, "0"),
                F.lpad(F.hex(F.split(F.col("dstaddr"), r"\.")[3].cast("int")), 2, "0")
            ), 16, 10
        ).cast("long")
    ) \
    .dropna(subset=["srcaddr", "dstaddr", "account_id"])

# Route53 Logs
r53_df = spark.read \
    .option("mergeSchema", "false") \
    .option("basePath",    R53_BASE) \
    .parquet(R53_PATH) \
    .select(
        "account_id", "region", "vpc_id",
        "srcaddr", "srcport", "srcids",
        "query_name", "query_type", "query_timestamp",
        "answers", "rcode", "transport", "version",
        "year", "month", "day"
    )

# Metadata — all small, will be broadcast
ec2_df      = spark.read.option("mergeSchema", "false").parquet(f"{META_BASE}ec2_details_parquet/")
geo_blk_df  = spark.read.option("mergeSchema", "false").parquet(f"{META_BASE}geo_country_blocks_range_parquet/")
geo_loc_df  = spark.read.option("mergeSchema", "false").parquet(f"{META_BASE}geo_country_locations_parquet/")
geo_asn_df  = spark.read.option("mergeSchema", "false").parquet(f"{META_BASE}geo_asn_blocks_range_parquet/")
aws_ip_df   = spark.read.option("mergeSchema", "false").parquet(f"{META_BASE}aws_ip_ranges_int_parquet/")

print("  ✅ All sources loaded")

# ──────────────────────────────────────────────────────────────
# STEP 2: CACHE FREQUENTLY USED DATAFRAMES
# ──────────────────────────────────────────────────────────────
print("\n[2/3] Caching datasets...")

vpc_df.cache()
r53_df.cache()
ec2_df.cache()
geo_blk_df.cache()
geo_loc_df.cache()
geo_asn_df.cache()
aws_ip_df.cache()

# ──────────────────────────────────────────────────────────────
# STEP 3: REGISTER TEMP VIEWS
# ──────────────────────────────────────────────────────────────
print("\n[3/3] Registering temp views...")

vpc_df.createOrReplaceTempView("vpc_logs")
r53_df.createOrReplaceTempView("r53_logs")
ec2_df.createOrReplaceTempView("ec2_details")
geo_blk_df.createOrReplaceTempView("geo_blocks")
geo_loc_df.createOrReplaceTempView("geo_locations")
geo_asn_df.createOrReplaceTempView("geo_asn")
aws_ip_df.createOrReplaceTempView("aws_ip_ranges")

print("  ✅ All views registered")
print(f"\n{'=' * 60}")
print(f"Starting Aggregations for {log_date_str}")
print(f"{'=' * 60}\n")

# ──────────────────────────────────────────────────────────────
# REUSABLE SNIPPETS
# ──────────────────────────────────────────────────────────────

# Log date SQL expression (reused in every query)
LOG_DATE_SQL = f"""
    TO_DATE(
        CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')),
        'yyyy-MM-dd'
    )
"""

# Private IP filter (reused in every query)
PRIVATE_IP_FILTER = """
    dstaddr NOT LIKE '10.%'
    AND dstaddr NOT LIKE '172.16.%'
    AND dstaddr NOT LIKE '192.168.%'
"""

# ──────────────────────────────────────────────────────────────
# AGG 1: Total Internet Egress (GB per VPC per day)
# ──────────────────────────────────────────────────────────────
print("[AGG 1] Total Internet Egress...")

agg1_df = spark.sql(f"""
    SELECT
        COALESCE(e.account_name, 'UNKNOWN')         AS account_name,
        v.account_id,
        v.region,
        v.vpc_id,
        ROUND(SUM(v.bytes) / 1073741824.0, 6)       AS total_gb,
        v.year, v.month, v.day,
        {LOG_DATE_SQL}                               AS log_date
    FROM vpc_logs v
    LEFT JOIN (SELECT DISTINCT account_id, account_name FROM ec2_details) e
        ON v.account_id = e.account_id
    WHERE v.flow_direction = 'egress'
      AND {PRIVATE_IP_FILTER}
    GROUP BY
        e.account_name, v.account_id, v.region,
        v.vpc_id, v.year, v.month, v.day
""")

write_to_postgres(agg1_df, "total_internet_egress", [
    "account_name", "account_id", "region", "vpc_id",
    "total_gb", "year", "month", "day", "log_date"
])

# ──────────────────────────────────────────────────────────────
# AGG 2: Internet Bound Flows (connection count per VPC per day)
# ──────────────────────────────────────────────────────────────
print("[AGG 2] Internet Bound Flows...")

agg2_df = spark.sql(f"""
    SELECT
        COALESCE(e.account_name, 'UNKNOWN')         AS account_name,
        v.account_id,
        v.region,
        v.vpc_id,
        COUNT(*)                                     AS total_connections,
        v.year, v.month, v.day,
        {LOG_DATE_SQL}                               AS log_date
    FROM vpc_logs v
    LEFT JOIN (SELECT DISTINCT account_id, account_name FROM ec2_details) e
        ON v.account_id = e.account_id
    WHERE v.flow_direction = 'egress'
      AND {PRIVATE_IP_FILTER}
    GROUP BY
        e.account_name, v.account_id, v.region,
        v.vpc_id, v.year, v.month, v.day
""")

write_to_postgres(agg2_df, "internet_bound_flows", [
    "account_name", "account_id", "region", "vpc_id",
    "total_connections", "year", "month", "day", "log_date"
])

# ──────────────────────────────────────────────────────────────
# AGG 3: Unique Destination IPs per VPC per day
# ──────────────────────────────────────────────────────────────
print("[AGG 3] Unique Destinations...")

agg3_df = spark.sql(f"""
    SELECT
        COALESCE(e.account_name, 'UNKNOWN')         AS account_name,
        v.account_id,
        v.region,
        v.vpc_id,
        v.dstaddr                                   AS destination_ip,
        SUM(v.bytes)                                AS total_bytes,
        COUNT(*)                                    AS connection_count,
        v.year, v.month, v.day,
        {LOG_DATE_SQL}                              AS log_date
    FROM vpc_logs v
    LEFT JOIN (SELECT DISTINCT account_id, account_name FROM ec2_details) e
        ON v.account_id = e.account_id
    WHERE v.flow_direction = 'egress'
      AND {PRIVATE_IP_FILTER}
    GROUP BY
        e.account_name, v.account_id, v.region,
        v.vpc_id, v.dstaddr, v.year, v.month, v.day
""")

write_to_postgres(agg3_df, "unique_destination", [
    "account_name", "account_id", "region", "vpc_id",
    "destination_ip", "total_bytes", "connection_count",
    "year", "month", "day", "log_date"
])

# ──────────────────────────────────────────────────────────────
# AGG 4: VPC Action Summary (ACCEPT/REJECT by flow direction)
# ──────────────────────────────────────────────────────────────
print("[AGG 4] VPC Action Summary...")

agg4_df = spark.sql(f"""
    SELECT
        COALESCE(e.account_name, 'UNKNOWN')         AS account_name,
        v.account_id,
        v.region,
        v.vpc_id,
        v.action,
        v.flow_direction,
        ROUND(SUM(v.bytes) / 1073741824.0, 4)       AS total_gb,
        SUM(v.packets)                              AS total_packets,
        COUNT(*)                                    AS record_count,
        v.year, v.month, v.day,
        {LOG_DATE_SQL}                              AS log_date
    FROM vpc_logs v
    LEFT JOIN (SELECT DISTINCT account_id, account_name FROM ec2_details) e
        ON v.account_id = e.account_id
    GROUP BY
        e.account_name, v.account_id, v.region, v.vpc_id,
        v.action, v.flow_direction,
        v.year, v.month, v.day
""")

write_to_postgres(agg4_df, "vpc_action_agg", [
    "account_name", "account_id", "region", "vpc_id",
    "action", "flow_direction", "total_gb", "total_packets",
    "record_count", "year", "month", "day", "log_date"
])

# ──────────────────────────────────────────────────────────────
# AGG 5: Egress Trends (hourly traffic MB)
# ──────────────────────────────────────────────────────────────
print("[AGG 5] Egress Trends Summary...")

agg5_df = spark.sql(f"""
    SELECT
        COALESCE(e.account_name, 'UNKNOWN')             AS account_name,
        v.account_id,
        v.region,
        v.vpc_id,
        DATE_TRUNC('HOUR', FROM_UNIXTIME(v.start_time)) AS time_hour,
        v.flow_direction,
        ROUND(SUM(v.bytes) / 1048576.0, 2)              AS traffic_mb,
        v.year, v.month, v.day,
        {LOG_DATE_SQL}                                  AS log_date
    FROM vpc_logs v
    LEFT JOIN (SELECT DISTINCT account_id, account_name FROM ec2_details) e
        ON v.account_id = e.account_id
    WHERE v.flow_direction = 'egress'
      AND {PRIVATE_IP_FILTER}
    GROUP BY
        e.account_name, v.account_id, v.region, v.vpc_id,
        DATE_TRUNC('HOUR', FROM_UNIXTIME(v.start_time)),
        v.flow_direction, v.year, v.month, v.day
""")

write_to_postgres(agg5_df, "vpc_egress_trends_summary", [
    "account_name", "account_id", "region", "vpc_id",
    "time_hour", "flow_direction", "traffic_mb",
    "year", "month", "day", "log_date"
])

# ──────────────────────────────────────────────────────────────
# AGG 6: Top Talkers (instance + dstaddr enriched)
# ──────────────────────────────────────────────────────────────
print("[AGG 6] Top Talkers Enriched...")

agg6_df = spark.sql(f"""
    WITH base AS (
        SELECT
            account_id, region, vpc_id, instance_id,
            dstaddr, flow_direction,
            year, month, day,
            ROUND(SUM(bytes) / 1073741824.0, 4) AS data_out_gb,
            SUM(packets)                        AS total_packets,
            COUNT(*)                            AS record_count
        FROM vpc_logs
        WHERE flow_direction = 'egress'
          AND {PRIVATE_IP_FILTER}
        GROUP BY
            account_id, region, vpc_id, instance_id,
            dstaddr, flow_direction, year, month, day
    ),
    acct_fb AS (
        SELECT DISTINCT account_id, account_name, biz_unit
        FROM ec2_details
    )
    SELECT
        COALESCE(e.account_name, a.account_name, 'UNKNOWN') AS account_name,
        COALESCE(e.biz_unit,     a.biz_unit,     'UNKNOWN') AS biz_unit,
        COALESCE(e.application_id,               'UNKNOWN') AS application_id,
        t.account_id, t.region, t.vpc_id, t.instance_id,
        t.dstaddr, t.flow_direction,
        SUM(t.data_out_gb)    AS total_data_out_gb,
        SUM(t.total_packets)  AS total_packets,
        SUM(t.record_count)   AS record_count,
        t.year, t.month, t.day,
        {LOG_DATE_SQL}        AS log_date
    FROM base t
    LEFT JOIN ec2_details e
        ON  t.account_id  = e.account_id
        AND t.region      = e.region
        AND t.instance_id = e.instance_id
    LEFT JOIN acct_fb a ON t.account_id = a.account_id
    GROUP BY
        COALESCE(e.account_name, a.account_name, 'UNKNOWN'),
        COALESCE(e.biz_unit,     a.biz_unit,     'UNKNOWN'),
        COALESCE(e.application_id,               'UNKNOWN'),
        t.account_id, t.region, t.vpc_id, t.instance_id,
        t.dstaddr, t.flow_direction,
        t.year, t.month, t.day
""")

write_to_postgres(agg6_df, "vpc_top_talkers_enriched", [
    "account_name", "biz_unit", "application_id",
    "account_id", "region", "vpc_id", "instance_id",
    "dstaddr", "flow_direction",
    "total_data_out_gb", "total_packets", "record_count",
    "year", "month", "day", "log_date"
])

# ──────────────────────────────────────────────────────────────
# AGG 7: Top Ports (top instance+dstaddr per port by bytes)
# ──────────────────────────────────────────────────────────────
print("[AGG 7] Top Ports...")

agg7_df = spark.sql(f"""
    WITH ranked AS (
        SELECT
            account_id, region, vpc_id,
            dstport, instance_id, dstaddr,
            year, month, day,
            SUM(bytes) AS total_bytes,
            ROW_NUMBER() OVER (
                PARTITION BY account_id, region, vpc_id,
                             dstport, year, month, day
                ORDER BY SUM(bytes) DESC
            ) AS rnk
        FROM vpc_logs
        WHERE flow_direction = 'egress'
          AND {PRIVATE_IP_FILTER}
        GROUP BY
            account_id, region, vpc_id,
            dstport, instance_id, dstaddr,
            year, month, day
    )
    SELECT
        COALESCE(e.account_name, 'UNKNOWN')         AS account_name,
        r.account_id, r.region, r.vpc_id,
        CASE
            WHEN r.dstport = 443  THEN '443 / HTTPS'
            WHEN r.dstport = 80   THEN '80 / HTTP'
            WHEN r.dstport = 53   THEN '53 / DNS'
            WHEN r.dstport = 22   THEN '22 / SSH'
            WHEN r.dstport = 3306 THEN '3306 / MySQL'
            WHEN r.dstport = 5432 THEN '5432 / PostgreSQL'
            WHEN r.dstport = 6379 THEN '6379 / Redis'
            WHEN r.dstport = 8080 THEN '8080 / HTTP-Alt'
            ELSE CONCAT(CAST(r.dstport AS STRING), ' / Other')
        END                                         AS destination_port,
        r.instance_id,
        r.dstaddr,
        ROUND(r.total_bytes / 1073741824.0, 4)      AS data_out_gb,
        r.year, r.month, r.day,
        {LOG_DATE_SQL}                              AS log_date
    FROM ranked r
    LEFT JOIN (SELECT DISTINCT account_id, account_name FROM ec2_details) e
        ON r.account_id = e.account_id
    WHERE r.rnk = 1
""")

write_to_postgres(agg7_df, "vpc_top_ports_agg", [
    "account_name", "account_id", "region", "vpc_id",
    "destination_port", "instance_id", "dstaddr",
    "data_out_gb", "year", "month", "day", "log_date"
])

# ──────────────────────────────────────────────────────────────
# AGG 8: Destination ASN + Service
# ──────────────────────────────────────────────────────────────
print("[AGG 8] Destination ASN + Service...")

agg8_df = spark.sql(f"""
    SELECT /*+ BROADCAST(geo_asn, aws_ip_ranges) */
        COALESCE(e.account_name, 'UNKNOWN')             AS account_name,
        v.account_id, v.region, v.vpc_id,
        COALESCE(asn.autonomous_system_organization,
                 'UNKNOWN')                             AS asn_org,
        CASE
            WHEN v.pkt_dst_aws_service IS NULL
              OR v.pkt_dst_aws_service = ''             THEN 'NON-AWS'
            ELSE v.pkt_dst_aws_service
        END                                             AS service_label,
        SUM(v.bytes)                                    AS total_bytes,
        ROUND(SUM(v.bytes) / 1073741824.0, 6)           AS total_gb,
        COUNT(*)                                        AS flow_count,
        v.year, v.month, v.day,
        {LOG_DATE_SQL}                                  AS log_date
    FROM vpc_logs v
    LEFT JOIN (SELECT DISTINCT account_id, account_name FROM ec2_details) e
        ON v.account_id = e.account_id
    LEFT JOIN geo_asn asn
        ON v.dst_ip_int BETWEEN asn.network_start_integer AND asn.network_end_integer
    LEFT JOIN aws_ip_ranges aws
        ON v.dst_ip_int BETWEEN aws.network_start_int AND aws.network_end_int
    WHERE v.flow_direction = 'egress'
      AND {PRIVATE_IP_FILTER}
    GROUP BY
        COALESCE(e.account_name, 'UNKNOWN'),
        v.account_id, v.region, v.vpc_id,
        asn.autonomous_system_organization,
        v.pkt_dst_aws_service,
        v.year, v.month, v.day
    ORDER BY total_bytes DESC
""")

write_to_postgres(agg8_df, "destination_asn_service_agg", [
    "account_name", "account_id", "region", "vpc_id",
    "asn_org", "service_label",
    "total_bytes", "total_gb", "flow_count",
    "year", "month", "day", "log_date"
])

# ──────────────────────────────────────────────────────────────
# AGG 9: Top Domain + Port + Country + GB (DNS enriched)
# ──────────────────────────────────────────────────────────────
print("[AGG 9] Top Domain + Port + Country...")

agg9_df = spark.sql(f"""
    WITH deduped_dns AS (
        SELECT
            account_id, vpc_id, client_ip,
            resolved_ip, year, month, day,
            MAX_BY(domain, query_count) AS primary_domain
        FROM (
            SELECT
                t.account_id, t.vpc_id,
                t.srcaddr   AS client_ip,
                t.query_name AS domain,
                ans.Rdata   AS resolved_ip,
                COUNT(*)    AS query_count,
                t.year, t.month, t.day
            FROM r53_logs t
            LATERAL VIEW EXPLODE(t.answers) ans_table AS ans
            WHERE t.rcode = 'NOERROR'
            GROUP BY
                t.account_id, t.vpc_id, t.srcaddr,
                t.query_name, ans.Rdata,
                t.year, t.month, t.day
        )
        GROUP BY
            account_id, vpc_id, client_ip,
            resolved_ip, year, month, day
    ),
    vpc_with_port AS (
        SELECT
            account_id, region, vpc_id,
            srcaddr, dstaddr, dstport,
            pkt_dst_aws_service,
            dst_ip_int,
            SUM(bytes)   AS total_bytes,
            SUM(packets) AS total_packets,
            COUNT(*)     AS flow_count,
            year, month, day
        FROM vpc_logs
        WHERE flow_direction = 'egress'
          AND {PRIVATE_IP_FILTER}
          AND log_status = 'OK'
        GROUP BY
            account_id, region, vpc_id,
            srcaddr, dstaddr, dstport,
            pkt_dst_aws_service, dst_ip_int,
            year, month, day
    )
    SELECT /*+ BROADCAST(deduped_dns, geo_blocks, geo_locations, geo_asn, aws_ip_ranges) */
        COALESCE(e.account_name,   'UNKNOWN')           AS account_name,
        v.account_id, v.region, v.vpc_id,
        COALESCE(d.primary_domain, 'UNKNOWN')           AS domain,
        v.dstport,
        CASE
            WHEN v.dstport = 443  THEN '443 / HTTPS'
            WHEN v.dstport = 80   THEN '80 / HTTP'
            WHEN v.dstport = 53   THEN '53 / DNS'
            WHEN v.dstport = 22   THEN '22 / SSH'
            WHEN v.dstport = 3306 THEN '3306 / MySQL'
            WHEN v.dstport = 5432 THEN '5432 / PostgreSQL'
            WHEN v.dstport = 6379 THEN '6379 / Redis'
            WHEN v.dstport = 8080 THEN '8080 / HTTP-Alt'
            ELSE CONCAT(CAST(v.dstport AS STRING), ' / Other')
        END                                             AS destination_port,
        v.dstaddr                                       AS destination_ip,
        loc.country_name,
        asn.autonomous_system_organization              AS asn_org,
        CASE
            WHEN aws.service IS NULL THEN 'NON-AWS'
            ELSE aws.service
        END                                             AS final_service_type,
        ROUND(SUM(v.total_bytes) / 1073741824.0, 6)    AS total_gb,
        SUM(v.total_packets)                            AS total_packets,
        SUM(v.flow_count)                               AS flow_count,
        v.year, v.month, v.day,
        {LOG_DATE_SQL}                                  AS log_date
    FROM vpc_with_port v
    LEFT JOIN deduped_dns d
        ON  v.dstaddr    = d.resolved_ip
        AND v.srcaddr    = d.client_ip
        AND v.account_id = d.account_id
        AND v.vpc_id     = d.vpc_id
        AND v.year       = d.year
        AND v.month      = d.month
        AND v.day        = d.day
    LEFT JOIN (SELECT DISTINCT account_id, account_name FROM ec2_details) e
        ON v.account_id = e.account_id
    LEFT JOIN geo_blocks gb
        ON v.dst_ip_int BETWEEN gb.network_start_integer AND gb.network_end_integer
    LEFT JOIN geo_locations loc ON gb.geoname_id = loc.geoname_id
    LEFT JOIN geo_asn asn
        ON v.dst_ip_int BETWEEN asn.network_start_integer AND asn.network_end_integer
    LEFT JOIN aws_ip_ranges aws
        ON v.dst_ip_int BETWEEN aws.network_start_int AND aws.network_end_int
    GROUP BY
        COALESCE(e.account_name,   'UNKNOWN'),
        v.account_id, v.region, v.vpc_id,
        d.primary_domain, v.dstport, v.dstaddr,
        loc.country_name,
        asn.autonomous_system_organization,
        aws.service,
        v.year, v.month, v.day
    ORDER BY total_gb DESC
""")

write_to_postgres(agg9_df, "top_domain_port_geo", [
    "account_name", "account_id", "region", "vpc_id",
    "domain", "dstport", "destination_port",
    "destination_ip", "country_name", "asn_org",
    "final_service_type", "total_gb", "total_packets",
    "flow_count", "year", "month", "day", "log_date"
])

# ──────────────────────────────────────────────────────────────
# AGG 10: Network Intelligence Final (geo + DNS + EC2 enriched)
# ──────────────────────────────────────────────────────────────
print("[AGG 10] Network Intelligence Final...")

agg10_df = spark.sql(f"""
    WITH deduped_dns AS (
        SELECT
            account_id, vpc_id, client_ip,
            resolved_ip, year, month, day,
            MAX_BY(domain, query_count) AS primary_domain
        FROM (
            SELECT
                t.account_id, t.vpc_id,
                t.srcaddr        AS client_ip,
                t.query_name     AS domain,
                ans.Rdata        AS resolved_ip,
                COUNT(*)         AS query_count,
                t.year, t.month, t.day
            FROM r53_logs t
            LATERAL VIEW EXPLODE(t.answers) ans_table AS ans
            WHERE t.rcode = 'NOERROR'
            GROUP BY
                t.account_id, t.vpc_id, t.srcaddr,
                t.query_name, ans.Rdata,
                t.year, t.month, t.day
        )
        GROUP BY
            account_id, vpc_id, client_ip,
            resolved_ip, year, month, day
    ),
    vpc_sum AS (
        SELECT
            account_id, region, vpc_id, instance_id,
            srcaddr, dstaddr, pkt_dst_aws_service,
            traffic_path, dst_ip_int,
            SUM(bytes)   AS total_bytes,
            SUM(packets) AS total_packets,
            year, month, day
        FROM vpc_logs
        WHERE flow_direction = 'egress'
          AND {PRIVATE_IP_FILTER}
          AND log_status = 'OK'
        GROUP BY
            account_id, region, vpc_id, instance_id,
            srcaddr, dstaddr, pkt_dst_aws_service,
            traffic_path, dst_ip_int,
            year, month, day
    ),
    unified AS (
        SELECT
            v.account_id, v.region, v.vpc_id, v.instance_id,
            v.srcaddr           AS client_ip,
            v.dstaddr           AS destination_ip,
            v.pkt_dst_aws_service,
            v.traffic_path,
            v.dst_ip_int,
            d.primary_domain    AS domain,
            SUM(v.total_bytes)  AS total_bytes,
            SUM(v.total_packets) AS total_packets,
            v.year, v.month, v.day
        FROM vpc_sum v
        LEFT JOIN deduped_dns d
            ON  v.dstaddr    = d.resolved_ip
            AND v.srcaddr    = d.client_ip
            AND v.account_id = d.account_id
            AND v.vpc_id     = d.vpc_id
            AND v.year       = d.year
            AND v.month      = d.month
            AND v.day        = d.day
        GROUP BY
            v.account_id, v.region, v.vpc_id, v.instance_id,
            v.srcaddr, v.dstaddr, v.pkt_dst_aws_service,
            v.traffic_path, v.dst_ip_int,
            d.primary_domain,
            v.year, v.month, v.day
    ),
    acct_fb AS (
        SELECT DISTINCT account_id, region, account_name, biz_unit
        FROM ec2_details
    )
    SELECT /*+ BROADCAST(geo_blocks, geo_locations, geo_asn, aws_ip_ranges) */
        COALESCE(e.account_name,  af.account_name, 'UNKNOWN') AS account_name,
        COALESCE(e.biz_unit,      af.biz_unit,     'UNKNOWN') AS biz_unit,
        COALESCE(e.application_id,                 'UNKNOWN') AS application_id,
        u.account_id, u.region, u.vpc_id, u.instance_id,
        u.client_ip, u.destination_ip,
        COALESCE(u.domain, 'UNKNOWN')                         AS domain,
        u.pkt_dst_aws_service,
        u.traffic_path,
        loc.country_name,
        asn.autonomous_system_organization                    AS asn_org,
        CASE
            WHEN aws.service IS NULL THEN 'NON-AWS'
            ELSE aws.service
        END                                                   AS final_service_type,
        aws.region                                            AS aws_region,
        CASE
            WHEN aws.service IS NULL        THEN 'Internet'
            WHEN aws.region  = u.region     THEN 'Same-Region'
            WHEN aws.region IS NOT NULL
             AND aws.region != u.region     THEN 'Cross-Region'
            ELSE 'Unknown'
        END                                                   AS region_scope,
        ROUND(SUM(u.total_bytes) / 1073741824.0, 6)           AS data_out_gb,
        SUM(u.total_packets)                                  AS total_packets,
        u.year, u.month, u.day,
        {LOG_DATE_SQL}                                        AS log_date
    FROM unified u
    LEFT JOIN ec2_details e
        ON  u.account_id  = e.account_id
        AND u.region      = e.region
        AND u.instance_id = e.instance_id
    LEFT JOIN acct_fb af
        ON  u.account_id = af.account_id
        AND u.region     = af.region
    LEFT JOIN geo_blocks gb
        ON u.dst_ip_int BETWEEN gb.network_start_integer AND gb.network_end_integer
    LEFT JOIN geo_locations loc ON gb.geoname_id = loc.geoname_id
    LEFT JOIN geo_asn asn
        ON u.dst_ip_int BETWEEN asn.network_start_integer AND asn.network_end_integer
    LEFT JOIN aws_ip_ranges aws
        ON  u.dst_ip_int      BETWEEN aws.network_start_int AND aws.network_end_int
        AND aws.service       = u.pkt_dst_aws_service
    GROUP BY
        COALESCE(e.account_name,  af.account_name, 'UNKNOWN'),
        COALESCE(e.biz_unit,      af.biz_unit,     'UNKNOWN'),
        COALESCE(e.application_id,                 'UNKNOWN'),
        u.account_id, u.region, u.vpc_id, u.instance_id,
        u.client_ip, u.destination_ip, u.domain,
        u.pkt_dst_aws_service, u.traffic_path,
        loc.country_name,
        asn.autonomous_system_organization,
        aws.service, aws.region,
        u.year, u.month, u.day
    ORDER BY total_bytes DESC
""")

write_to_postgres(agg10_df, "network_intelligence_final", [
    "account_name", "biz_unit", "application_id",
    "account_id", "region", "vpc_id", "instance_id",
    "client_ip", "destination_ip", "domain",
    "pkt_dst_aws_service", "traffic_path",
    "country_name", "asn_org",
    "final_service_type", "aws_region", "region_scope",
    "data_out_gb", "total_packets",
    "year", "month", "day", "log_date"
])

# ──────────────────────────────────────────────────────────────
# UNPERSIST CACHE
# ──────────────────────────────────────────────────────────────
vpc_df.unpersist()
r53_df.unpersist()
ec2_df.unpersist()
geo_blk_df.unpersist()
geo_loc_df.unpersist()
geo_asn_df.unpersist()
aws_ip_df.unpersist()

print(f"\n{'=' * 60}")
print(f"✅ All aggregations complete for {log_date_str}")
print(f"{'=' * 60}\n")

job.commit()
