from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType
)
import time

# ── Session Spark ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("ONISR_2024_Pipeline") \
    .config("spark.ui.port", "4040") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.sql.warehouse.dir", r"C:\Users\Boly-\Desktop\data_p\spark-warehouse") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Contournement winutils Windows
spark.sparkContext._jsc.hadoopConfiguration().set(
    "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
)

# ── Chemins ────────────────────────────────────────────────
RAW    = r"C:\Users\Boly-\Desktop\data_p\raw"
SILVER = r"C:\Users\Boly-\Desktop\data_p\silver"
GOLD   = r"C:\Users\Boly-\Desktop\data_p\gold"

# ══════════════════════════════════════════════════════════
# ÉTAPE 1 — INGESTION (Bronze → Silver)
# ══════════════════════════════════════════════════════════

schema_caract = StructType([
    StructField("Num_Acc", StringType(),  True),
    StructField("jour",    IntegerType(), True),
    StructField("mois",    IntegerType(), True),
    StructField("an",      IntegerType(), True),
    StructField("hrmn",    StringType(),  True),
    StructField("lum",     IntegerType(), True),
    StructField("dep",     StringType(),  True),
    StructField("com",     StringType(),  True),
    StructField("agg",     IntegerType(), True),
    StructField("int",     IntegerType(), True),
    StructField("atm",     IntegerType(), True),
    StructField("col",     IntegerType(), True),
    StructField("adr",     StringType(),  True),
    StructField("lat",     FloatType(),   True),
    StructField("long",    FloatType(),   True),
])

schema_lieux = StructType([
    StructField("Num_Acc",  StringType(),  True),
    StructField("catr",     IntegerType(), True),
    StructField("voie",     StringType(),  True),
    StructField("v1",       StringType(),  True),
    StructField("v2",       StringType(),  True),
    StructField("circ",     IntegerType(), True),
    StructField("nbv",      IntegerType(), True),
    StructField("vosp",     IntegerType(), True),
    StructField("prof",     IntegerType(), True),
    StructField("pr",       StringType(),  True),
    StructField("pr1",      StringType(),  True),
    StructField("plan",     IntegerType(), True),
    StructField("lartpc",   StringType(),  True),
    StructField("larrout",  StringType(),  True),
    StructField("surf",     IntegerType(), True),
    StructField("infra",    IntegerType(), True),
    StructField("situ",     IntegerType(), True),
    StructField("vma",      IntegerType(), True),
])

schema_vehicules = StructType([
    StructField("Num_Acc",     StringType(),  True),
    StructField("id_vehicule", StringType(),  True),
    StructField("num_veh",     StringType(),  True),
    StructField("senc",        IntegerType(), True),
    StructField("catv",        IntegerType(), True),
    StructField("obs",         IntegerType(), True),
    StructField("obsm",        IntegerType(), True),
    StructField("choc",        IntegerType(), True),
    StructField("manv",        IntegerType(), True),
    StructField("motor",       IntegerType(), True),
    StructField("occutc",      IntegerType(), True),
])

schema_usagers = StructType([
    StructField("Num_Acc",     StringType(),  True),
    StructField("id_usager",   StringType(),  True),
    StructField("id_vehicule", StringType(),  True),
    StructField("num_veh",     StringType(),  True),
    StructField("place",       IntegerType(), True),
    StructField("catu",        IntegerType(), True),
    StructField("grav",        IntegerType(), True),
    StructField("sexe",        IntegerType(), True),
    StructField("an_nais",     IntegerType(), True),
    StructField("trajet",      IntegerType(), True),
    StructField("secu1",       IntegerType(), True),
    StructField("secu2",       IntegerType(), True),
    StructField("secu3",       IntegerType(), True),
    StructField("locp",        IntegerType(), True),
    StructField("actp",        StringType(),  True),
    StructField("etatp",       IntegerType(), True),
])

# ── Lecture CSV ────────────────────────────────────────────
def read_csv(path, schema):
    return spark.read \
        .option("header", "true") \
        .option("sep", ";") \
        .option("encoding", "utf-8") \
        .option("mode", "PERMISSIVE") \
        .schema(schema) \
        .csv(path)

print("=== Lecture des fichiers bruts ===")
caract    = read_csv(f"{RAW}/caract-2024.csv",    schema_caract)
lieux     = read_csv(f"{RAW}/lieux-2024.csv",     schema_lieux)
vehicules = read_csv(f"{RAW}/vehicules-2024.csv", schema_vehicules)
usagers   = read_csv(f"{RAW}/usagers-2024.csv",   schema_usagers)

print(f"Lignes brutes caract   : {caract.count()}")
print(f"Lignes brutes lieux    : {lieux.count()}")
print(f"Lignes brutes vehicules: {vehicules.count()}")
print(f"Lignes brutes usagers  : {usagers.count()}")

caract.show(3)
usagers.show(3)

# ══════════════════════════════════════════════════════════
# ÉTAPE 2 — NETTOYAGE
# ══════════════════════════════════════════════════════════
print("\n=== Nettoyage ===")

caract_clean = caract \
    .withColumn("heure", F.split(F.col("hrmn"), ":")[0].cast(IntegerType())) \
    .dropDuplicates(["Num_Acc"]) \
    .filter(F.col("Num_Acc").isNotNull()) \
    .filter(F.col("mois").between(1, 12)) \
    .filter(F.col("jour").between(1, 31))

lieux_clean = lieux \
    .dropDuplicates(["Num_Acc"]) \
    .filter(F.col("Num_Acc").isNotNull())

vehicules_clean = vehicules \
    .dropDuplicates(["Num_Acc", "id_vehicule"]) \
    .filter(F.col("Num_Acc").isNotNull())

usagers_clean = usagers \
    .dropDuplicates(["Num_Acc", "id_usager"]) \
    .filter(F.col("Num_Acc").isNotNull()) \
    .filter(F.col("grav").between(1, 4))

print(f"caract    après nettoyage : {caract_clean.count()}")
print(f"lieux     après nettoyage : {lieux_clean.count()}")
print(f"vehicules après nettoyage : {vehicules_clean.count()}")
print(f"usagers   après nettoyage : {usagers_clean.count()}")

# ══════════════════════════════════════════════════════════
# ÉTAPE 3 — ÉCRITURE SILVER (Parquet)
# ══════════════════════════════════════════════════════════
print("\n=== Écriture Silver (Parquet) ===")

caract_clean.write.mode("overwrite") \
    .partitionBy("mois") \
    .parquet(f"{SILVER}/caract")

lieux_clean.write.mode("overwrite") \
    .parquet(f"{SILVER}/lieux")

vehicules_clean.write.mode("overwrite") \
    .parquet(f"{SILVER}/vehicules")

usagers_clean.write.mode("overwrite") \
    .partitionBy("grav") \
    .parquet(f"{SILVER}/usagers")

print("✅ Silver écrit avec succès !")
print("\n=== Ingestion terminée ===")

spark.stop()