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

# ══════════════════════════════════════════════════════════
# ÉTAPE 4 — ANALYSES (Silver → Gold)
# ══════════════════════════════════════════════════════════

# Relire la couche silver
print("\n=== Lecture Silver ===")
caract_s    = spark.read.parquet(f"{SILVER}/caract")
lieux_s     = spark.read.parquet(f"{SILVER}/lieux")
vehicules_s = spark.read.parquet(f"{SILVER}/vehicules")
usagers_s   = spark.read.parquet(f"{SILVER}/usagers")

# ── ANALYSE 1 : Gravité par conditions météo (agrégation) ──
print("\n=== Analyse 1 : Gravité par météo ===")
# atm : 1=normale, 2=pluie légère, 3=pluie forte, 4=neige, 5=brouillard, etc.
# grav : 1=indemne, 2=tué, 3=blessé hospitalisé, 4=blessé léger

t0 = time.time()

gravite_meteo = caract_s.join(usagers_s, "Num_Acc") \
    .groupBy("atm") \
    .agg(
        F.count("*").alias("nb_victimes"),
        F.round(F.avg("grav"), 2).alias("gravite_moyenne"),
        F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues")
    ) \
    .orderBy("atm")

t1 = time.time()
print(f"Temps analyse 1 : {t1-t0:.2f}s")
gravite_meteo.show(10)
gravite_meteo.write.mode("overwrite").parquet(f"{GOLD}/gravite_meteo")

# ── ANALYSE 2 : Jointure des 4 tables — profil complet ────
print("\n=== Analyse 2 : Jointure 4 tables ===")

t0 = time.time()

# Broadcast de lieux (petite table) pour optimisation
from pyspark.sql.functions import broadcast

accidents_complets = caract_s \
    .join(broadcast(lieux_s), "Num_Acc") \
    .join(vehicules_s, "Num_Acc") \
    .join(usagers_s, "Num_Acc")

# Accidents mortels par type de route et luminosité
accidents_mortels = accidents_complets \
    .filter(F.col("grav") == 2) \
    .groupBy("catr", "lum") \
    .agg(F.count("*").alias("nb_tues")) \
    .orderBy(F.desc("nb_tues"))

t1 = time.time()
print(f"Temps analyse 2 : {t1-t0:.2f}s")
accidents_mortels.show(10)
accidents_mortels.write.mode("overwrite").parquet(f"{GOLD}/accidents_mortels")

# ── ANALYSE 3 : Classement des départements (window) ──────
print("\n=== Analyse 3 : Classement départements (window) ===")
from pyspark.sql.window import Window

t0 = time.time()

# Nombre d'accidents et tués par département
dep_stats = caract_s.join(usagers_s, "Num_Acc") \
    .groupBy("dep") \
    .agg(
        F.countDistinct("Num_Acc").alias("nb_accidents"),
        F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues")
    )

# Window : rang par nb_accidents
window_dep = Window.orderBy(F.desc("nb_accidents"))

dep_classe = dep_stats \
    .withColumn("rang", F.rank().over(window_dep)) \
    .withColumn("taux_mortalite",
        F.round(F.when(F.col("nb_accidents") > 0,
            F.col("nb_tues") / F.col("nb_accidents") * 100
        ).otherwise(0), 2)
    ) \
    .orderBy("rang")

t1 = time.time()
print(f"Temps analyse 3 : {t1-t0:.2f}s")
dep_classe.show(15)
dep_classe.write.mode("overwrite").parquet(f"{GOLD}/classement_departements")

print("\n=== Analyses terminées, résultats écrits dans Gold ===")

spark.stop()