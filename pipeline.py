from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import time


spark = SparkSession.builder \
    .appName("ONISR_2024_Pipeline") \
    .config("spark.ui.port", "4040") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.sql.warehouse.dir", r"C:\Users\Boly-\Desktop\data_p\spark-warehouse") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sparkContext._jsc.hadoopConfiguration().set(
    "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
)

RAW    = r"C:\Users\Boly-\Desktop\data_p\raw"
SILVER = r"C:\Users\Boly-\Desktop\data_p\silver"
GOLD   = r"C:\Users\Boly-\Desktop\data_p\gold"


# Etape 1 : Lecture des fichiers bruts avec schéma explicite

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

def read_csv(path, schema):
    return spark.read \
        .option("header", "true") \
        .option("sep", ";") \
        .option("encoding", "utf-8") \
        .option("mode", "PERMISSIVE") \
        .schema(schema) \
        .csv(path)

print("Lecture des fichiers bruts")
caract    = read_csv(f"{RAW}/caract-2024.csv",    schema_caract)
lieux     = read_csv(f"{RAW}/lieux-2024.csv",     schema_lieux)
vehicules = read_csv(f"{RAW}/vehicules-2024.csv", schema_vehicules)
usagers   = read_csv(f"{RAW}/usagers-2024.csv",   schema_usagers)

print(f"caract   : {caract.count()} lignes")
print(f"lieux    : {lieux.count()} lignes")
print(f"vehicules: {vehicules.count()} lignes")
print(f"usagers  : {usagers.count()} lignes")

caract.show(3)
usagers.show(3)


# Etape 2 : Nettoyage des données

print("Nettoyage en cours...")

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


# Etape 3 : Ecriture de la couche Silver en Parquet

print("Ecriture Silver...")

caract_clean.write.mode("overwrite").partitionBy("mois").parquet(f"{SILVER}/caract")
lieux_clean.write.mode("overwrite").parquet(f"{SILVER}/lieux")
vehicules_clean.write.mode("overwrite").parquet(f"{SILVER}/vehicules")
usagers_clean.write.mode("overwrite").partitionBy("grav").parquet(f"{SILVER}/usagers")

print("Silver écrit avec succès")


# Etape 4 : Analyses sur la couche Silver

print("Lecture de la couche Silver")
caract_s    = spark.read.parquet(f"{SILVER}/caract")
lieux_s     = spark.read.parquet(f"{SILVER}/lieux")
vehicules_s = spark.read.parquet(f"{SILVER}/vehicules")
usagers_s   = spark.read.parquet(f"{SILVER}/usagers")


# Analyse 1 : gravité des accidents selon la météo
# atm : 1=normale, 2=pluie légère, 3=pluie forte, 4=neige, 5=brouillard
# grav : 1=indemne, 2=tué, 3=blessé hospitalisé, 4=blessé léger

print("Analyse 1 : gravité par météo")
t0 = time.time()

stats_meteo = caract_s.join(usagers_s, "Num_Acc") \
    .groupBy("atm") \
    .agg(
        F.count("*").alias("nb_victimes"),
        F.round(F.avg("grav"), 2).alias("gravite_moyenne"),
        F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues")
    ) \
    .orderBy("atm")

t1 = time.time()
print(f"Temps : {t1-t0:.2f}s")
stats_meteo.show(10)
stats_meteo.write.mode("overwrite").parquet(f"{GOLD}/gravite_meteo")


# Analyse 2 : accidents mortels par type de route et luminosité (jointure 4 tables)
# lieux est broadcasté car c'est la plus petite table

print("Analyse 2 : jointure 4 tables")
t0 = time.time()

df_joint = caract_s \
    .join(broadcast(lieux_s), "Num_Acc") \
    .join(vehicules_s, "Num_Acc") \
    .join(usagers_s, "Num_Acc")

accidents_mortels = df_joint \
    .filter(F.col("grav") == 2) \
    .groupBy("catr", "lum") \
    .agg(F.count("*").alias("nb_tues")) \
    .orderBy(F.desc("nb_tues"))

t1 = time.time()
print(f"Temps : {t1-t0:.2f}s")
accidents_mortels.show(10)
accidents_mortels.write.mode("overwrite").parquet(f"{GOLD}/accidents_mortels")


# Analyse 3 : classement des départements avec window function

print("Analyse 3 : classement des départements")
t0 = time.time()

accidents_par_dep = caract_s.join(usagers_s, "Num_Acc") \
    .groupBy("dep") \
    .agg(
        F.countDistinct("Num_Acc").alias("nb_accidents"),
        F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues")
    )

window_dep = Window.orderBy(F.desc("nb_accidents"))

dep_classe = accidents_par_dep \
    .withColumn("rang", F.rank().over(window_dep)) \
    .withColumn("taux_mortalite",
        F.round(
            F.when(F.col("nb_accidents") > 0,
                F.col("nb_tues") / F.col("nb_accidents") * 100
            ).otherwise(0), 2)
    ) \
    .orderBy("rang")

t1 = time.time()
print(f"Temps : {t1-t0:.2f}s")
dep_classe.show(15)
dep_classe.write.mode("overwrite").parquet(f"{GOLD}/classement_departements")

print("Analyses terminées")


# Etape 5 : Optimisation mesurée

# Test broadcast join : on désactive d'abord le broadcast automatique
print("Optimisation : broadcast join")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
t0 = time.time()
sans_broadcast = caract_s.join(lieux_s, "Num_Acc") \
    .groupBy("dep", "catr") \
    .agg(F.count("*").alias("nb")) \
    .orderBy(F.desc("nb"))
sans_broadcast.write.mode("overwrite").parquet(f"{GOLD}/test_sans_broadcast")
t1 = time.time()
temps_sans = t1 - t0
print(f"Sans broadcast : {temps_sans:.2f}s")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
t0 = time.time()
avec_broadcast = caract_s.join(broadcast(lieux_s), "Num_Acc") \
    .groupBy("dep", "catr") \
    .agg(F.count("*").alias("nb")) \
    .orderBy(F.desc("nb"))
avec_broadcast.write.mode("overwrite").parquet(f"{GOLD}/test_avec_broadcast")
t1 = time.time()
temps_avec = t1 - t0
print(f"Avec broadcast : {temps_avec:.2f}s")
print(f"Gain : {((temps_sans - temps_avec) / temps_sans * 100):.1f}%")

print("Plan sans broadcast :")
caract_s.join(lieux_s, "Num_Acc").explain()
print("Plan avec broadcast :")
caract_s.join(broadcast(lieux_s), "Num_Acc").explain()

# Test cache : on réutilise la même jointure deux fois
print("Optimisation : cache")

t0 = time.time()
base = caract_s.join(usagers_s, "Num_Acc")
r1 = base.filter(F.col("grav") == 2).count()
r2 = base.filter(F.col("atm") == 1).count()
t1 = time.time()
print(f"Sans cache : {t1-t0:.2f}s  (tués={r1}, atm_normale={r2})")

t0 = time.time()
base_cached = caract_s.join(usagers_s, "Num_Acc").cache()
base_cached.count()
r1 = base_cached.filter(F.col("grav") == 2).count()
r2 = base_cached.filter(F.col("atm") == 1).count()
t1 = time.time()
print(f"Avec cache : {t1-t0:.2f}s  (tués={r1}, atm_normale={r2})")
base_cached.unpersist()


# Etape 6 : Exploration AQE
# J'ai testé plusieurs valeurs pour comprendre l'effet de ce paramètre sur les performances

print("Exploration AQE : effet du nombre de partitions")

spark.conf.set("spark.sql.adaptive.enabled", "true")

partitions_test = [4, 8, 20, 200]

for n in partitions_test:
    spark.conf.set("spark.sql.shuffle.partitions", str(n))
    t0 = time.time()
    result = caract_s.join(usagers_s, "Num_Acc") \
        .groupBy("dep", "mois") \
        .agg(
            F.count("*").alias("nb_victimes"),
            F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues")
        ) \
        .orderBy(F.desc("nb_victimes"))
    result.write.mode("overwrite").parquet(f"{GOLD}/aqe_test_{n}")
    t1 = time.time()
    print(f"  partitions={n}  →  {t1-t0:.2f}s")

# Même test avec AQE désactivé pour voir la différence
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "200")
t0 = time.time()
result = caract_s.join(usagers_s, "Num_Acc") \
    .groupBy("dep", "mois") \
    .agg(
        F.count("*").alias("nb_victimes"),
        F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues")
    ) \
    .orderBy(F.desc("nb_victimes"))
result.write.mode("overwrite").parquet(f"{GOLD}/aqe_off_200")
t1 = time.time()
print(f"  AQE désactivé, partitions=200  →  {t1-t0:.2f}s")

print("Pipeline complet terminé")

spark.stop()