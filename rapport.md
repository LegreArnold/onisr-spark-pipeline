# Rapport — Pipeline Spark ONISR 2024

**Auteur** : LegreArnold  
**Date** : 26 juin 2026  
**Jeu de données** : Accidents corporels de la circulation — France 2024 (ONISR/BAAC)

---

## 1. Jeu de données et schéma cible

### Source
Ministère de l'Intérieur — data.gouv.fr. 4 fichiers CSV, séparateur `;`, encodage UTF-8.

### Volume brut
| Table | Lignes |
|---|---|
| caracteristiques | 54 402 |
| lieux | 70 248 |
| vehicules | 92 678 |
| usagers | 125 187 |

### Schéma cible
Clé de jointure : `Num_Acc` relie les 4 tables.

| Table | Colonnes clés |
|---|---|
| caracteristiques | Num_Acc, jour, mois, an, hrmn, lum, dep, atm, col |
| lieux | Num_Acc, catr, surf, infra, vma |
| vehicules | Num_Acc, id_vehicule, catv, manv |
| usagers | Num_Acc, id_usager, id_vehicule, grav, sexe, an_nais |

---

## 2. Pipeline Bronze → Silver → Gold

### Bronze → Silver (nettoyage)
- Schéma explicite (`StructType`) pour les 4 tables — pas d'`inferSchema`
- Encodage UTF-8 détecté sur les fichiers 2024
- Colonne `heure` dérivée depuis `hrmn` avec `withColumn`
- Doublons supprimés : `dropDuplicates(["Num_Acc"])` sur caract et lieux
- Filtre métier : `grav` entre 1 et 4 (valeurs hors code ONISR écartées)
- Filtre cohérence : `mois` entre 1-12, `jour` entre 1-31

**Lignes après nettoyage** : aucune ligne écartée — les données 2024 sont propres.

### Partitionnement Silver
- `caract` partitionné par `mois` (12 partitions, faible cardinalité)
- `usagers` partitionné par `grav` (4 niveaux : indemne, tué, blessé hosp., blessé léger)

### Silver → Gold
3 analyses + optimisation + exploration AQE écrits en Parquet dans `gold/`.

---

## 3. Les trois analyses

### Analyse 1 — Gravité par conditions météo (agrégation)

**Question** : Les conditions météo influencent-elles la gravité des accidents ?

**Code clé** :
```python
gravite_meteo = caract_s.join(usagers_s, "Num_Acc") \
    .groupBy("atm") \
    .agg(
        F.count("*").alias("nb_victimes"),
        F.round(F.avg("grav"), 2).alias("gravite_moyenne"),
        F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues")
    ) \
    .orderBy("atm")
```

**Résultat** :
| atm | nb_victimes | gravite_moyenne | nb_tues |
|---|---|---|---|
| 1 (normale) | 96 243 | 2.52 | 2 628 |
| 2 (pluie légère) | 15 491 | 2.57 | 345 |
| 9 (autre) | 529 | 2.71 | 22 |
| 7 (éblouissement) | 1 824 | 2.35 | 61 |

**Lecture métier** : Le brouillard et conditions dégradées (atm=9) produisent la gravité moyenne la plus élevée (2.71). L'éblouissement (atm=7) a paradoxalement la gravité la plus faible (2.35), probablement car il survient en zone urbaine à vitesse réduite. La grande majorité des accidents (96 243) survient par temps normal — la météo n'est pas le facteur principal.

---

### Analyse 2 — Accidents mortels par type de route et luminosité (jointure 4 tables)

**Question** : Quels types de routes et quelles conditions de luminosité concentrent le plus de tués ?

**Code clé** :
```python
accidents_complets = caract_s \
    .join(broadcast(lieux_s), "Num_Acc") \
    .join(vehicules_s, "Num_Acc") \
    .join(usagers_s, "Num_Acc")

accidents_mortels = accidents_complets \
    .filter(F.col("grav") == 2) \
    .groupBy("catr", "lum") \
    .agg(F.count("*").alias("nb_tues")) \
    .orderBy(F.desc("nb_tues"))
```

**Résultat** :
| catr | lum | nb_tues |
|---|---|---|
| 3 (route nationale) | 1 (plein jour) | 2 002 |
| 3 (route nationale) | 3 (nuit éclairée) | 814 |
| 4 (route départementale) | 1 (plein jour) | 577 |

**Lecture métier** : Les routes nationales de jour concentrent le plus grand nombre de tués (2 002). Ce résultat contre-intuitif s'explique par les vitesses élevées pratiquées de jour sur ces axes. La nuit éclairée en nationale (814 tués) reste dangereuse malgré l'éclairage public.

---

### Analyse 3 — Classement des départements par accidentalité (window function)

**Question** : Quels départements ont le plus d'accidents et les taux de mortalité les plus élevés ?

**Code clé** :
```python
window_dep = Window.orderBy(F.desc("nb_accidents"))
dep_classe = dep_stats \
    .withColumn("rang", F.rank().over(window_dep)) \
    .withColumn("taux_mortalite",
        F.round(F.when(F.col("nb_accidents") > 0,
            F.col("nb_tues") / F.col("nb_accidents") * 100
        ).otherwise(0), 2)
    )
```

**Résultat (top 5)** :
| dep | nb_accidents | nb_tues | rang | taux_mortalite |
|---|---|---|---|---|
| 75 (Paris) | 4 191 | 31 | 1 | 0.74% |
| 93 (Seine-Saint-Denis) | 2 640 | 29 | 2 | 1.10% |
| 13 (Bouches-du-Rhône) | 2 120 | 115 | 4 | 5.42% |
| 83 (Var) | 866 | 62 | 14 | 7.16% |

**Lecture métier** : Paris est 1er en volume (4 191 accidents) mais avec le taux de mortalité le plus bas (0.74%) — les vitesses faibles en zone dense limitent la gravité. À l'inverse, le Var (83) avec 866 accidents a un taux de 7.16% — les routes rurales à grande vitesse sont beaucoup plus meurtrières proportionnellement.

---

## 4. Optimisation mesurée

### Broadcast join
`lieux` (54 402 lignes, ~7 Mo) est broadcasté dans la jointure avec `caract`.

| Stratégie | Temps |
|---|---|
| Sans broadcast (sort-merge) | 2.19s |
| Avec broadcast | 1.62s |
| **Gain** | **26%** |

Le plan confirme `BroadcastHashJoin` avec `BroadcastExchange` dans les deux cas — AQE détecte automatiquement que `lieux` est petite et applique le broadcast même sans l'imposer.

### Cache
Résultat contre-intuitif : le cache est **plus lent** (0.76s → 1.83s) sur ce volume. Le coût de matérialisation dépasse le gain car les données tiennent facilement en mémoire et Spark relit le Parquet très vite. Le cache serait utile sur des volumes plus importants ou des transformations coûteuses répétées.

---

## 5. Lecture de la Spark UI

**Port** : localhost:4040

Les captures montrent :
- **9 jobs complétés** avec des durées de 20 à 91ms — pipeline très rapide sur ce volume
- **Stage 44** : Shuffle Write de 3.9 MiB — produit par le `groupBy("dep", "catr")` qui redistribue les données entre partitions
- **Stage 43** : Shuffle Read de 1082 KiB — lit le résultat du shuffle précédent pour finaliser l'agrégation
- **Job 23** : 4/4 tasks, Shuffle Write 4.4 MiB — le shuffle est ici la jointure + groupBy sur `dep` et `mois`

Le shuffle apparaît systématiquement après les `join` et `groupBy` : Spark doit redistribuer les lignes par clé avant d'agréger.

---

## 6. Exploration AQE — Effet du nombre de partitions de shuffle

**Protocole** : même agrégation (`groupBy dep, mois` sur la jointure caract + usagers), un seul paramètre varie : `spark.sql.shuffle.partitions`. AQE activé puis désactivé à 200 partitions.

**Résultats** :
| shuffle.partitions | AQE | Temps |
|---|---|---|
| 4 | activé | 0.90s |
| 8 | activé | 0.62s |
| 20 | activé | 0.64s |
| 200 | activé | 1.40s |
| 200 | **désactivé** | **5.28s** |

**Conclusion** : L'AQE réduit le temps de 5.28s à 1.40s à 200 partitions, soit un gain de **73%**. Sans AQE, Spark crée 200 partitions de shuffle même si les données sont petites — la majorité des tasks sont vides et le overhead de coordination domine. Avec AQE activé, Spark fusionne automatiquement les petites partitions (coalescing). Le sweet spot sans AQE est autour de 8 partitions pour ce volume (~125K lignes d'usagers jointé avec ~54K de caract).

---

## 7. Ce qu'on a appris et limites

### Apprentissages
- Sur Windows, PySpark nécessite `winutils.exe` + `hadoop.dll` pour écrire des fichiers locaux
- L'AQE est le levier le plus puissant sur ce volume : 73% de gain sans rien changer au code
- Le cache n'est pas toujours bénéfique — il faut que le DataFrame soit réutilisé plusieurs fois ET que la relecture soit coûteuse
- Paris concentre les accidents en volume mais les routes rurales tuent proportionnellement beaucoup plus

### Limites
- Les coordonnées GPS (`lat`, `long`) sont NULL pour la majorité des accidents — pas d'analyse spatiale possible
- Le volume (54K accidents) est modeste en mode local — les effets d'optimisation seraient plus marqués sur plusieurs millions de lignes
- La colonne `an_nais` des usagers contient des valeurs incohérentes (1927, valeurs très anciennes) non filtrées
- La window function sans `partitionBy` ramène tout sur un seul executor — acceptable ici mais à éviter sur gros volume