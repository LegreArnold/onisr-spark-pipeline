# Rapport – Pipeline Spark ONISR 2024

**Auteur :** LegreArnold  
**Date :** 26 juin 2026

## Introduction

L'objectif de ce projet est de mettre en œuvre un pipeline de traitement de données avec PySpark à partir des données d'accidents corporels de la circulation publiées par l'ONISR pour l'année 2024. Le travail consiste à construire un pipeline de type Bronze → Silver → Gold, puis à réaliser plusieurs analyses afin d'illustrer les principales fonctionnalités de Spark, notamment les jointures, les agrégations, les fonctions de fenêtre et quelques mécanismes d'optimisation.

## Présentation des données

Les données proviennent de la plateforme data.gouv.fr et sont réparties dans quatre fichiers CSV : les caractéristiques des accidents, les lieux, les véhicules et les usagers. La clé `Num_Acc` permet de relier les quatre tables entre elles.

Voici le volume de données traité :

| Table | Lignes brutes |
|---|---|
| caracteristiques | 54 402 |
| lieux | 70 248 |
| vehicules | 92 678 |
| usagers | 125 187 |

Avant de commencer les analyses, un schéma explicite (`StructType`) a été défini pour chaque fichier. Cette étape permet d'éviter les erreurs de typage et d'obtenir des temps de lecture plus stables qu'avec une détection automatique des types.

## Nettoyage des données

Une première étape de préparation a été réalisée afin de garantir la qualité des données utilisées dans les analyses. Les doublons ont été supprimés lorsque cela était nécessaire et plusieurs contrôles de cohérence ont été appliqués. Par exemple, les mois sont limités entre 1 et 12 et les jours entre 1 et 31. Les valeurs de gravité ont également été filtrées afin de conserver uniquement les codes définis par l'ONISR (1 à 4).

J'ai également créé une colonne `heure` à partir de la colonne `hrmn`, ce qui facilite certaines analyses temporelles.

Dans l'ensemble, les données de 2024 sont relativement propres puisque très peu d'enregistrements ont dû être supprimés. La couche silver a été écrite en Parquet, partitionnée par `mois` pour les caractéristiques et par `grav` pour les usagers.

## Analyse 1 – Gravité des accidents selon les conditions météorologiques

La première analyse cherche à savoir si les conditions météorologiques influencent la gravité des accidents.

Pour cela, les tables des caractéristiques et des usagers ont été jointes avant de calculer le nombre de victimes, la gravité moyenne et le nombre de personnes décédées pour chaque condition météorologique.

Extrait des résultats :

| atm | nb_victimes | gravite_moyenne | nb_tues |
|---|---|---|---|
| 1 (normale) | 96 243 | 2.52 | 2 628 |
| 2 (pluie légère) | 15 491 | 2.57 | 345 |
| 7 (éblouissement) | 1 824 | 2.35 | 61 |
| 9 (autre) | 529 | 2.71 | 22 |

Les résultats montrent que la majorité des accidents se produisent lorsque les conditions météorologiques sont normales. Certaines catégories moins fréquentes présentent une gravité moyenne plus élevée, mais elles concernent un nombre beaucoup plus faible d'accidents. Ces résultats suggèrent donc que la météo peut avoir une influence sur la gravité, sans être le principal facteur expliquant les accidents.

## Analyse 2 – Routes et luminosité

La deuxième analyse combine les quatre tables afin d'obtenir une vision plus complète des accidents.

L'utilisation d'une jointure avec les quatre jeux de données permet d'étudier les accidents mortels selon le type de route et les conditions de luminosité.

Extrait des résultats :

| catr | lum | nb_tues |
|---|---|---|
| 3 (route nationale) | 1 (plein jour) | 2 002 |
| 3 (route nationale) | 3 (nuit éclairée) | 814 |
| 4 (route départementale) | 1 (plein jour) | 577 |

Les résultats indiquent que les routes nationales concentrent un nombre important de décès, notamment en journée. Une explication possible est que les vitesses pratiquées sur ces axes restent élevées malgré de bonnes conditions de visibilité.

## Analyse 3 – Classement des départements

La dernière analyse utilise une fonction de fenêtre (`Window`) pour établir un classement des départements selon le nombre d'accidents.

Extrait des résultats (top 5) :

| dep | nb_accidents | nb_tues | rang | taux_mortalite |
|---|---|---|---|---|
| 75 (Paris) | 4 191 | 31 | 1 | 0.74% |
| 93 (Seine-Saint-Denis) | 2 640 | 29 | 2 | 1.10% |
| 13 (Bouches-du-Rhône) | 2 120 | 115 | 4 | 5.42% |
| 83 (Var) | 866 | 62 | 14 | 7.16% |

Les résultats montrent que Paris est le département qui enregistre le plus grand nombre d'accidents. En revanche, son taux de mortalité reste relativement faible comparé à certains départements plus ruraux. Cela montre qu'un nombre élevé d'accidents ne signifie pas forcément une gravité plus importante.

## Optimisations

Plusieurs optimisations proposées par Spark ont été testées.

Le broadcast join permet d'améliorer le temps d'exécution lorsque l'une des tables est suffisamment petite pour être diffusée sur l'ensemble des exécuteurs. Dans les mesures réalisées, cette optimisation réduit sensiblement le temps d'exécution :

| Stratégie | Temps |
|---|---|
| Sans broadcast | 2.19s |
| Avec broadcast | 1.62s |
| Gain | 26% |

À l'inverse, le cache n'apporte pas d'amélioration dans ce projet. Les données étant relativement peu volumineuses, le coût de mise en cache est supérieur au gain obtenu lors des réutilisations (0.76s sans cache contre 1.83s avec cache).

## Exploration AQE – Effet du nombre de partitions

Plusieurs mesures ont été réalisées en faisant varier le paramètre `spark.sql.shuffle.partitions` sur la même agrégation, avec l'AQE activé puis désactivé.

| shuffle.partitions | AQE | Temps |
|---|---|---|
| 4 | activé | 0.90s |
| 8 | activé | 0.62s |
| 20 | activé | 0.64s |
| 200 | activé | 1.40s |
| 200 | désactivé | 5.28s |

Ces résultats montrent que l'AQE a un impact très important. Sans AQE, Spark crée 200 partitions même si les données sont peu volumineuses, ce qui génère beaucoup de tâches inutiles. Avec AQE activé, Spark fusionne automatiquement les petites partitions et réduit le temps d'exécution de 5.28s à 1.40s, soit un gain de 73%. Le meilleur réglage manuel se situe autour de 8 partitions pour ce volume de données.

## Lecture de la Spark UI

La Spark UI a été consultée pendant l'exécution du pipeline sur le port 4040.

![Jobs](images/Capture_d_écran_2026-06-26_124815.png)
*9 jobs complétés, durées de 20 à 91ms*

![Stages](images/Capture_d_écran_2026-06-26_124830.png)
*Stage 44 : Shuffle Write de 3.9 MiB — produit par le groupBy sur dep et catr*

![SQL DataFrame](images/Capture_d_écran_2026-06-26_124846.png)
*28 queries complétées, plans d'exécution visibles*

![Job 23 détail](images/Capture_d_écran_2026-06-26_125049.png)
*4 tasks, Shuffle Write de 4.4 MiB sur la jointure caract + usagers*

Le shuffle apparaît systématiquement après les opérations de jointure et d'agrégation. Spark doit redistribuer les données entre les partitions avant de pouvoir calculer les agrégats, ce qui génère des échanges de données entre les exécuteurs.

## Conclusion

Ce projet m'a permis d'approfondir l'utilisation de PySpark sur un cas concret de traitement de données. J'ai pu mettre en pratique les différentes étapes d'un pipeline de données, depuis l'ingestion jusqu'à la production d'indicateurs analytiques.

Les expérimentations réalisées montrent également que certaines optimisations, comme le broadcast ou l'AQE, peuvent avoir un impact important sur les performances, alors que d'autres, comme le cache, ne sont pas systématiquement pertinentes. Cela souligne l'importance de mesurer les performances plutôt que d'appliquer les optimisations de manière automatique.