0# Architecture — Pipeline Qualité de l'Air (Madagascar)

## Le projet en une phrase

On récupère automatiquement, toutes les heures, la qualité de l'air de
5 villes de Madagascar, on nettoie ces données, et on les stocke pour
pouvoir ensuite les analyser et les visualiser.

## Les 5 villes suivies

| Ville | Latitude | Longitude |
|---|---|---|
| Antananarivo | -18.9185 | 47.5211 |
| Toliara | -23.3583 | 43.6672 |
| Toamasina | -18.1716 | 49.3761 |
| Mahajanga | -15.7180 | 46.3173 |
| Antsiranana | -12.2783 | 49.2915 |

---

## Le pipeline en 4 étapes

```
1. RÉCUPÉRER            2. STOCKER (brut)        3. NETTOYER            4. STOCKER (propre)
   API météo        -->    AWS S3 - RAW/     -->    Script Python   -->    AWS S3 - CLEAN/
   (n8n, toutes                                      (pandas)
    les heures)
```

Ensuite, ces données propres sont chargées dans un **data warehouse**
(une base de données organisée), pour que chacun puisse construire ses
propres graphiques dessus (Bloc 2, travail individuel).

---

## Étape 1 — Récupérer les données (n8n)

**Outil : n8n**, installé sur un serveur (VPS) qui tourne en continu.

Toutes les heures, n8n se réveille automatiquement et va chercher les
données de qualité de l'air pour les 5 villes en même temps, en
appelant l'API `weather.yotech.mg`.

**Pourquoi n8n et pas Airflow ?**
Airflow est plus puissant mais aussi plus lourd à installer et à faire
tourner (plusieurs services à gérer : scheduler, workers...). Pour un
pipeline simple comme le nôtre (5 appels API + un envoi vers S3), n8n
suffit largement, s'installe en quelques minutes, et son interface
visuelle est plus facile à montrer en démo.

---

## Étape 2 — Stocker les données brutes (AWS S3)

**Outil : AWS S3**, qui joue le rôle de "data lake" — un grand espace
de stockage où on met les fichiers tels quels, sans les trier.

Les données arrivent au format **JSON** (le format que l'API renvoie
directement) dans un dossier appelé `RAW/` (= "brut").

**Pourquoi S3 ?**
C'est l'outil de stockage le plus utilisé et le moins cher pour ce
genre d'usage, et il se connecte facilement à la fois à n8n et à
Python.

---

## Étape 3 — Nettoyer les données (script Python)

**Outil : script Python** (`clean_air_quality.py`, avec la librairie
pandas), lancé automatiquement juste après la récupération, directement
depuis n8n (node "Execute Command" sur le VPS).

Le script fait le ménage dans les données brutes :
- il enlève les doublons (si la même mesure arrive deux fois)
- il repère et signale les valeurs manquantes
- il transforme les dates illisibles (ex: `1753779600`) en dates
  normales (ex: `29 juillet 2025, 09h00`)
- il retrouve le nom de la ville à partir de ses coordonnées GPS
- il range tout ça proprement dans un nouveau fichier JSON

Ce fichier propre est ensuite déposé dans un second dossier, `CLEAN/`
(= "propre"), toujours sur S3.

**Pourquoi un script à part, plutôt que tout faire dans n8n ?**
Parce que nettoyer une grosse quantité de données (jusqu'à 12 mois
d'historique x 5 villes, soit plus de 40 000 mesures) est bien plus
simple et rapide avec Python/pandas qu'avec les outils de n8n, qui sont
plutôt faits pour des tâches simples.

**Pourquoi le script sort du JSON, et pas du CSV ?**
Parce que l'API nous donne déjà du JSON — pas besoin de le transformer
en CSV puis de le reconvertir en JSON après, ça ferait une étape
inutile qui ralentit tout, surtout avec beaucoup de données.

---

## Étape 4 — Charger dans le data warehouse (à finaliser)

Un **data warehouse**, c'est une base de données bien rangée, organisée
en tables, pensée pour qu'on puisse poser des questions dessus
facilement (ex : "quelle est la ville la plus polluée en moyenne ?").

C'est différent du data lake (S3) qui, lui, stocke juste des fichiers
en vrac.

*[Cette section sera complétée une fois l'outil choisi par le groupe :
PostgreSQL, BigQuery, Snowflake ou Redshift]*

Les tables seront organisées en **modélisation en étoile** : une table
centrale avec les mesures (ville, date, polluants), reliée à des petites
tables qui décrivent le contexte (la liste des villes, les dates).


## Ce qu'on a envisagé mais pas retenu

| Option | Pourquoi on ne l'a pas prise |
|---|---|
| **Airflow** | Trop lourd à installer pour un pipeline aussi simple |
| **AWS Lambda (serverless)** | On a déjà un serveur (VPS) qui tourne en continu, pas besoin d'ajouter une brique supplémentaire |
| **Sortie en CSV** | L'API donne déjà du JSON, pas besoin de convertir deux fois |

---

## Résumé technique

| Étape | Outil |
|---|---|
| Récupération des données | n8n (sur VPS) |
| Source des données | API weather.yotech.mg |
| Stockage brut | AWS S3 (`RAW/`) |
| Nettoyage | Python + pandas |
| Stockage propre | AWS S3 (`CLEAN/`) |
| Base de données finale | *[à compléter]* |
| Visualisation | Power BI / Tableau / autre |

---

## Auteurs

*[À compléter avec les noms et std de l'équipe et ce que chacun a fait]*
