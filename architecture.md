0# Architecture — Pipeline Qualité de l'Air (Madagascar)

## Le projet en une phrase

On récupère automatiquement, tous les jours, la qualité de l'air de
5 villes de Madagascar, on nettoie ces données, et on les stocke pour
pouvoir ensuite les analyser et les visualiser.

---

## Le pipeline en 5 étapes

```
1. RÉCUPÉRER          2. STOCKER (brut)      3. NETTOYER            4. UPLOAD CSV           5. METTRE À JOUR
   API météo      -->    AWS S3 - RAW/   -->    Script Python   -->    AWS S3 - CLEAN/  -->    Data warehouse
   (n8n,                                     CSV propre
    tous les jours)
```

Les données sont d'abord récupérées depuis l'API météo et conservées telles
quelles dans `RAW/`. Un script Python les nettoie ensuite et enregistre la
version propre au format CSV. Ce CSV propre est uploadé dans `CLEAN/`, puis
les données de `CLEAN/` mettent à jour le **data warehouse** (une base de
données organisée), pour que chacun puisse construire ses propres graphiques
dessus (Bloc 2, travail individuel).

---

## Étape 1 — Récupérer les données (n8n)

**Outil : n8n**, installé sur un serveur (VPS) qui tourne en continu.
données de qualité de l'air pour les 5 villes en même temps, en
appelant l'API `weather.yotech.mg`.

**À propos de l'API `weather.yotech.mg` :**
Il s'agit d'une API que nous avons conçue nous-mêmes. Elle fait office de
passerelle vers l'API publique **OpenWeatherMap**. Concrètement, quand n8n
interroge `weather.yotech.mg`, notre API relaie la requête vers
OpenWeatherMap en utilisant une première clé API. Pour éviter d'être bloqués
en cas de forte utilisation, elle est configurée avec **deux clés API
OpenWeatherMap** : si la première clé atteint sa limite de requêtes, l'API
basculera automatiquement sur la seconde clé, afin de garantir la continuité
de la récupération des données.

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

**Outil : script Python** (`clean_air_quality.py`), lancé
automatiquement juste après la récupération, directement depuis n8n (node
"Execute Command" sur le VPS).

Le script fait le ménage dans les données brutes :
- il enlève les doublons (si la même mesure arrive deux fois)
- il repère et signale les valeurs manquantes
- il transforme les dates illisibles (ex: `1753779600`) en dates
  normales (ex: `29 juillet 2025, 09h00`)
- il retrouve le nom de la ville à partir de ses coordonnées GPS
- il range tout ça proprement dans un nouveau fichier CSV

Ce fichier CSV propre est ensuite **uploadé** dans un second dossier,
`CLEAN/` (= "propre"), toujours sur S3.

**Pourquoi une étape d'upload séparée ?**
Séparer la génération du CSV propre de son dépôt dans `CLEAN/` permet de
relancer l'envoi vers S3 sans refaire tout le nettoyage, et de garder une
copie claire des données prêtes à être chargées dans le data warehouse.

**Pourquoi un script à part, plutôt que tout faire dans n8n ?**
Parce que nettoyer une grosse quantité de données (jusqu'à 12 mois
d'historique x 5 villes, soit plus de 40 000 mesures) est plus simple avec
un script Python dédié qu'avec les outils de n8n, qui sont plutôt faits pour
des tâches d'orchestration et des transformations simples.

**Pourquoi le script sort du CSV ?**
Le JSON reçu de l'API est conservé tel quel dans `RAW/` afin de garder une
copie fidèle des données originales. Après nettoyage, le CSV est plus
adapté à la mise à jour du data warehouse et à l'analyse tabulaire.

---

## Étape 5 — Mettre à jour le data warehouse

Un **data warehouse**, c'est une base de données bien rangée, organisée
en tables, pensée pour qu'on puisse poser des questions dessus
facilement (ex : "quelle est la ville la plus polluée en moyenne ?").

C'est différent du data lake (S3) qui, lui, stocke juste des fichiers
en vrac.

Les fichiers CSV propres déposés dans `CLEAN/` mettent ensuite à jour le
data warehouse. La mise à jour intègre les nouvelles mesures aux données
existantes, afin de conserver une table exploitable pour les analyses et
les visualisations.

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
| Récupération des données | n8n (sur VPS, tous les jours) |
| Source des données | API weather.yotech.mg |
| Stockage brut | AWS S3 (`RAW/`) |
| Nettoyage | Python (script `clean_air_quality.py`) |
| Upload CSV propre | AWS S3 (`CLEAN/`) |
| Base de données finale | Data warehouse (mise à jour) |
| Visualisation | Power BI / Tableau / autre |

---

## Auteurs

*[À compléter avec les noms et std de l'équipe et ce que chacun a fait]*
