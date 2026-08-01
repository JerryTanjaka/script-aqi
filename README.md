# script-aqi

## Pipeline de qualité de l'air — Madagascar

Ce projet permet de **collecter automatiquement les données de qualité de l'air de cinq villes de Madagascar**. Les données collectées sont ensuite traitées et nettoyées avant d'être stockées afin de pouvoir être analysées et visualisées.

### Les 5 villes suivies

| Ville        | Latitude | Longitude |
| ------------ | -------: | --------: |
| Antananarivo | -18.9185 |   47.5211 |
| Toliara      | -23.3583 |   43.6672 |
| Toamasina    | -18.1716 |   49.3761 |
| Mahajanga    | -15.7180 |   46.3173 |
| Antsiranana  | -12.2783 |   49.2915 |

### Technologies utilisées

Le projet utilise principalement **n8n, AWS S3 et Python** pour automatiser la collecte et le traitement des données de qualité de l'air.

### Données collectées

Les données de qualité de l'air comprennent notamment :

* l'indice de qualité de l'air (AQI) ;
* le CO ;
* le NO ;
* le NO2 ;
* l'O3 ;
* le SO2 ;
* les PM2.5 ;
* les PM10 ;
* le NH3.

Pour consulter le fonctionnement détaillé du pipeline, les différentes étapes de traitement et les choix techniques, voir [`architecture.md`](architecture.md).
