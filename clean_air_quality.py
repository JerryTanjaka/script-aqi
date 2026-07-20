"""
clean_air_quality.py
---------------------
Etape RAW -> CLEAN du pipeline "Qualité de l'air" (Bloc 1).

Ton node n8n "Convert to File" produit déjà des fichiers CSV "aplatis"
(colonnes du type list.0.components.pm2_5). Ce script part donc de CE
format par défaut (--raw-format csv). Il gère aussi le cas où tu voudrais
repartir des JSON bruts de l'API (--raw-format json), au cas où tu changes
le node n8n plus tard.

Dans tous les cas, le script :
  1. Charge les fichiers bruts (dossier local ou bucket S3).
  2. Renomme/aplati les colonnes vers un schéma propre.
  3. Ajoute le nom de la ville (via une table de correspondance lat/lon,
     en attendant que le workflow n8n envoie directement le nom de la ville).
  4. Nettoie les données : types, doublons, valeurs manquantes, unités.
  5. Sauvegarde un CSV propre dans CLEAN/, prêt pour le data warehouse.

Usage :
    python clean_air_quality.py --source local --input-dir ./raw_data --raw-format csv
    python clean_air_quality.py --source local --input-dir ./raw_data --raw-format json
    python clean_air_quality.py --source s3 --bucket weather-data-hei --prefix RAW/ --raw-format csv
"""

import argparse
import json
import glob
import os
from datetime import datetime, timezone

import pandas as pd

# ----------------------------------------------------------------------
# 1. Table de correspondance lat/lon -> ville
#    A COMPLETER avec tes 5 vraies villes une fois le workflow corrigé
#    (chaque ville doit avoir SES propres coordonnées dans n8n).
# ----------------------------------------------------------------------
CITY_LOOKUP = {
    (-18.91, 47.54): "Antananarivo",
    # (lat, lon): "Nom de la ville",
    # ex: (-6.17, 106.83): "Jakarta",
    #     (48.85, 2.35): "Paris",
}


def resolve_city(lat: float, lon: float) -> str:
    """Retrouve le nom de la ville à partir des coordonnées (arrondies)."""
    key = (round(lat, 2), round(lon, 2))
    return CITY_LOOKUP.get(key, f"Inconnue ({lat},{lon})")


# ----------------------------------------------------------------------
# 2. Chargement des fichiers bruts
# ----------------------------------------------------------------------
def load_local_json_files(input_dir: str) -> list[dict]:
    """Charge tous les .json d'un dossier local."""
    records = []
    for path in glob.glob(os.path.join(input_dir, "**", "*.json"), recursive=True):
        with open(path, "r", encoding="utf-8") as f:
            try:
                records.append(json.load(f))
            except json.JSONDecodeError:
                print(f"[WARN] Fichier JSON invalide ignoré : {path}")
    return records


def load_s3_json_files(bucket: str, prefix: str) -> list[dict]:
    """Charge tous les .json d'un préfixe S3 (nécessite boto3 + credentials AWS configurés)."""
    import boto3

    s3 = boto3.client("s3")
    records = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            try:
                records.append(json.loads(body))
            except json.JSONDecodeError:
                print(f"[WARN] Fichier JSON invalide ignoré : {key}")
    return records


def load_local_csv_files(input_dir: str) -> pd.DataFrame:
    """Charge et concatène tous les .csv déjà aplatis par n8n (Convert to File)."""
    frames = []
    for path in glob.glob(os.path.join(input_dir, "**", "*.csv"), recursive=True):
        try:
            frames.append(pd.read_csv(path))
        except pd.errors.EmptyDataError:
            print(f"[WARN] Fichier CSV vide ignoré : {path}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_s3_csv_files(bucket: str, prefix: str) -> pd.DataFrame:
    """Charge et concatène tous les .csv d'un préfixe S3."""
    import io
    import boto3

    s3 = boto3.client("s3")
    frames = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            try:
                frames.append(pd.read_csv(io.BytesIO(body)))
            except pd.errors.EmptyDataError:
                print(f"[WARN] Fichier CSV vide ignoré : {key}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# Mapping des colonnes "aplaties" par n8n -> schéma propre attendu par clean_dataframe
N8N_COLUMN_MAP = {
    "coord.lon": "lon",
    "coord.lat": "lat",
    "list.0.main.aqi": "aqi",
    "list.0.components.co": "co",
    "list.0.components.no": "no",
    "list.0.components.no2": "no2",
    "list.0.components.o3": "o3",
    "list.0.components.so2": "so2",
    "list.0.components.pm2_5": "pm2_5",
    "list.0.components.pm10": "pm10",
    "list.0.components.nh3": "nh3",
    "list.0.dt": "dt",
}


def normalize_n8n_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Renomme les colonnes du CSV n8n vers le schéma propre + ajoute la ville."""
    if df.empty:
        return df
    df = df.rename(columns=N8N_COLUMN_MAP)
    df["city"] = df.apply(lambda row: resolve_city(row.get("lat"), row.get("lon")), axis=1)
    return df


# ----------------------------------------------------------------------
# 3. Aplatissement (flatten) d'une réponse OpenWeather Air Pollution
#    (utile seulement si --raw-format json)
# ----------------------------------------------------------------------
def flatten_record(raw: dict) -> list[dict]:
    """
    Un objet brut OpenWeather a la forme :
    {
      "coord": {"lon": .., "lat": ..},
      "list": [ {"dt": .., "main": {"aqi": ..}, "components": {...}} , ... ]
    }
    "list" peut contenir plusieurs mesures (ex: si on interroge un historique).
    On sort une ligne par mesure.
    """
    rows = []
    coord = raw.get("coord", {})
    lat, lon = coord.get("lat"), coord.get("lon")

    for entry in raw.get("list", []):
        components = entry.get("components", {})
        rows.append({
            "city": resolve_city(lat, lon) if lat is not None and lon is not None else None,
            "lat": lat,
            "lon": lon,
            "dt": entry.get("dt"),
            "aqi": entry.get("main", {}).get("aqi"),
            "co": components.get("co"),
            "no": components.get("no"),
            "no2": components.get("no2"),
            "o3": components.get("o3"),
            "so2": components.get("so2"),
            "pm2_5": components.get("pm2_5"),
            "pm10": components.get("pm10"),
            "nh3": components.get("nh3"),
        })
    return rows


# ----------------------------------------------------------------------
# 4. Nettoyage
# ----------------------------------------------------------------------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # dt (epoch) -> datetime UTC lisible
    df["datetime_utc"] = df["dt"].apply(
        lambda x: datetime.fromtimestamp(x, tz=timezone.utc) if pd.notna(x) else pd.NaT
    )

    # Colonnes numériques attendues
    numeric_cols = ["aqi", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", "lat", "lon"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Supprime les lignes sans mesure exploitable (ville inconnue ET pas de polluants)
    df = df.dropna(subset=["aqi"] + [c for c in numeric_cols if c not in ("lat", "lon")], how="all")

    # Doublons : même ville + même timestamp = même mesure
    df = df.drop_duplicates(subset=["city", "dt"])

    # Valeurs manquantes restantes sur les polluants -> on les garde mais on les marque
    df["has_missing_pollutant"] = df[["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]].isna().any(axis=1)

    # Tri chronologique par ville
    df = df.sort_values(["city", "dt"]).reset_index(drop=True)

    # Réordonner les colonnes
    ordered_cols = ["city", "lat", "lon", "dt", "datetime_utc", "aqi",
                     "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3",
                     "has_missing_pollutant"]
    return df[ordered_cols]


# ----------------------------------------------------------------------
# 5. Main
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Nettoyage des données brutes de qualité de l'air")
    parser.add_argument("--source", choices=["local", "s3"], default="local")
    parser.add_argument("--raw-format", choices=["csv", "json"], default="csv",
                         help="Format des fichiers bruts : 'csv' = sortie du node n8n Convert to File (par défaut), "
                              "'json' = réponse brute de l'API OpenWeather")
    parser.add_argument("--input-dir", default="./raw_data", help="Dossier local contenant les fichiers bruts")
    parser.add_argument("--bucket", help="Nom du bucket S3 (si --source s3)")
    parser.add_argument("--prefix", default="RAW/", help="Préfixe S3 (si --source s3)")
    parser.add_argument("--output", default="./clean_data/air_quality_clean.csv")
    args = parser.parse_args()

    if args.raw_format == "csv":
        if args.source == "local":
            df = load_local_csv_files(args.input_dir)
        else:
            if not args.bucket:
                raise SystemExit("--bucket est requis avec --source s3")
            df = load_s3_csv_files(args.bucket, args.prefix)
        df = normalize_n8n_csv(df)
        print(f"[INFO] {len(df)} ligne(s) chargée(s) depuis les CSV bruts.")
    else:
        if args.source == "local":
            raw_records = load_local_json_files(args.input_dir)
        else:
            if not args.bucket:
                raise SystemExit("--bucket est requis avec --source s3")
            raw_records = load_s3_json_files(args.bucket, args.prefix)

        print(f"[INFO] {len(raw_records)} fichier(s) brut(s) chargé(s).")

        all_rows = []
        for raw in raw_records:
            all_rows.extend(flatten_record(raw))

        df = pd.DataFrame(all_rows)
        print(f"[INFO] {len(df)} mesure(s) extraite(s) avant nettoyage.")

    df_clean = clean_dataframe(df)
    print(f"[INFO] {len(df_clean)} mesure(s) après nettoyage.")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df_clean.to_csv(args.output, index=False)
    print(f"[OK] Fichier propre sauvegardé : {args.output}")


if __name__ == "__main__":
    main()