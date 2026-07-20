"""
clean_air_quality.py
---------------------
Etape RAW -> CLEAN du pipeline "Qualité de l'air" (Bloc 1).

Ce script :
  1. Charge les fichiers JSON bruts produits par le workflow n8n
     (soit depuis un dossier local, soit depuis un bucket S3).
  2. Aplati (flatten) la structure imbriquée de l'API OpenWeather
     Air Pollution (coord + list[0].main + list[0].components).
  3. Ajoute le nom de la ville (via une table de correspondance lat/lon,
     en attendant que le workflow n8n envoie directement le nom de la ville).
  4. Nettoie les données : types, doublons, valeurs manquantes, unités.
  5. Sauvegarde un CSV propre dans CLEAN/, prêt pour le data warehouse.

Usage :
    python clean_air_quality.py --source local --input-dir ./raw_data
    python clean_air_quality.py --source s3 --bucket weather-data-hei --prefix RAW/
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


# ----------------------------------------------------------------------
# 3. Aplatissement (flatten) d'une réponse OpenWeather Air Pollution
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
    parser.add_argument("--input-dir", default="./raw_data", help="Dossier local contenant les .json bruts")
    parser.add_argument("--bucket", help="Nom du bucket S3 (si --source s3)")
    parser.add_argument("--prefix", default="RAW/", help="Préfixe S3 (si --source s3)")
    parser.add_argument("--output", default="./clean_data/air_quality_clean.csv")
    args = parser.parse_args()

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
