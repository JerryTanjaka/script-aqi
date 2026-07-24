"""
clean_air_quality.py
---------------------
Etape RAW -> CLEAN du pipeline "Qualité de l'air" (Bloc 1).

Le node n8n "Convert to File" upload maintenant le JSON brut tel quel
(pas de conversion CSV intermédiaire) vers S3 RAW/ : c'est le format
natif renvoyé par l'API OpenWeather, donc --raw-format json est le
comportement par défaut. L'ancien format CSV aplati (colonnes du type
list.0.components.pm2_5) reste supporté via --raw-format csv, pour
compatibilité avec d'anciens exports.

Le script :
  1. Charge les fichiers bruts JSON (dossier local ou bucket S3).
  2. Aplati la structure imbriquée (coord + list[].main + list[].components).
  3. Ajoute le nom de la ville (via une table de correspondance lat/lon).
  4. Nettoie les données : types, doublons, valeurs manquantes.
  5. Exporte un fichier JSON propre (liste d'enregistrements), localement
     et/ou uploadé vers S3 CLEAN/, prêt pour le chargement dans le
     data warehouse.

Usage :
    python clean_air_quality.py --source local --input-dir ./raw_data
    python clean_air_quality.py --source local --input-dir ./raw_data --raw-format csv
    python clean_air_quality.py --source s3 --bucket weather-data-hei --prefix RAW/ \
        --output /tmp/clean.json --upload-s3-key CLEAN/air_quality_clean.json

Codes de sortie :
    0 = succès (même si 0 ligne après nettoyage -> fichier JSON vide "[]")
    1 = erreur de configuration (arguments manquants/invalides)
    2 = erreur de chargement des données brutes
    3 = erreur d'écriture ou d'upload du résultat
"""

from __future__ import annotations

import argparse
import glob
import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger("clean_air_quality")

# ----------------------------------------------------------------------
# 1. Table de correspondance lat/lon -> ville
#    A COMPLETER avec les vraies villes du projet.
#    Peut aussi être fournie via un fichier externe avec --city-lookup-file
#    (JSON: [{"lat": -18.91, "lon": 47.54, "city": "Antananarivo"}, ...])
# ----------------------------------------------------------------------
DEFAULT_CITY_LOOKUP: dict[tuple[float, float], str] = {
    (-18.9185, 47.5211): "Antananarivo",
    (-23.3583, 43.6672): "Toliara",
    (-18.1716, 49.3761): "Toamasina",
    (-15.7180, 46.3173): "Mahajanga",
    (-12.2783, 49.2915): "Antsiranana",
}

REQUIRED_OUTPUT_COLUMNS = [
    "city", "lat", "lon", "dt", "datetime_utc", "aqi",
    "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3",
    "has_missing_pollutant",
]

POLLUTANT_COLUMNS = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
NUMERIC_COLUMNS = ["aqi", *POLLUTANT_COLUMNS, "lat", "lon"]


def load_city_lookup(path: str | None) -> dict[tuple[float, float], str]:
    """Charge la table lat/lon -> ville depuis un fichier JSON, sinon utilise la table par défaut."""
    if not path:
        return DEFAULT_CITY_LOOKUP
    try:
        with open(path, "r", encoding="utf-8") as f:
            entries = json.load(f)
        lookup = {(round(float(e["lat"]), 4), round(float(e["lon"]), 4)): e["city"] for e in entries}
        logger.info("Table de correspondance ville chargée depuis %s (%d entrées).", path, len(lookup))
        return lookup
    except (OSError, json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.warning("Impossible de charger --city-lookup-file (%s) : %s. Utilisation de la table par défaut.", path, exc)
        return DEFAULT_CITY_LOOKUP


def resolve_city(lat: Any, lon: Any, lookup: dict[tuple[float, float], str]) -> str | None:
    """Retrouve le nom de la ville à partir des coordonnées (arrondies)."""
    if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
        return None
    key = (round(float(lat), 4), round(float(lon), 4))
    if key in lookup:
        return lookup[key]
    # Tolérance : certaines sources arrondissent à 2 décimales seulement
    key2 = (round(float(lat), 2), round(float(lon), 2))
    for (k_lat, k_lon), name in lookup.items():
        if round(k_lat, 2) == key2[0] and round(k_lon, 2) == key2[1]:
            return name
    return f"Inconnue ({lat},{lon})"


# ----------------------------------------------------------------------
# 2. Chargement des fichiers bruts
# ----------------------------------------------------------------------
def load_local_json_files(input_dir: str) -> list[dict]:
    """Charge tous les .json d'un dossier local."""
    records = []
    pattern = os.path.join(input_dir, "**", "*.json")
    paths = glob.glob(pattern, recursive=True)
    if not paths:
        logger.warning("Aucun fichier .json trouvé dans %s", input_dir)
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                records.append(json.load(f))
        except json.JSONDecodeError:
            logger.warning("Fichier JSON invalide ignoré : %s", path)
        except OSError as exc:
            logger.warning("Impossible de lire %s : %s", path, exc)
    return records


def load_s3_json_files(bucket: str, prefix: str) -> list[dict]:
    """Charge tous les .json d'un préfixe S3 (nécessite boto3 + credentials AWS configurés)."""
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3")
    records: list[dict] = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        found_any = False
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                found_any = True
                body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                try:
                    records.append(json.loads(body))
                except json.JSONDecodeError:
                    logger.warning("Fichier JSON invalide ignoré : s3://%s/%s", bucket, key)
        if not found_any:
            logger.warning("Aucun fichier .json trouvé sous s3://%s/%s", bucket, prefix)
    except ClientError as exc:
        logger.error("Erreur d'accès S3 (%s/%s) : %s", bucket, prefix, exc)
        raise
    return records


def load_local_csv_files(input_dir: str) -> pd.DataFrame:
    """Charge et concatène tous les .csv déjà aplatis par n8n (Convert to File)."""
    frames = []
    pattern = os.path.join(input_dir, "**", "*.csv")
    paths = glob.glob(pattern, recursive=True)
    if not paths:
        logger.warning("Aucun fichier .csv trouvé dans %s", input_dir)
    for path in paths:
        try:
            frames.append(pd.read_csv(path))
        except pd.errors.EmptyDataError:
            logger.warning("Fichier CSV vide ignoré : %s", path)
        except (OSError, pd.errors.ParserError) as exc:
            logger.warning("Impossible de lire %s : %s", path, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_s3_csv_files(bucket: str, prefix: str) -> pd.DataFrame:
    """Charge et concatène tous les .csv d'un préfixe S3."""
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3")
    frames = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        found_any = False
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".csv"):
                    continue
                found_any = True
                body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                try:
                    frames.append(pd.read_csv(io.BytesIO(body)))
                except pd.errors.EmptyDataError:
                    logger.warning("Fichier CSV vide ignoré : s3://%s/%s", bucket, key)
        if not found_any:
            logger.warning("Aucun fichier .csv trouvé sous s3://%s/%s", bucket, prefix)
    except ClientError as exc:
        logger.error("Erreur d'accès S3 (%s/%s) : %s", bucket, prefix, exc)
        raise
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


def normalize_n8n_csv(df: pd.DataFrame, city_lookup: dict[tuple[float, float], str]) -> pd.DataFrame:
    """Renomme les colonnes du CSV n8n vers le schéma propre + ajoute la ville."""
    if df.empty:
        return df
    df = df.rename(columns=N8N_COLUMN_MAP)

    missing = [c for c in ["lat", "lon", "dt", "aqi"] if c not in df.columns]
    if missing:
        logger.warning("Colonnes attendues absentes du CSV brut : %s", missing)
        for col in missing:
            df[col] = pd.NA

    df["city"] = df.apply(lambda row: resolve_city(row.get("lat"), row.get("lon"), city_lookup), axis=1)
    return df


# ----------------------------------------------------------------------
# 3. Aplatissement (flatten) d'une réponse brute (JSON) en mesures
# ----------------------------------------------------------------------
def iter_city_records(raw_file_content: Any) -> list[dict]:
    """
    Un fichier JSON brut peut prendre plusieurs formes selon comment il a été exporté :
      - un objet unique  : {"coord": {...}, "list": [...]}  (réponse API directe, 1 ville)
      - un objet unique  : {"lat": .., "lon": .., "data": [...]}  (format custom weather.yotech.mg)
      - une LISTE d'items exportés depuis n8n (ex: node "Merge") :
        [ {"json": {...}, "pairedItem": {...}}, ... ]  (plusieurs villes, une par item)
    Cette fonction déroule tous ces cas vers une liste plate de dicts "1 ville = 1 dict".
    """
    if isinstance(raw_file_content, dict):
        return [raw_file_content]
    if isinstance(raw_file_content, list):
        records = []
        for element in raw_file_content:
            if not isinstance(element, dict):
                continue
            if "json" in element and isinstance(element["json"], dict):
                records.append(element["json"])  # item n8n {"json": {...}, "pairedItem": {...}}
            else:
                records.append(element)  # déjà un objet ville brut
        return records
    return []


def flatten_record(raw: dict, city_lookup: dict[tuple[float, float], str]) -> list[dict]:
    """
    Gère deux formats de réponse "1 ville" :

    Format OpenWeather classique :
    {
      "coord": {"lon": .., "lat": ..},
      "list": [ {"dt": .., "main": {"aqi": ..}, "components": {...}} , ... ]
    }

    Format custom (ex: weather.yotech.mg, historique) :
    {
      "lat": .., "lon": ..,
      "data": [ {"dt": .., "main": {"aqi": ..}, "components": {...}} , ... ]
    }

    Dans les deux cas, on sort une ligne par mesure.
    """
    if not isinstance(raw, dict):
        return []

    coord = raw.get("coord") or {}
    lat = coord.get("lat", raw.get("lat"))
    lon = coord.get("lon", raw.get("lon"))
    measurements = raw.get("list") or raw.get("data") or []

    rows = []
    for entry in measurements:
        if not isinstance(entry, dict):
            continue
        components = entry.get("components") or {}
        rows.append({
            "city": resolve_city(lat, lon, city_lookup),
            "lat": lat,
            "lon": lon,
            "dt": entry.get("dt"),
            "aqi": (entry.get("main") or {}).get("aqi"),
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
    """Nettoie et normalise le DataFrame vers le schéma final REQUIRED_OUTPUT_COLUMNS."""
    if df.empty:
        return pd.DataFrame(columns=REQUIRED_OUTPUT_COLUMNS)

    # S'assure que toutes les colonnes attendues existent, même absentes en entrée
    for col in ["city", "lat", "lon", "dt", *NUMERIC_COLUMNS]:
        if col not in df.columns:
            df[col] = pd.NA

    # dt (epoch) -> datetime UTC lisible (ISO 8601)
    def _to_iso(x: Any) -> str | None:
        if pd.isna(x):
            return None
        try:
            return datetime.fromtimestamp(float(x), tz=timezone.utc).isoformat()
        except (ValueError, OSError, OverflowError):
            return None

    df["datetime_utc"] = df["dt"].apply(_to_iso)

    # Colonnes numériques attendues
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Supprime les lignes totalement vides (ni aqi ni aucun polluant)
    df = df.dropna(subset=["aqi", *POLLUTANT_COLUMNS], how="all")

    # Supprime les lignes sans timestamp exploitable (clé de dédup/tri)
    df = df.dropna(subset=["dt"])
    df["dt"] = df["dt"].astype("int64")

    # Doublons : même ville + même timestamp = même mesure
    df = df.drop_duplicates(subset=["city", "dt"])

    # Valeurs manquantes restantes sur les polluants -> gardées mais marquées
    df["has_missing_pollutant"] = df[POLLUTANT_COLUMNS].isna().any(axis=1)

    # Tri chronologique par ville
    df = df.sort_values(["city", "dt"], na_position="last").reset_index(drop=True)

    return df[REQUIRED_OUTPUT_COLUMNS]


# ----------------------------------------------------------------------
# 5. Export JSON
# ----------------------------------------------------------------------
def dataframe_to_json_records(df: pd.DataFrame) -> list[dict]:
    """Convertit le DataFrame nettoyé en liste de dicts JSON-sérialisables."""
    records = json.loads(df.to_json(orient="records", date_format="iso"))
    return records


def write_json(records: list[dict], output_path: str, pretty: bool) -> None:
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        if pretty:
            json.dump(records, f, ensure_ascii=False, indent=2)
        else:
            json.dump(records, f, ensure_ascii=False)


def upload_json_to_s3(local_path: str, bucket: str, key: str) -> None:
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3")
    try:
        s3.upload_file(
            local_path, bucket, key,
            ExtraArgs={"ContentType": "application/json"},
        )
    except ClientError as exc:
        logger.error("Echec de l'upload vers s3://%s/%s : %s", bucket, key, exc)
        raise


# ----------------------------------------------------------------------
# 6. Main
# ----------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Nettoyage RAW -> CLEAN des données de qualité de l'air (sortie JSON)")
    parser.add_argument("--source", choices=["local", "s3"], default="local")
    parser.add_argument("--raw-format", choices=["csv", "json"], default="json",
                         help="'json' = réponse brute de l'API OpenWeather (par défaut, format natif), "
                              "'csv' = ancien format aplati par le node n8n Convert to File (legacy)")
    parser.add_argument("--input-dir", default="./raw_data", help="Dossier local contenant les fichiers bruts")
    parser.add_argument("--bucket", help="Nom du bucket S3 (requis si --source s3)")
    parser.add_argument("--prefix", default="RAW/", help="Préfixe S3 (si --source s3)")
    parser.add_argument("--output", default="./clean_data/air_quality_clean.json",
                         help="Chemin local du fichier JSON de sortie")
    parser.add_argument("--upload-s3-key", default=None,
                         help="Si fourni (ex: 'CLEAN/air_quality_clean.json'), upload le résultat vers ce "
                              "chemin dans le bucket --bucket après nettoyage.")
    parser.add_argument("--city-lookup-file", default=None,
                         help="Fichier JSON optionnel de correspondance lat/lon -> ville "
                              "(remplace la table par défaut intégrée au script).")
    parser.add_argument("--pretty", action="store_true", help="Formate le JSON de sortie avec indentation.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.source == "s3" and not args.bucket:
        logger.error("--bucket est requis avec --source s3")
        return 1
    if args.upload_s3_key and not args.bucket:
        logger.error("--bucket est requis pour utiliser --upload-s3-key")
        return 1

    city_lookup = load_city_lookup(args.city_lookup_file)

    # ---- Chargement ----
    try:
        if args.raw_format == "csv":
            if args.source == "local":
                df = load_local_csv_files(args.input_dir)
            else:
                df = load_s3_csv_files(args.bucket, args.prefix)
            df = normalize_n8n_csv(df, city_lookup)
            logger.info("%d ligne(s) chargée(s) depuis les CSV bruts.", len(df))
        else:
            if args.source == "local":
                raw_records = load_local_json_files(args.input_dir)
            else:
                raw_records = load_s3_json_files(args.bucket, args.prefix)
            logger.info("%d fichier(s) brut(s) chargé(s).", len(raw_records))

            all_rows: list[dict] = []
            for raw_file_content in raw_records:
                for city_record in iter_city_records(raw_file_content):
                    all_rows.extend(flatten_record(city_record, city_lookup))
            df = pd.DataFrame(all_rows)
            logger.info("%d mesure(s) extraite(s) avant nettoyage.", len(df))
    except Exception:
        logger.exception("Erreur lors du chargement des données brutes.")
        return 2

    # ---- Nettoyage ----
    try:
        df_clean = clean_dataframe(df)
        logger.info("%d mesure(s) après nettoyage.", len(df_clean))
        if df_clean.empty:
            logger.warning("Aucune mesure exploitable après nettoyage : le JSON de sortie sera vide ([]).")
    except Exception:
        logger.exception("Erreur lors du nettoyage des données.")
        return 2

    # ---- Export ----
    try:
        records = dataframe_to_json_records(df_clean)
        write_json(records, args.output, pretty=args.pretty)
        logger.info("Fichier JSON propre sauvegardé localement : %s", args.output)
    except Exception:
        logger.exception("Erreur lors de l'écriture du fichier JSON de sortie (%s).", args.output)
        return 3

    if args.upload_s3_key:
        try:
            upload_json_to_s3(args.output, args.bucket, args.upload_s3_key)
            logger.info("Fichier uploadé vers s3://%s/%s", args.bucket, args.upload_s3_key)
        except Exception:
            logger.exception("Erreur lors de l'upload vers S3.")
            return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
