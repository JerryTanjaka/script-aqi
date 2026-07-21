"""
reshape_historical.py
----------------------
Reshape le CSV "historique" produit par ton appel à l'API OpenWeather Air
Pollution History : 1 ligne par ville, avec des milliers de colonnes
"data.<i>.main.aqi", "data.<i>.components.co", ..., "data.<i>.dt"
(une mesure horaire par index i, ~8376 pour 12 mois).

Sortie : un CSV "long" propre, 1 ligne par (ville, timestamp), prêt pour le
data warehouse (table de faits).

Usage :
    python reshape_historical.py --input File__1_.csv --output historical_clean.csv
"""

import argparse
from datetime import datetime, timezone

import pandas as pd

# Complète avec tes 5 vraies villes une fois identifiées (lat/lon arrondis à 4 décimales,
# comme dans le fichier source)
CITY_LOOKUP = {
    (-18.9185, 47.5211): "Antananarivo",
    (-23.3583, 43.6672): "Toliara",       # a verifier / renommer
    (-18.1716, 49.3761): "Toamasina",     # a verifier / renommer
    (-15.7180, 46.3173): "Mahajanga",     # a verifier / renommer
    (-12.2783, 49.2915): "Antsiranana",   # a verifier / renommer
}


def resolve_city(lat: float, lon: float) -> str:
    key = (round(lat, 4), round(lon, 4))
    return CITY_LOOKUP.get(key, f"Inconnue ({lat},{lon})")


def reshape(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path)
    id_vars = ["lat", "lon", "period.start", "period.end", "nb_points"]
    value_vars = [c for c in df.columns if c not in id_vars]

    df = df.reset_index().rename(columns={"index": "row_id"})

    long = df.melt(
        id_vars=id_vars + ["row_id"],
        value_vars=value_vars,
        var_name="col",
        value_name="value",
    )
    long = long.dropna(subset=["value"])

    # "data.123.components.pm2_5" -> idx=123, field="components.pm2_5"
    extracted = long["col"].str.extract(r"^data\.(\d+)\.(.+)$")
    long["idx"] = extracted[0].astype(int)
    long["field"] = extracted[1]

    wide = long.pivot_table(
        index=["row_id", "lat", "lon", "idx"],
        columns="field",
        values="value",
        aggfunc="first",
    ).reset_index()

    wide = wide.rename(columns={
        "main.aqi": "aqi",
        "components.co": "co",
        "components.no": "no",
        "components.no2": "no2",
        "components.o3": "o3",
        "components.so2": "so2",
        "components.pm2_5": "pm2_5",
        "components.pm10": "pm10",
        "components.nh3": "nh3",
        "dt": "dt",
    })

    wide["city"] = wide.apply(lambda r: resolve_city(r["lat"], r["lon"]), axis=1)
    return wide


def clean(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = ["aqi", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", "dt", "lat", "lon"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["datetime_utc"] = df["dt"].apply(
        lambda x: datetime.fromtimestamp(x, tz=timezone.utc) if pd.notna(x) else pd.NaT
    )

    df = df.drop_duplicates(subset=["city", "dt"])
    df["has_missing_pollutant"] = df[["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]].isna().any(axis=1)

    df = df.sort_values(["city", "dt"]).reset_index(drop=True)

    ordered = ["city", "lat", "lon", "dt", "datetime_utc", "aqi",
               "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3",
               "has_missing_pollutant"]
    return df[ordered]


def main():
    parser = argparse.ArgumentParser(description="Reshape le CSV historique large en format long")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="./historical_clean.csv")
    args = parser.parse_args()

    print("[INFO] Reshape en cours (peut prendre quelques secondes)...")
    wide = reshape(args.input)
    print(f"[INFO] {len(wide)} mesures extraites avant nettoyage.")

    df_clean = clean(wide)
    print(f"[INFO] {len(df_clean)} mesures après nettoyage.")

    df_clean.to_csv(args.output, index=False)
    print(f"[OK] Fichier sauvegardé : {args.output}")

    print("\nAperçu par ville :")
    print(df_clean.groupby("city")["dt"].agg(["min", "max", "count"]))


if __name__ == "__main__":
    main()
