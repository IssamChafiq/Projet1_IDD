import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import requests
import os

# ============================================================
# CONFIGURATION
# ============================================================
GEOJSON_URL = "https://datavaccin-covid.ameli.fr/explore/dataset/georef-france-commune-arrondissement-municipal/download/?format=geojson&timezone=Europe/Paris&lang=fr"
MAP_FILENAME = "communes-plm-arrondissements.geojson"
STOPS_FILE = "gtfs-stops-france-export-2026-01-13.csv"
OUTPUT_FILE = "arrets_par_commune_FINAL.csv"

# ============================================================
# TÉLÉCHARGEMENT DU RÉFÉRENTIEL GÉOGRAPHIQUE
# ============================================================
def download_map():
    if os.path.exists(MAP_FILENAME):
        print(f"Fichier cartographique {MAP_FILENAME} déjà présent.")
        return

    print(">> Téléchargement du référentiel (incluant Arrondissements PLM)...")
    try:
        response = requests.get(GEOJSON_URL, stream=True)
        response.raise_for_status()
        with open(MAP_FILENAME, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Téléchargement terminé.")
    except Exception as e:
        print(f"Erreur lors du téléchargement : {e}")
        exit()

# ============================================================
# TRAITEMENT DES ARRÊTS DE TRANSPORT
# ============================================================
def main():
    # --- A. Chargement de la Carte ---
    print(">> Chargement des données géographiques...")
    communes_gdf = gpd.read_file(MAP_FILENAME)
    
    # Identification de la colonne Code INSEE
    possible_cols = ['com_arm_code', 'com_code', 'insee_com', 'code_insee', 'codgeo']
    target_col = next((c for c in possible_cols if c in communes_gdf.columns), None)
    
    if target_col:
        print(f"   Colonne identifiée pour les codes INSEE : '{target_col}'.")
        communes_gdf = communes_gdf.rename(columns={target_col: 'CODE_INSEE_OFFICIAL'})
        communes_gdf = communes_gdf[['CODE_INSEE_OFFICIAL', 'geometry']]
    else:
        print(f"Erreur : Colonne code INSEE introuvable. Colonnes disponibles : {communes_gdf.columns}")
        return

    print(f"Carte chargée : {len(communes_gdf)} zones administratives.")

    # --- B. Chargement des Arrêts (avec nettoyage) ---
    print(f">> Chargement des arrêts depuis {STOPS_FILE}...")
    if not os.path.exists(STOPS_FILE):
        print("Erreur : Fichier d'arrêts introuvable.")
        return

    try:
        stops_df = pd.read_csv(STOPS_FILE, on_bad_lines='skip', low_memory=False)
    except:
        stops_df = pd.read_csv(STOPS_FILE, error_bad_lines=False, low_memory=False)

    initial_count = len(stops_df)
    stops_df = stops_df.dropna(subset=['stop_lat', 'stop_lon'])
    
    # CRITIQUE : Déduplication des arrêts physiques
    stops_df['lat_round'] = stops_df['stop_lat'].round(6)
    stops_df['lon_round'] = stops_df['stop_lon'].round(6)
    stops_df = stops_df.drop_duplicates(subset=['lat_round', 'lon_round'])
    
    print(f"Arrêts chargés : {initial_count} lignes")
    print(f"Arrêts physiques uniques : {len(stops_df)}")

    # --- C. Jointure Spatiale ---
    print(">> Conversion en géométries vectorielles...")
    geometry = [Point(lon, lat) for lon, lat in zip(stops_df['stop_lon'], stops_df['stop_lat'])]
    stops_gdf = gpd.GeoDataFrame(stops_df, geometry=geometry, crs="EPSG:4326")

    if communes_gdf.crs != stops_gdf.crs:
        print("   Alignement des projections CRS...")
        stops_gdf = stops_gdf.to_crs(communes_gdf.crs)

    print(">> Exécution de la Jointure Spatiale (Opération lourde ~1-2 min)...")
    joined = gpd.sjoin(stops_gdf, communes_gdf, how="left", predicate="within")

    # --- D. Comptage et Export ---
    print(">> Agrégation par commune...")
    counts = joined['CODE_INSEE_OFFICIAL'].value_counts().reset_index()
    counts.columns = ['code_commune_INSEE', 'nb_arrets']
    
    # --- MODIFICATION : FORÇAGE FORMAT 5 CHIFFRES ---
    print("Normalisation des codes INSEE")
    counts['code_commune_INSEE'] = counts['code_commune_INSEE'].astype(str).str.zfill(5)
    
    counts.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSUCCÈS ! Résultats sauvegardés dans {OUTPUT_FILE}")
    
    # tests
    print("Test Nice (Attendu: 06088) :")
    print(counts[counts['code_commune_INSEE'] == '06088'])
    print("Test Paris 15e (Attendu: 75115) :")
    print(counts[counts['code_commune_INSEE'] == '75115'])

if __name__ == "__main__":
    download_map()
    main()
