import geopandas as gpd
import pandas as pd
import mapclassify 

# ==============================================================================
# CONFIGURATION ET CHARGEMENT
# ==============================================================================
INPUT_GEOJSON = "communes-plm-arrondissements.geojson"
INPUT_EXCEL = "population_revenu_transport.xlsx"
OUTPUT_HTML = "carte_attractivite.html"

print(">> Chargement des données...")
map_df = gpd.read_file(INPUT_GEOJSON)
data_df = pd.read_excel(INPUT_EXCEL)

# ==============================================================================
# PRÉPARATION (JOINTURE ROBUSTE)
# ==============================================================================

# 1. Détection colonne code INSEE
priority_cols = ['com_arm_code', 'codgeo', 'CODGEO', 'code_insee', 'insee', 'code']
map_code_col = next((c for c in priority_cols if c in map_df.columns), None)

if not map_code_col:
    raise ValueError(f"Erreur : Colonne INSEE introuvable dans le GeoJSON.")

# 2. Filtrage France Métropolitaine (Exclusion DOM/TOM)
map_df = map_df[~map_df[map_code_col].astype(str).str.startswith(('97', '98'))]

# 3. Normalisation des clés (5 caractères string)
map_df['join_key'] = map_df[map_code_col].astype(str).str.zfill(5)
data_df['join_key'] = data_df['code_insee'].astype(str).str.zfill(5)

# 4. Fusion
print(">> Fusion des tables...")
merged = map_df.merge(data_df, on='join_key', how='left')

# 5. Nettoyage
merged['score_attractivite'] = merged['score_attractivite'].fillna(0)

# ==============================================================================
# GÉNÉRATION DE LA CARTE (ALGORITHME JENKS / NATURAL BREAKS)
# ==============================================================================
print(">> Calcul des classes statistiques ")

# Documentation des nouveaux paramètres :
# - scheme='natural_breaks' (ou Fisher-Jenks) : Contrairement aux quantiles, cette méthode 
#   respecte la distribution réelle. Les communes "jaunes" seront uniquement celles 
#   avec un score objectivement très haut, pas juste le "top 10%".
# - k=15 : Un nombre élevé de classes permet de "casser" (break down) les groupes moyens
#   pour révéler les subtilités locales sans saturer la carte.

m = merged.explore(
    column="score_attractivite",        
    cmap="plasma",                      
    scheme="natural_breaks",            
    k=15,                               
    tooltip=[                           
        "nom_commune", 
        "nom_departement", 
        "score_attractivite", 
        "population", 
        "revenu_median",
        "nb_arrets"
    ],
    tooltip_kwds={"labels": True},      
    tiles="CartoDB positron",           
    style_kwds={
        "weight": 0.3,
        "fillOpacity": 0.8
    },
    legend_kwds={
        "caption": "Score d'Attractivité ",
        "colorbar": False
    }
)

# ==============================================================================
# EXPORT
# ==============================================================================
m.save(OUTPUT_HTML)
print(f">> Terminé.")
print(f">> Carte enregistrée sous : {OUTPUT_HTML}")