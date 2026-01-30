import pandas as pd

print(">> Initialisation du chargement...")

# ==========================================
# CHARGEMENT POPULATION & DÉPARTEMENTS
# ==========================================
# Chargement des données communales
pop_df = pd.read_excel('ensemble.xlsx', sheet_name='Communes', skiprows=7)

# Chargement référentiel départements pour lisibilité
dept_df = pd.read_excel('ensemble.xlsx', sheet_name='Départements', skiprows=7)
dept_df = dept_df[['Code département', 'Nom du département']]
# Conversion explicite en chaînes (str) pour jointure
dept_df['Code département'] = dept_df['Code département'].astype(str)
pop_df['Code département'] = pop_df['Code département'].astype(str)

# Jointure pour intégrer le nom du département
pop_df = pd.merge(pop_df, dept_df, on='Code département', how='left')

# --- FILTRE : EXCLUSION DOM-TOM ---
pop_df = pop_df[~pop_df['Code département'].str.startswith(('97', '98'))]

def build_metropolitan_code(row):
    dept = row['Code département'].strip()
    commune = str(row['Code commune']).strip()
    if len(dept) == 1: dept = '0' + dept
    commune = commune.zfill(3)
    return dept + commune

pop_df['code_insee'] = pop_df.apply(build_metropolitan_code, axis=1)

# Sélection et renommage des colonnes cibles
pop_clean = pop_df[['code_insee', 'Nom de la commune', 'Nom du département', 'Population totale']].rename(
    columns={'Nom de la commune': 'nom_commune', 
             'Nom du département': 'nom_departement',
             'Population totale': 'population'}
)
print(f">> Population chargée : {len(pop_clean)} communes")

# ==========================================
# CHARGEMENT REVENUS (Source: FILO2021)
# ==========================================
print(">> Chargement données revenus...")
rev_df = pd.read_excel('FILO2021_DEC_COM.xlsx', sheet_name='ENSEMBLE', skiprows=5, engine='calamine')

rev_df['CODGEO'] = rev_df['CODGEO'].astype(str).str.zfill(5)
rev_df['Q221'] = pd.to_numeric(rev_df['Q221'], errors='coerce')

rev_clean = rev_df[['CODGEO', 'Q221']].rename(
    columns={'CODGEO': 'code_insee', 'Q221': 'revenu_median'}
)
print(f">> Revenus chargés.")

# ==========================================
# CHARGEMENT ARRÊTS (Source: GTFS)
# ==========================================
print(">> Chargement données transport...")
stops_df = pd.read_csv('arrets_par_commune_FINAL.csv', dtype={'code_commune_INSEE': str})

stops_clean = stops_df[['code_commune_INSEE', 'nb_arrets']].rename(
    columns={'code_commune_INSEE': 'code_insee'}
)
print(f">> Arrêts chargés.")

# ==========================================
# FUSION DES JEUX DE DONNÉES
# ==========================================
print(">> Exécution des jointures...")

merged_1 = pd.merge(pop_clean, rev_clean, on='code_insee', how='left')
final_df = pd.merge(merged_1, stops_clean, on='code_insee', how='left')

# Gestion valeurs manquantes (0 si pas d'arrêt)
final_df['nb_arrets'] = final_df['nb_arrets'].fillna(0).astype(int)

# ==========================================
# CALCUL SCORE ATTRACTIVITÉ
# ==========================================
print(">> Calcul algorithmique du score...")

# Fonction utilitaire : Normalisation Min-Max (0-1)
def normalize(series):
    return (series - series.min()) / (series.max() - series.min())

# Copie pour calculs intermédiaires
# .copy() garantit l'intégrité des données
calc_df = final_df.copy()

calc_df['norm_inc'] = normalize(calc_df['revenu_median'])
calc_df['norm_pop'] = normalize(calc_df['population'])
calc_df['norm_stops'] = normalize(calc_df['nb_arrets'])

# Pondérations : Revenu (50%), Pop (40%), Transport (10%)
w_income = 0.5
w_pop = 0.4
w_stops = 0.1

final_df['score_attractivite'] = (
    (w_income * calc_df['norm_inc']) + 
    (w_pop * calc_df['norm_pop']) + 
    (w_stops * calc_df['norm_stops'])
) * 100 # Mise à l'échelle 100

# Arrondi de propreté
final_df['score_attractivite'] = final_df['score_attractivite'].round(2)

# Tri décroissant par Score
final_df = final_df.sort_values('score_attractivite', ascending=False)

# Mise en forme finale des colonnes
cols = ['code_insee', 'nom_commune', 'nom_departement', 'population', 'revenu_median', 'nb_arrets', 'score_attractivite']
final_df = final_df[cols]

# ==========================================
# EXPORT
# ==========================================
output_file = 'population_revenu_transport.xlsx'
final_df.to_excel(output_file, index=False)

print("\n>> SUCCÈS !")
print(f">> Fichier généré : {output_file}")
print("\n>> Top 10 Communes les plus attractives :")
print(final_df.head(10).to_string(index=False))