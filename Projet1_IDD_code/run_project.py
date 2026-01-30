import subprocess
import sys
import os

# Liste des scripts à exécuter dans l'ordre
SCRIPTS = [
    "TransStops.py",
    "Union.py",
    "VIsual1.py"
]

def run_script(script_name):
    script_path = os.path.join(os.getcwd(), script_name)
    
    if not os.path.exists(script_path):
        print(f"Erreur : '{script_name}' introuvable.")
        sys.exit(1)

    print(f">> Execution de {script_name}...")

    try:
        subprocess.run([sys.executable, script_name], check=True)
    except subprocess.CalledProcessError:
        print(f"Echec de l'execution de {script_name}.")
        sys.exit(1)
    except Exception as e:
        print(f"Erreur : {e}")
        sys.exit(1)

if __name__ == "__main__":
    for script in SCRIPTS:
        run_script(script)

    print("Project termine avec succes.")