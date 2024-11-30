import dask.dataframe as dd
from unidecode import unidecode

def lire_variables(fichier_config):
    """Lit les variables depuis un fichier texte."""
    variables = {}
    with open(fichier_config, 'r') as f:
        for ligne in f:
            if '=' in ligne:
                cle, valeur = ligne.strip().split('=', 1)
                variables[cle] = valeur
    return variables


def normaliser(texte):
    """Normalise un texte en supprimant les diacritiques et en le mettant en minuscules."""
    return unidecode(str(texte)).lower()

def filtrer_csv(input_file, output_file, keys, colonne=1):
    """
    Filtre un fichier CSV en fonction des clés spécifiées dans une colonne donnée.
    La comparaison est insensible à la casse et aux diacritiques.

    Args:
        input_file (str): Chemin du fichier d'entrée.
        output_file (str): Chemin du fichier de sortie.
        keys (list): Clés à filtrer.
        colonne (int): Index de la colonne (commence à 0) à vérifier.
    """
    # Charger le fichier CSV avec Dask

    df = dd.read_csv(input_file, sep=';', assume_missing=True)

    #Affiche le nom des colonnes du CSV
    print("Colonnes détectées :", df.columns)
    print("Headers :", df.head())


    # Identifier la colonne cible (les colonnes commencent à 0)
    nom_colonne = df.columns[colonne]

    # Normaliser les clés de filtrage
    keys_normalisees = set(normaliser(key) for key in keys)

    # Ajouter une colonne normalisée pour le filtrage
    df["colonne_normalisee"] = df[nom_colonne].map(normaliser, meta=('x', 'str'))

    # Construire une condition pour la correspondance partielle
    condition = df["colonne_normalisee"].map(lambda x: any(key in x for key in keys_normalisees), meta=('x', 'bool'))

    # Filtrer les lignes
    df_filtre = df[condition]

    # Sauvegarder le résultat
    df_filtre = df_filtre.drop("colonne_normalisee", axis=1)  # Retirer la colonne temporaire
    df_filtre.to_csv(output_file, single_file=True, index=False)
    print(f"Fichier filtré sauvegardé dans : {output_file}")

if __name__ == "__main__":
    # Charger les variables
    config = lire_variables("variables.txt")
    input_file = config["input_file"]
    output_file = config["output_file"]
    keys = config["keys"].split(',')

    # Appliquer le filtrage
    filtrer_csv(input_file, output_file, keys)

#test : premier commit depuis VScode