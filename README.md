Petit script permettant de chercher les orthographes les plus utilisées pour un prénom donné.

Le fichier variables.txt contient 3 paramètres : 
- le chemin du fichier d'entrée, en l'occurence le fichier des prénoms de l'Insee
- le chemin de sortie
- les clés de tri 

en sortie, le script renvoie toutes les lignes où la valeur de la colonne prénom matche une des clés de tri, même partiellement et de manière insensible à la casse.
J'ai personnellement ensuite utilisé un tableur dynamique sur LibreOffice Calc pour agréger les données ensuite. 
