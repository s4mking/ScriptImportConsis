# ScriptImportConsis

How to run this script

## Usage

```bash
python3 -m venv script
source script/bin/activate
pip install -r requirements.txt
python3 scriptImportConsistoire.py
```

## But du script

Ce script sert à mettre les jours les communautés ainsi que les consistoires accessibles via ces 2 urls :
[Consistoires](http://www.consistoire.org/getJson?f=_consistoire)
[Communautés](http://www.consistoire.org/getJson?f=_communaute)

Tous les champs postMetas seront mis à jour ou créés si ils n'existent pas. Certains champs sont en durs dans le script:

* Si la commuanuté ne comporte pas de nom alors elle est nommée synagogue
* Une liste des différents régions est mis en place dans le fichier sous la variable regions ligne 321
* Si une ville à laquelle doit être rattaché une communauté n'est pas trouvé alors on ne modifie ou on ne créé rien. Les villes ne sont pas gérées par ce script et doivent être créées en amont.
* Le champ image-principale des consistoires est toujours empli à 207 pour l'instant.