# directory_database

Ces scripts facilitent le recueil de statistiques sur les volumes occupés par un ensemble de répertoires, leurs id utilisateurs associés, incluant également des fonctionnalités tels que le montage virtuel d’arborescences.

Le script ddb_fullscan recueille pour chaque dossier existant dans un dossier passé en paramètre le volume occupé, les id utilisateurs associés et stocke l’ensemble dans une base de données sqlite. L’opération peut être longue, se divise en plusieurs processus parallèles et produit en sortie une base par dossier. La commande se lance de la façon suivante :

```bash
cd directory_database
./ddb_fullscan dossier_de_destination_des_fichiers_sqlite_en_sortie
```

Quand il a terminé, il y a une commande à lancer pour fusionner l’ensemble des bases créées dans un sqlite global (-m pour merge).

```bash
./ddb_fullscan -m chemin_vers_sqlite_global dossier_de_destination_des_fichiers_sqlite_en_sortie
```

Il y a un script pour faire des statistiques (il peut servir d'exemple d'utilisation de la base sqlite):

```bash
ddb_stats -u Users.sqlite
```

L’option -u affiche les informations relatives aux id utilisateurs, l’option -s affiche des informations sur les sous-dossiers de la base traitée (jusqu’à un niveau de sous-dossier).

On peut lancer l’analyse d’un dossier spécifique à l’aide du script ddb_create comme dans l’exemple suivant :

```bash
ddb_stats -e tout_sous_dossier_à_exclure -v dossier_à_inspecter fichier_sqlite_créé_en_sortie
```

Enfin, il est possible de voir un des fichiers sqlite comme un répertoire (sans avoir accès au contenu des fichiers) en lançant la commande suivante (la commande doit tourner en parallèle tant qu’on souhaite accéder au répertoire) :

```bash
mkdir /tmp/mount_point
ddb_mount chemin_vers_sqlite_global /tmp/mount_point
```

NB : la dernière commande nécessite le package python-fuse sous Ubuntu 12.04 (et fusepy sous pip)


Credits: 
- Y. Cointepas
- G. Operto
