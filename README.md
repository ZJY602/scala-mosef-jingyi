# Projet Scala MOSEF Yao ZHANG & Jingyi ZHOU

Ce projet contient un ensemble de fonctionnalités Scala pour le traitement de données. Il utilise Spark pour le traitement des données CSV et Parquet.

**Remarque :**
1. Pour le traitement des données csv, utilisez cette commande :

java -cp scala-mosef-jingyi-1.3-jar-with-dependencies.jar fr.mosef.scala.template.Main local[1] ./input/test1.csv default

Cette commande exécutera le traitement sur le fichier CSV spécifié et générera les résultats dans le dossier "output-csv".

2. Pour le traitement des données Parquet, utilisez la commande suivante :


java -cp scala-mosef-jingyi-1.3-jar-with-dependencies.jar fr.mosef.scala.template.Main local[1] ./input/Iris.parquet --parquet


Cette commande exécutera le traitement sur le fichier Parquet spécifié et générera les résultats correspondants.
