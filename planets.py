from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import DateType
from pyspark.ml.feature import StringIndexer, Imputer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
import pandas as pd

def use_planets(data):
    #création session spark
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df_inter=pd.DataFrame(data,index=[data.pop("planete_number")])
    df = spark.createDataFrame(df_inter)
    #drop colonnes inutiles
    compact_data = df.drop('P_GEO_ALBEDO', 'P_DETECTION_MASS', 'P_DETECTION_RADIUS', 'P_ALT_NAMES', 'P_ATMOSPHERE',
                           'S_DISC', 'S_MAGNETIC_FIELD',
                           'P_TEMP_MEASURED', 'P_TPERI',
                           'P_DENSITY', 'P_ESCAPE', 'P_GRAVITY', 'P_POTENTIAL', 'P_OMEGA', 'P_INCLINATION',
                           'P_IMPACT_PARAMETER', 'P_HILL_SPHERE', 'P_UPDATE',
                           'P_MASS')

    error_columns = [col for col in compact_data.columns if 'ERROR' in col]
    df_cleaned = compact_data.drop(*error_columns)
    #remplir les valeurs manquantes avec la moyenne
    mean_values = {
        col_name: df_cleaned.select(avg(col_name)).first()[0]
        for col_name, dtype in df_cleaned.dtypes
        if dtype != 'string' and not isinstance(df_cleaned.schema[col_name].dataType, DateType)
    }
    compact_data = df_cleaned.na.fill(mean_values)
    # Identifier les colonnes catégorielles
    categorical_columns = [col_name for col_name, dtype in compact_data.dtypes if dtype == 'string']

    # Créer un indexeur pour chaque colonne catégorielle
    indexers = [StringIndexer(inputCol=column, outputCol=column + "_indexed").setHandleInvalid("keep") for column in
                categorical_columns]

    # Créer le pipeline
    pipeline = Pipeline(stages=indexers)

    # Appliquer le pipeline au DataFrame
    compact_data = pipeline.fit(compact_data).transform(compact_data)

    # supprimer les anciennes colonnes catégorielles si nécessaire
    for column in categorical_columns:
        compact_data = compact_data.drop(column)

    # Identifier les colonnes numériques pour l'imputation
    numeric_cols = [col_name for col_name, dtype in compact_data.dtypes if dtype in ['int', 'double']]

    # Créer un imputer pour les colonnes numériques identifiées
    imputer = Imputer(
        inputCols=numeric_cols,
        outputCols=["{}_imputed".format(col_name) for col_name in numeric_cols]
    ).setStrategy("mean")  # ou "median" selon le cas d'usage

    # Appliquer l'imputation
    compact_data = imputer.fit(compact_data).transform(compact_data)
    # Identifier les colonnes numériques
    numeric_cols = [col_name for col_name, dtype in compact_data.dtypes if dtype in ['int', 'double']]

    # Assembler les colonnes numériques dans un vecteur
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    feature_vector_df = assembler.transform(compact_data)

    # Calculer la matrice de corrélation
    correlation_matrix = Correlation.corr(feature_vector_df, "features").head()[0]

    # Extraire la matrice de corrélation en tant que tableau Numpy (si nécessaire)
    correlation_array = correlation_matrix.toArray()
    # Seuil de corrélation haute pour retirer les colonnes
    high_corr_threshold = 0.9
    cols_to_remove = []

    for i in range(len(correlation_array)):
        for j in range(i + 1, len(correlation_array)):
            if abs(correlation_array[i, j]) > high_corr_threshold:
                cols_to_remove.append(numeric_cols[j])

    # Supprimer les colonnes sélectionnées
    cols_to_remove = list(set(cols_to_remove))  # Enlever les doublons
    compact_data = compact_data.drop(*cols_to_remove)
    return compact_data
