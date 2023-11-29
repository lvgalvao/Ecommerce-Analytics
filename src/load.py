from pyspark.sql import DataFrame


class LoadToAWS:
    """
    Classe para carregar DataFrames do PySpark para a AWS.
    """

    def __init__(self):
        pass

    def load_to_aws(self, df: DataFrame, destination: str):
        """
        Carrega um DataFrame do PySpark para a AWS.

        Parâmetros:
            df (DataFrame): DataFrame do Spark a ser carregado.
            destination (str): Destino na AWS onde os dados devem ser carregados.
        """
        # Substitua "delta" pelo formato desejado, como "parquet" ou "csv"
        # E certifique-se de que o destino é um caminho válido na AWS (ex: "s3://bucket/folder")
        df.write.format("parquet").mode("overwrite").save(destination)

    def execute(self, dataframes, destinations):
        """
        Executa o processo de carregamento de DataFrames para a AWS.

        Parâmetros:
            dataframes (List[DataFrame]): Lista de DataFrames para carregar.
            destinations (List[str]): Lista de destinos correspondentes na AWS.
        """
        for df, dest in zip(dataframes, destinations):
            self.load_to_aws(df, dest)
