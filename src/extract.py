from pyspark.sql import SparkSession, DataFrame
import os


class ReadCsv:
    """
    Classe para ler arquivos CSV e retornar DataFrames do PySpark.
    """

    def __init__(self, input_dirs):
        """
        Inicializa a classe ReadCsv com diretórios de entrada.

        Parâmetros:
            input_dirs (list): Lista de diretórios de entrada contendo arquivos CSV.
        """
        self.input_dirs = input_dirs

    def read_csv(self, spark: SparkSession, file_path: str) -> DataFrame:
        """
        Lê um arquivo CSV usando o Spark e retorna um DataFrame.

        Parâmetros:
            spark (SparkSession): A sessão do Spark.
            file_path (str): Caminho do arquivo CSV a ser lido.

        Retorna:
            DataFrame: DataFrame do Spark contendo os dados do CSV.
        """
        return spark.read.csv(file_path, header=True, inferSchema=True)

    def execute(self):
        """
        Executa o processo de leitura de arquivos CSV.

        Retorna:
            List[DataFrame]: Lista de DataFrames do Spark.
        """
        spark = SparkSession.builder.appName("ReadCsv").getOrCreate()
        dataframes = []

        for input_dir in self.input_dirs:
            for file_name in os.listdir(input_dir):
                if file_name.endswith(".csv"):
                    file_path = os.path.join(input_dir, file_name)
                    df = self.read_csv(spark, file_path)
                    dataframes.append(df)

        spark.stop()
        return dataframes
