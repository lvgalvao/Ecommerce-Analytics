from pyspark.sql import SparkSession
from delta import *
import os


class CSVToDeltaWriter:
    """
    Classe para converter arquivos CSV em Delta Tables usando PySpark.

    Atributos:
        input_dirs (list): Lista de diretórios de entrada contendo arquivos CSV.
        output_dir (str): Diretório de saída para as Delta Tables.

    Métodos:
        create_output_dir: Cria o diretório de saída, se não existir.
        read_csv: Lê um arquivo CSV e retorna um DataFrame do Spark.
        write_to_delta: Escreve um DataFrame do Spark em uma Delta Table.
        execute: Executa o processo de conversão para todos os arquivos CSV nos diretórios de entrada.
    """

    def __init__(self, input_dirs, output_dir):
        """
        Inicializa a classe CSVToDeltaWriter com diretórios de entrada e saída.

        Parâmetros:
            input_dirs (list): Lista de diretórios de entrada contendo arquivos CSV.
            output_dir (str): Diretório de saída para as Delta Tables.
        """
        self.input_dirs = input_dirs
        self.output_dir = output_dir
        self.create_output_dir()

    def create_output_dir(self):
        """Cria o diretório de saída, se ele não existir."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def read_csv(self, spark, file_path):
        """
        Lê um arquivo CSV usando o Spark e retorna um DataFrame.

        Parâmetros:
            spark (SparkSession): A sessão do Spark.
            file_path (str): Caminho do arquivo CSV a ser lido.

        Retorna:
            DataFrame: DataFrame do Spark contendo os dados do CSV.
        """
        return spark.read.csv(file_path, header=True, inferSchema=True)

    def write_to_delta(self, df, output_path):
        """
        Escreve um DataFrame do Spark em uma Delta Table.

        Parâmetros:
            df (DataFrame): DataFrame do Spark a ser escrito.
            output_path (str): Caminho da Delta Table de saída.
        """
        df.write.format("delta").mode("overwrite").save(output_path)

    def execute(self):
        """
        Executa o processo de leitura de arquivos CSV e gravação em Delta Tables.
        Percorre todos os arquivos nos diretórios de entrada especificados.
        """
        builder = (
            SparkSession.builder.appName("MyApp")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        for input_dir in self.input_dirs:
            for file_name in os.listdir(input_dir):
                if file_name.endswith(".csv"):
                    file_path = os.path.join(input_dir, file_name)
                    df = self.read_csv(spark, file_path)
                    delta_file_name = os.path.splitext(file_name)[0] + ".delta"
                    output_path = os.path.join(self.output_dir, delta_file_name)
                    self.write_to_delta(df, output_path)

        spark.stop()


if __name__ == "__main__":
    input_dirs = ["data/input_marketing", "data/input_olist"]
    output_dir = "data/output_delta"

    writer = CSVToDeltaWriter(input_dirs, output_dir)
    writer.execute()
