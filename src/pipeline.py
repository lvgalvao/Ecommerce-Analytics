from extract import ReadCsv
from load import LoadToAWS


class CsvToAwsPipeline:
    """
    Classe de pipeline para ler arquivos CSV e carregar os dados para a AWS.
    """

    def __init__(self, input_dirs, destinations):
        """
        Inicializa o pipeline.

        Parâmetros:
            input_dirs (list): Lista de diretórios de entrada contendo arquivos CSV.
            destinations (list): Lista de destinos correspondentes na AWS para cada DataFrame.
        """
        self.input_dirs = input_dirs
        self.destinations = destinations

    def run(self):
        # Etapa 1: Ler arquivos CSV
        reader = ReadCsv(self.input_dirs)
        dataframes = reader.execute()

        # Verificação se a lista de destinos corresponde ao número de DataFrames
        if len(dataframes) != len(self.destinations):
            raise ValueError(
                "O número de destinos deve corresponder ao número de DataFrames lidos."
            )

        # Etapa 2: Carregar para a AWS
        loader = LoadToAWS()
        loader.execute(dataframes, self.destinations)


# Exemplo de uso
if __name__ == "__main__":
    input_dirs = ["data/external/input_marketing", "data/external/input_olist"]
    destinations = ["data/lakehouse/input_marketing", "data/lakehouse/input_olist"]

    pipeline = CsvToAwsPipeline(input_dirs, destinations)
    pipeline.run()
