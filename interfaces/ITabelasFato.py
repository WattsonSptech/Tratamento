from abc import abstractmethod
from utils.Utils import Utils
from pandas import DataFrame

class ITabelasFato:
   
    def __init__(self) -> None:
        self.utils = Utils()
        self.spark = self.utils.init_spark()

    @abstractmethod
    def __gerar_tabela_fato__(self) -> None:
        pass

    @abstractmethod
    def __salvar_flat_na_s3__(self, df: DataFrame, path: str) -> None:
        pass