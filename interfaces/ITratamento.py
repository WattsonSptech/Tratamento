from abc import abstractmethod
from utils.Utils import Utils

class ITratamentoDados:
    nome_sensor: str
    tipo_dado: str
    outlier_min: float
    outlier_max: float

    def __init__(self) -> None:
        self.utils = Utils()
        self.spark = self.utils.init_spark()

    @abstractmethod
    def __tratar_dado__(self) -> None:
        pass