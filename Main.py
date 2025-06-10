import os.path
import dotenv
from glob import glob
from tratamentos.Frequencia import Frequencia
from tratamentos.Harmonica import Harmonica
from tratamentos.Tensao import Tensao


def chamar_funcoes():
    sensores = (Frequencia, Harmonica, Tensao)
    for sensor in sensores:
        print(f"\tIniciando tratamento de {sensor.__class__.__name__}...")
        sensor().__tratar_dado__()

if __name__ == "__main__":
    dotenv.load_dotenv()

    if not os.path.exists("./temp"):
        os.mkdir("./temp")

    chamar_funcoes()

    [os.remove(f) for f in glob("./temp/*")]