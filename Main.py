import os.path
import dotenv
from glob import glob
from tratamentos.Frequencia import Frequencia
from tratamentos.Harmonica import Harmonica
from tratamentos.Tensao import Tensao


def chamar_funcoes():
    sensores = (Frequencia, Harmonica, Tensao)
    for sensor in sensores:
        try:
            print(f"\n\tIniciando tratamento de {sensor.__name__}...\n")
            sensor().__tratar_dado__()
        except Exception:
            print(f"\t\033[31m[!] Erro no sensor de {sensor.__name__}!\033[0m")

if __name__ == "__main__":
    dotenv.load_dotenv()

    if not os.path.exists("./temp"):
        os.mkdir("./temp")

    chamar_funcoes()

    [os.remove(f) for f in glob("./temp/*")]