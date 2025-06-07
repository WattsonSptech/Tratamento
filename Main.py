import dotenv
from tratamentos.Frequencia import Frequencia
from tratamentos.Harmonica import Harmonica


def chamar_funcoes():
    sensores = (Frequencia, Harmonica)
    for sensor in sensores:
        sensor().__tratar_dado__()

if __name__ == "__main__":
    dotenv.load_dotenv()
    chamar_funcoes()