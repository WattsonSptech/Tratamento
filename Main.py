import dotenv
from tratamentos.Frequencia import Frequencia
from tratamentos.Harmonica import Harmonica
from tratamentos.Temperatura import Temperatura
import schedule
import time


def chamar_funcoes():
    sensores = (Frequencia, Harmonica, Temperatura)
    for sensor in sensores:
        sensor().__tratar_dado__()

if __name__ == "__main__":
    dotenv.load_dotenv()
    chamar_funcoes()
    schedule.every(5).minutes.do(chamar_funcoes)
    while True:
        schedule.run_pending()
        time.sleep(10)