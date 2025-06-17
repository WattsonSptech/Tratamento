import dotenv
from tratamentos.Frequencia import Frequencia
from tratamentos.Harmonica import Harmonica
from tratamentos.Temperatura import Temperatura

from tratamentos.Corrente import Corrente

from client.Client import Client
import schedule
import time


def chamar_funcoes():
    #client tem sempre que ser o ultimo!
    sensores = (Frequencia, Harmonica, Temperatura,Corrente, Client)
    for sensor in sensores:
        sensor().__tratar_dado__()

if __name__ == "__main__":
    dotenv.load_dotenv()
    chamar_funcoes()
    schedule.every(5).minutes.do(chamar_funcoes)
    while True:
        schedule.run_pending()
        time.sleep(10)