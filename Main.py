import os.path
import dotenv
from glob import glob
from tqdm import tqdm
from client.Client import Client
from tratamentos.Corrente import Corrente
from tratamentos.Frequencia import Frequencia
from tratamentos.Harmonica import Harmonica
from tratamentos.Tensao import Tensao
from tratamentos.Temperatura import Temperatura
from tratamentos.FatorPotencia import FatorPotencia
import time


def chamar_funcoes(dev_mode):
    sensores = (Frequencia, Harmonica, Tensao, Temperatura, Corrente, FatorPotencia, Client)
    for sensor in sensores:
        try:
            print(f"\n\tIniciando tratamento de {sensor.__name__}...\n")
            sensor().__tratar_dado__()
        except Exception as e:
            print(f"\t\033[31m[!] Erro no sensor de {sensor.__name__}!\033[0m")
            if dev_mode:
                raise e

if __name__ == "__main__":
    dotenv.load_dotenv()
    dev_mode = os.getenv("DEV_MODE", 0) == "1"
    treatment_timeout = int(os.getenv("TREATMENT_TIMEOUT", "50"))

    if not os.path.exists("./temp"):
        os.mkdir("./temp")

    while True:
        try:
            [os.remove(f) for f in glob("./temp/*")]
        except PermissionError as e:
            print(e)

        chamar_funcoes(dev_mode)
        print("Tratamentos concluídos!")

        print("\nTempo para a próxima execução...")
        for i in tqdm(range(treatment_timeout)):
            time.sleep(1)