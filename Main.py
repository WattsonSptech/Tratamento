import os.path
import dotenv
from glob import glob
from tqdm import tqdm
import time
from tratamentos.Tensao import Tensao
from bases_externas.Clima import Clima
from bases_externas.ReclameAqui import ReclameAqui
# from bases_externas.ElecriticityGeneration import ElecriticityGeneration
from bases_externas.GeracaoEnergia import GeracaoEnergia


def chamar_funcoes(dev_mode):
    # sensores = (Tensao, GeracaoEnergia, Clima, ReclameAqui)
    sensores = [Clima]
    for sensor in sensores:
        try:
            print(f"\n\tIniciando tratamento de {sensor.__name__}...\n")
            sensor().__tratar_dado__()
        except Exception as e:
            print(f"\t\033[31m[!] Erro no sensor de {sensor.__name__}!\033[0m")
            print(e)
            # if dev_mode:
            #     raise e

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