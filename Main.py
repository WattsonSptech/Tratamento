import os.path
import dotenv
from glob import glob
from tqdm import tqdm
import time
from tratamentos.Tensao import Tensao
from bases_externas.Clima import Clima
from bases_externas.ReclameAqui import ReclameAqui
from bases_externas.GeracaoEnergia import GeracaoEnergia
from client.FatoTensaoClima import FatoTensaoClima
from client.FatoConsumo import FatoConsumo
from client.FatoReclamacao import FatoReclamacao


def chamar_funcoes_tratamento():
    sensores = (Tensao, GeracaoEnergia, Clima, ReclameAqui)

    for sensor in sensores:
        try:
            print(f"\n\tIniciando tratamento de {sensor.__name__}...\n")
            sensor().__tratar_dado__()
        except Exception as e:
            print(f"\t\033[31m[!] Erro no sensor de {sensor.__name__}!\033[0m")
            print(e)
    

def chamar_funcoes_tabelas_flat():
    flats = (FatoTensaoClima, FatoConsumo, FatoReclamacao)
    
    for flat in flats:
        try:
            print(f"\n\tIniciando tratamento da tabela {flat.__name__}...\n")
            flat.__gerar_tabela_fato__()

        except Exception as e:
            print(f"\t\033[31m[!] Erro no flat {flat.__name__}!\033[0m")
            print(e)


if __name__ == "__main__":
    dotenv.load_dotenv()
    treatment_timeout = int(os.getenv("TREATMENT_TIMEOUT", "50"))

    if not os.path.exists("./temp"):
        os.mkdir("./temp")

    while True:
        try:
            [os.remove(f) for f in glob("./temp/*")]
        except PermissionError as e:
            print(e)

        chamar_funcoes_tratamento()
        print("Tratamentos concluídos!")

        chamar_funcoes_tabelas_flat()
        print("Criação das tabelas flat concluída!")

        print("\nTempo para a próxima execução...")
        for i in tqdm(range(treatment_timeout)):
            time.sleep(1)