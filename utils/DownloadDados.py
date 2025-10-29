import requests
import basedosdados as bd
import json
# import kagglehub


class DownloadDados:
    def __init__(self) -> None:
        print("Contato com download")

    def consultarPorUrl(self, url):
        response = requests.get(url).text
        print(response)
        data = json.loads(response)
        print(data)
        return data
    
    def consultarPorQueryBase(self, query):
        query = """
        SELECT
            dados.ano as ano,
            dados.mes as mes,
            dados.sigla_uf AS sigla_uf,
            diretorio_sigla_uf.nome AS sigla_uf_nome,
            dados.tipo_consumo as tipo_consumo,
            dados.numero_consumidores as numero_consumidores,
            dados.consumo as consumo
        FROM `basedosdados.br_mme_consumo_energia_eletrica.uf` AS dados
        LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
            ON dados.sigla_uf = diretorio_sigla_uf.sigla
        """

        return bd.read_sql(query = query, billing_project_id='projeto-consulta-wattson')
    
    # def consultarKaggle(self):
    #     path = kagglehub.dataset_download("arusouza/daily-eletricity-generation-by-source-on-brazil")
    #     print("Path to dataset files:", path)
    #     return path + "/daily_eletricity_generation_by_source_brazil.csv"
