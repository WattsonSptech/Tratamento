from interfaces.ITratamento import ITratamentoDados
import pandas as pd
from interfaces.EnumBuckets import EnumBuckets
import unicodedata
import re
from datetime import datetime

class ReclameAqui(ITratamentoDados):
    
    def __init__(self):
        super().__init__()
        
    
    def __tratar_dado__(self):

        df = pd.read_csv(self.utils.get_data_s3_csv(EnumBuckets.RAW.value, "ReclameAqui_Raw_"), sep=";")


        df['CIDADE'] = self.__remover_acentos__(df['CIDADE'])

        df = df.loc[df['UF'] == 'SP']
        df = df.loc[df['CIDADE'] == 'SAO PAULO']

        df['STATUS'] = self.__remover_acentos__(df['STATUS'])
        df['CATEGORIA'] = self.__remover_acentos__(df['CATEGORIA'])
        df['TIPO_PRODUTO'] = self.__remover_acentos__(df['TIPO_PRODUTO'])
        df['TIPO_PROBLEMA'] = self.__remover_acentos__(df['TIPO_PROBLEMA'])
        df['SENTIMENTO_FRASE'] = self.__remover_acentos__(df['SENTIMENTO_FRASE'])
        df['DATA'] = self.__converter_data_para_formato_iso__(df['DATA'])

        df['DATA'] = pd.to_datetime(df['DATA'])

        df = df.drop(['Unnamed: 0', 'URL', 'TITULO', 'RECLAMACAO', 'UF', 'CIDADE'], axis=1)
        
        df['DATA_HORA_RECLAMACAO'] = self.__converter_para_timestamp__(df['DATA'], df['HORA'])
        df['DATA_HORA_RECLAMACAO'] = pd.to_datetime(df['DATA_HORA_RECLAMACAO'])

        df = df.rename(
            columns={
                'DATA': 'DATA_RECLAMACAO',
                'HORA': 'HORA_MINUTO_RECLAMACAO', 
                'SENTIMENTO_FRASE': 'RECLAMACAO_SENTIMENTO',
                'STATUS': 'RECLAMACAO_STATUS',
                'PRODUTO': 'RECLAMACAO_PRODUTO',
                'CATEGORIA': 'RECLAMACAO_CATEGORIA',
                'PROBLEMA': 'RECLAMACAO_PROBLEMA',
             }
            )

        self.__subir_dados_s3__(df)

    def __remover_acentos__(self, series):
        series_normalizada = []

        for texto in series:
            texto_normalizado = unicodedata.normalize('NFD', texto)
            texto_sem_acentos = re.sub(r'[\u0300-\u036f]', '', texto_normalizado)
            series_normalizada.append(texto_sem_acentos.upper())
        return series_normalizada

    def __converter_data_para_formato_iso__(self, series):
        series_normalizada = []

        for data in series:
            data_separada = str(data).split('/')
            data_separada.reverse()
            data_normalizada = '-'.join(data_separada)
            series_normalizada.append(data_normalizada)
        return series_normalizada
    
    def __converter_para_timestamp__(self, data, horario):
        series_normalizada = []
        
        if len(data) == len(horario):
        
            horas = [hora + ":00.00000" for hora in horario]
            datas = [item for item in data]

            for i in range (len(horas)):
                series_normalizada.append(f"{datas[i]} {horas[i]}")
                
        return series_normalizada

    def __subir_dados_s3__(self, dados: pd.DataFrame):
        
        ano = datetime.today().year
        mes = datetime.today().month
        dia = datetime.today().day

        filepath = "./temp/ReclameAqui_TRUSTED_{}{}{}.csv".format(ano, mes, dia)
        dados.to_csv(filepath, sep=";")
        self.utils.set_data_s3_file(filepath, EnumBuckets.TRUSTED.value)