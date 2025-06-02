import pandas as pd
import pyspark
import dotenv
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import json
from tratamentos.Frequencia import Frequencia
from tratamentos.Harmonica import Harmonica


def chamar_funcoes():
    print("y")


if __name__ == "__main__":
    dotenv.load_dotenv()
    # frequencia = Frequencia()
    # frequencia.__tratar_dado__("arquivo_exemplo")
    harmonica = Harmonica()
    harmonica.__tratar_dado__("generation-01-06-2025_17-29-22.723.json")