import pandas as pd
import pyspark
import dotenv
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import json
from tratamentos.Frequencia import Frequencia


def chamar_funcoes():
    print("y")


if __name__ == "__main__":
    dotenv.load_dotenv()
    frequencia = Frequencia()
    frequencia.__tratar_dado__()