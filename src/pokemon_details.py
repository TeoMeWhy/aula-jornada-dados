# Databricks notebook source
import datetime
import requests
import json
from tqdm import tqdm

from multiprocessing import Pool

def get_and_save(url):
    data = requests.get(url).json()
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"/Volumes/raw/pokemon/pokemo_raw/pokemons_details/{data['id']}_{now}.json"
    with open(filename, "w") as open_file:
        json.dump(data, open_file)

df = spark.table("bronze.pokemon.pokemon_list")
urls = df.select("url").toPandas()["url"].tolist()

with Pool(4) as p:
    print(p.map(get_and_save, urls))


# COMMAND ----------

df = spark.read.json("/Volumes/raw/pokemon/pokemo_raw/pokemons_details/")
df.display()
