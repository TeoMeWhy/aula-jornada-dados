# Databricks notebook source
import requests
import json
import datetime
import pandas as pd
url = "https://pokeapi.co/api/v2/pokemon?limit=2000"

resp = requests.get(url)
data = resp.json()

data_save = data['results']

now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
path = f"/Volumes/raw/pokemon/pokemo_raw/pokemons_list/{now}.json"

with open(path, 'w')  as open_file:
    json.dump(data_save, open_file)
