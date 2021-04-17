from datetime import datetime
from dotenv import load_dotenv, find_dotenv
import os
from pymongo import MongoClient
import requests
import time

load_dotenv(find_dotenv())

MONGODB_IP = os.environ.get("MONGODB_IP")
MONGODB_PORT = int(os.environ.get("MONGODB_PORT"))


def get_matches_batch(**kwargs):
    """
    Função responsável por retornar o histórico de partidas de dota.

    Caso não for passado nenhum parâmetro ele retorna o histórico mais
    atualizado. Caso contrário, se passarmos um parâmetro especificando um
    id ele retorna outras 100 partidas mais antigas a ela.
    """
    url = "https://api.opendota.com/api/proMatches"
    if kwargs:
        url += f"?less_than_match_id={kwargs['min_match_id']}"
    data = requests.get(url).json()
    return data


def save_matches(data, db_collection):
    """Salva lista de partidas no banco de dados"""
    db_collection.insert_many(data)
    return True


def get_oldest_matches(db_collection):
    qtd_documents = db_collection.count_documents({})
    min_match_id = db_collection.find_one(sort=[('match_id', 1)])['match_id']
    print(f"Até o momento foram coletados: {qtd_documents} documentos")
    print("Iniciando coleta dos dados")
    while True:
        data_raw = get_matches_batch(min_match_id=min_match_id)
        data = [match for match in data_raw if "match_id" in match]

        if len(data) == 0:
            print(data_raw)
            break

        save_matches(data, db_collection)
        min_match_id = min([match['match_id'] for match in data])
        time.sleep(1)
        qtd_documents += len(data)
        print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Foram coletados: {qtd_documents} documentos"
        )


def get_newest_matches(db_collection):
    qtd_documents = db_collection.count_documents({})
    if qtd_documents > 0:
        list_match_id = [match['match_id'] for match in db_collection.find()]
        max_match_id_mongodb = db_collection.find_one(sort=[('match_id', -1)])['match_id']
    else:
        list_match_id = []
        max_match_id_mongodb = 0
    print(f"Até o momento foram coletados: {qtd_documents} documentos")
    print("Iniciando coleta dos dados")
    data = get_matches_batch()
    data = [match for match in data if match['match_id'] not in list_match_id]
    # print(data)
    if len(data) == 0:
        print('Todos os recentes foram baixados')
        min_match_id = 0
    else:
        save_matches(data, db_collection)
        qtd_documents += len(data)
        min_match_id = min([match['match_id'] for match in data])
    while (max_match_id_mongodb < min_match_id) & (len(data) > 0):
        list_match_id = [match['match_id'] for match in db_collection.find()]
        data_raw = get_matches_batch(min_match_id=min_match_id)
        data = [match for match in data_raw if match['match_id'] not in list_match_id]
        data = [match for match in data if "match_id" in match]
        if len(data) == 0:
            print(data_raw)
            break

        save_matches(data, db_collection)
        min_match_id = min([match['match_id'] for match in data])
        time.sleep(1)
        qtd_documents += len(data)
        print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Foram coletados: {qtd_documents} documentos"
        )


def main(types):

    mongodb_client = MongoClient(MONGODB_IP, MONGODB_PORT)
    mongodb_database = mongodb_client["dota_raw"]

    if types == 'oldest':
        get_oldest_matches(mongodb_database['pro_match_history'])
    elif types == 'newest':
        get_newest_matches(mongodb_database['pro_match_history'])


if __name__ == '__main__':
    main()
