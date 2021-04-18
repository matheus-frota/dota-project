from datetime import datetime
import requests
import time


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
    """Função responsável por baixar os dados de partidas mais antigas."""
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


def checks_database_empty(db_collection):
    """Verifica se a base de dados do mongodb está vazia"""
    qtd_documents = db_collection.count_documents({})
    if qtd_documents == 0:
        return True
    return False


def get_newest_matches(db_collection):
    """Verifica se existe informação de alguma partida nova e atualiza a base de dados"""
    qtd_documents = db_collection.count_documents({})
    if not checks_database_empty(db_collection):
        list_match_id = [match['match_id'] for match in db_collection.find()]
        max_match_id_mongodb = db_collection.find_one(sort=[('match_id', -1)])['match_id']
    else:
        list_match_id = []
        max_match_id_mongodb = 0
    print(f"Até o momento foram coletados: {qtd_documents} documentos")
    print("Iniciando coleta dos dados")
    data = get_matches_batch()
    data = [match for match in data if match['match_id'] not in list_match_id]
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
