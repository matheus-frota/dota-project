import requests
import time
from tqdm import tqdm


def get_data(match_id):
    url = f"https://api.opendota.com/api/matches/{match_id}"
    return requests.get(url).json()


def save_data(data, db_collection):
    db_collection.insert_one(data)
    return True


def get_id_details_not_collected(db_collection_pro_match,
                                 db_collection_details):
    list_match_id_pro_match = [
        match['match_id'] for match in db_collection_pro_match.find()
    ]
    list_match_id_details = [
        match['match_id'] for match in db_collection_details.find()
    ]

    list_match_id_details_not_collected = [
        match_id for match_id in list_match_id_pro_match
        if match_id not in list_match_id_details
    ]

    return list_match_id_details_not_collected


def get_details(db_collection_pro_match, db_collection_details):
    list_match_id_details_not_collected = get_id_details_not_collected(
        db_collection_pro_match, db_collection_details)

    for match_id in tqdm(list_match_id_details_not_collected):
        data = get_data(match_id)
        save_data(data, db_collection_details)
        time.sleep(1)
