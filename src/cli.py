import click
from dotenv import load_dotenv, find_dotenv
import os
from pymongo import MongoClient

import get_match_history as get_history

load_dotenv(find_dotenv())

MONGODB_IP = os.environ.get("MONGODB_IP")
MONGODB_PORT = int(os.environ.get("MONGODB_PORT"))


class MongoDB(object):
    def __init__(self, mongodb_ip=None, mongodb_port=None):
        self.mongodb_ip = mongodb_ip
        self.mongodb_port = mongodb_port
        self.mongo_client = MongoClient(self.mongodb_ip, self.mongodb_port)
        self.mongo_database = self.mongo_client["dota_raw"]


"""Grupo criado para extração dos dados da API de dota"""


@click.group('extract')
@click.pass_context
def extract(ctx):
    ctx.obj = MongoDB(MONGODB_IP, MONGODB_PORT)


@extract.command('download_history')
@click.option(
    '-t',
    '--types',
    type=click.STRING,
    help='Escolha para baixar os dados de partidas mais antigos ou mais novas')
@click.pass_obj
def download_history(ctx, types):
    if types == 'oldest':
        click.echo('Baixando histórico antigo de partidas')
        get_history.get_oldest_matches(ctx.mongo_database['pro_match_history'])
    elif types == 'newest':
        click.echo('Baixando histórico mais recente de partidas')
        get_history.get_newest_matches(ctx.mongo_database['pro_match_history'])


if __name__ == '__main__':
    extract()
