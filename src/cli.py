import click

import get_match_history as get_history
"""Grupo criado para extração dos dados da API de dota"""


@click.group('extract')
def extract():
    pass


@extract.command('download_history')
@click.option(
    '-t',
    '--types',
    type=click.STRING,
    help='Escolha para baixar os dados de partidas mais antigos ou mais novas')
def download_history(types):
    if types == 'oldest':
        click.echo('Baixando histórico antigo de partidas')
        get_history.main(types)
    elif types == 'newest':
        click.echo('Baixando histórico mais recente de partidas')
        get_history.main(types)


extract()
