import cassiopeia as cass
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
import json
import pandas as pd

# Cassiopeia is a Python library for interacting with the Riot Games API.
# Here is a link to the documentation.
# https://cassiopeia.readthedocs.io/en/latest/


def get_connection():

    url_object = sqlalchemy.URL.create(
        "mysql+pymysql",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    return sqlalchemy.create_engine(url_object, echo=True)


def match_crawler(seed_player_name: str, limit: int, region: str, step=1):
    # The thought process is, with the correct players that play daily (like streamers), we can assuredly get seed
    # IDs that are relatively recent This is NOT future proof.
    """
    (Naively) Crawls through matches by match ID sequentially, from the seed_player's first match, decrementing
    through earlier matches at a given step, (default is 1).
    """
    gamesdf = pd.DataFrame({
        'Result': ["Blue"],
        'queue': ['CLASSIC'],
        'region': ['NA'],
        'dragonSoul': ['cloud'],
        'firstDragon': ["Blue"],
        'firstTower': ["Blue"],
        'firstBlood': ["Blue"],
        'firstInhibitor': ["Blue"]
    })
    entries = 0
    seed_player = cass.Summoner(name=seed_player_name, region=region)
    current_id = seed_player.match_history[0].id
    for i in range(limit):
        match = cass.get_match(id=int(current_id), region=region)
        if is_match_valid(match):
            data = match_logger(match, entries)
            gamesdf = pd.concat([gamesdf, data], ignore_index=True)
            entries += 1
            print(entries)
        current_id -= step
    s = games.select()
    conn = engine.connect()
    result = conn.execute(s)
    for row in result:
        print(row)
    gamesdf.to_excel('FinalData.xlsx')


def match_logger(match: cass.Match, entries: int):
    # TODO Log Matches into a DB
    red = 'Red'
    blue = 'Blue'
    if entries < 100000:
        gamewinner = blue if match.blue_team.win else red
        dragonwinner = blue if match.blue_team.first_dragon else red
        towerwinner = blue if match.blue_team.first_tower else red
        bloodwinner = blue if match.blue_team.first_blood else red
        inhibwinner = blue if match.blue_team.first_inhibitor else red
        matchdf = pd.DataFrame({
            'Result': [gamewinner],
            'queue': ['CLASSIC'],
            'region': ['NA'],
            'dragonSoul': ['cloud'],
            'firstDragon': [dragonwinner],
            'firstTower': [towerwinner],
            'firstBlood': [bloodwinner],
            'firstInhibitor': [inhibwinner]
        })
        if entries < 1000:
            ins = games.insert().values(result=gamewinner, queue='CLASSIC', region='NA', dragonSoul='Cloud',
                                        firstDragon=dragonwinner, firstTower=towerwinner, firstBlood=bloodwinner,
                                        firstInhibitor=inhibwinner)
            conn = engine.connect()
            conn.execute(ins)
        return matchdf

    # These are some random match stats for example use.
    # print([participant.champion.name for participant in match.participants])


def is_match_valid(match: cass.Match):
    """Returns true if a given Match is acceptable for data gathering, false otherwise."""
    # TODO Implement fully

    is_valid = False
    try:
        # A try statement to catch "recall error" and related errors which are prone to occur for some matches.
        if (not match.is_remake and
                match.mode.value == "CLASSIC"):
            is_valid = True
    finally:
        return is_valid


cass.apply_settings("config.json")  # You will need to set this up individually

with open("dbinfo.json", "r") as read_file:
    info = json.load(read_file)
    username = info["username"]
    password = info["password"]
    host = info["host"]
    port = info["port"]
    database = info["database"]

engine = get_connection()
meta = MetaData()

games = Table(
    'games', meta,
    Column('matchId', Integer, primary_key=True),
    Column('result', String(4)),
    Column('queue', String(8)),
    Column('region', String(20)),
    Column('dragonSoul', String(10)),
    Column('firstDragon', String(4)),
    Column('firstTower', String(4)),
    Column('firstBlood', String(4)),
    Column('firstInhibitor', String(4))
)

players = Table(
    'players', meta,
    Column('playerId', Integer, primary_key=True),
    Column('matchId', Integer, ForeignKey("games.matchId")),
    Column('champion', String(20)),
    Column('role', String(7)),
    Column('spells', String(255)),
    Column('kills', Integer),
    Column('deaths', Integer),
    Column('assists', Integer)
)

meta.create_all(engine)

match_crawler(seed_player_name="treyhest", limit=5, region="NA")
