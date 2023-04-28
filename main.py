import cassiopeia as cass
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Float, MetaData, ForeignKey, insert
import json
import pandas as pd
#import matplotlib.pyplot


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
    return sqlalchemy.create_engine(url_object)


def match_crawler(seed_player_name: str, limit: int, region: str, step=1):
    # The thought process is, with the correct players that play daily (like streamers), we can assuredly get seed
    # IDs that are relatively recent This is NOT future-proof.
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
    #gamesdf.to_excel('FinalData.xlsx')


def match_logger(match: cass.Match, entries: int):
    # TODO Log Matches into a DB
    red = 'Red'
    blue = 'Blue'
    if entries < 100000:
        gameregion = match.region.value
        gamequeue = match.queue.name
        gamewinner = blue if match.blue_team.win else red
        dragonwinner = blue if match.blue_team.first_dragon else red
        towerwinner = blue if match.blue_team.first_tower else red
        bloodwinner = blue if match.blue_team.first_blood else red
        inhibwinner = blue if match.blue_team.first_inhibitor else red
        matchdf = pd.DataFrame({
            'Result': [gamewinner],
            'queue': [gamequeue],
            'region': [gameregion],
            'dragonSoul': ['cloud'],
            'firstDragon': [dragonwinner],
            'firstTower': [towerwinner],
            'firstBlood': [bloodwinner],
            'firstInhibitor': [inhibwinner]
        })
        if entries < 1000:
            ins = insert(games).values(match_id=str(match.id), result=gamewinner, queue='CLASSIC', region=gameregion, dragonSoul='Cloud',
                                       firstDragon=dragonwinner, firstTower=towerwinner, firstBlood=bloodwinner,
                                       firstInhibitor=inhibwinner)
            SQLScriptExc(ins)
            match_seconds = match.duration.seconds
            for participant in match.participants:
                ins = insert(players).values(match_id=str(match.id), champion_name=participant.champion.name, side=participant.team.side.name, 
                                             kills=participant.stats.kills, deaths=participant.stats.deaths, assists=participant.stats.assists, kda=participant.stats.kda,
                                             objective_damage=participant.stats.damage_dealt_to_objectives,
                                             gold_ps=participant.stats.gold_earned/match_seconds, creep_score_pm=(participant.stats.neutral_minions_killed + participant.stats.total_minions_killed)/(match_seconds/60),
                                             damage_dealt_ps=participant.stats.total_damage_dealt/match_seconds, damage_taken_ps=participant.stats.total_damage_taken/match_seconds, healing_ps=participant.stats.total_heal/match_seconds,
                                             vision_bought=participant.stats.vision_wards_bought, vision_placed=participant.stats.wards_placed, vision_destroyed=participant.stats.wards_killed, vision_score_pm=participant.stats.vision_score/(match_seconds/60),
                                             seconds_dead=participant.stats.total_time_spent_dead
                                             )
                SQLScriptExc(ins)

        return matchdf

    # These are some random match stats for example use.
    # print([participant.champion.name for participant in match.participants])


def SQLScriptExc(script):
    with engine.connect() as conn:
        conn.execute(script)
        conn.commit()


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
    Column('index', Integer, primary_key=True),
    Column('match_id', String(20), unique=True),
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
    Column('match_id', String(20)), # Note, could not get Foreign keys to work (temporary)
    Column('champion_name', String(20)),
    Column('side', String(4)),
    Column('kills', Integer),
    Column('deaths', Integer),
    Column('assists', Integer),
    Column('kda', Float),
    Column('objective_damage', Integer),
    Column('gold_ps', Float),
    Column('creep_score_pm', Float),
    Column('damage_dealt_ps', Float),
    Column('damage_taken_ps', Float),
    Column('healing_ps', Float),
    Column('vision_bought', Integer),
    Column('vision_placed', Integer),
    Column('vision_destroyed', Integer),
    Column('vision_score_pm', Float),
    Column('seconds_dead', Integer)
)

meta.create_all(engine)

match_crawler(seed_player_name="treyhest", limit=5, region="NA")
