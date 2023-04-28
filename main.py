import cassiopeia as cass
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Float, MetaData, insert
import json
import pandas as pd
from datetime import datetime


# import matplotlib.pyplot


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
    start = datetime.now()
    gamesdf = pd.DataFrame({
        'Result': ["Blue"],
        'queue': ['normal_draft_fives'],
        'region': ['NA'],
        'firstDragon': ["Blue"],
        'firstTower': ["Blue"],
        'firstBlood': ["Blue"],
        'firstInhibitor': ["Blue"],
        'firstBaron': ["Blue"],
        'firstRiftHerald': ["Blue"],
        'player0Champ': ['Shen'],
        'player0Side': ['Blue'],
        'player0Kills': [10],
        'player0Deaths': [8],
        'player0Assists': [10],
        'player0KDA': [2.5],
        'player0ObjDmg': [2436],
        'player0GoldPS': [5.461],
        'player0CSPM': [3.2],
        'player0DmgDPS': [50.8596],
        'player0DmgTPS': [27.6516],
        'player0HealingPS': [2.14],
        'player0VisionScorePM': [.90],
        'player0SecDead': [289],
        'player1Champ': ['Nocturne'],
        'player1Side': ['Blue'],
        'player1Kills': [5],
        'player1Deaths': [11],
        'player1Assists': [8],
        'player1KDA': [1.18],
        'player1ObjDmg': [44408],
        'player1GoldPS': [5.68],
        'player1CSPM': [5.4],
        'player1DmgDPS': [105.99],
        'player1DmgTPS': [21.09],
        'player1HealingPS': [6.9],
        'player1VisionScorePM': [1.17],
        'player1SecDead': [389],
        'player2Champ': ['Talon'],
        'player2Side': ['Blue'],
        'player2Kills': [6],
        'player2Deaths': [8],
        'player2Assists': [6],
        'player2KDA': [1.5],
        'player2ObjDmg': [1938],
        'player2GoldPS': [5.3],
        'player2CSPM': [4.88],
        'player2DmgDPS': [69.15],
        'player2DmgTPS': [1.6],
        'player2HealingPS': [3.2],
        'player2VisionScorePM': [.66],
        'player2SecDead': [308],
        'player3Champ': ['Ziggs'],
        'player3Side': ['Blue'],
        'player3Kills': [12],
        'player3Deaths': [4],
        'player3Assists': [9],
        'player3KDA': [5.25],
        'player3ObjDmg': [2000],
        'player3GoldPS': [6.5],
        'player3CSPM': [9.4],
        'player3DmgDPS': [100.67],
        'player3DmgTPS': [25.5],
        'player3HealingPS': [7.4],
        'player3VisionScorePM': [.1],
        'player3SecDead': [200],
        'player4Champ': ['Nautilus'],
        'player4Side': ['Blue'],
        'player4Kills': [3],
        'player4Deaths': [14],
        'player4Assists': [19],
        'player4KDA': [1.54],
        'player4ObjDmg': [600],
        'player4GoldPS': [4.4],
        'player4CSPM': [1.02],
        'player4DmgDPS': [40.6],
        'player4DmgTPS': [200.5],
        'player4HealingPS': [10.6],
        'player4VisionScorePM': [2.4],
        'player4SecDead': [436],
        'player5Champ': ['K`Sante'],
        'player5Side': ['Red'],
        'player5Kills': [3],
        'player5Deaths': [7],
        'player5Assists': [8],
        'player5KDA': [1.57],
        'player5ObjDmg': [9153],
        'player5GoldPS': [5.48],
        'player5CSPM': [4.7],
        'player5DmgDPS': [100.4],
        'player5DmgTPS': [40.5],
        'player5HealingPS': [3.7],
        'player5VisionScorePM': [.89],
        'player5SecDead': [471],
        'player6Champ': ['Gragas'],
        'player6Side': ['Red'],
        'player6Kills': [10],
        'player6Deaths': [6],
        'player6Assists': [22],
        'player6KDA': [5.33],
        'player6ObjDmg': [1056],
        'player6GoldPS': [4.42],
        'player6CSPM': [8.5],
        'player6DmgDPS': [94.7],
        'player6DmgTPS': [20.7],
        'player6HealingPS': [15.4],
        'player6VisionScorePM': [.6],
        'player6SecDead': [102],
        'player7Champ': ['Neeko'],
        'player7Side': ['Red'],
        'player7Kills': [14],
        'player7Deaths': [5],
        'player7Assists': [17],
        'player7KDA': [6.2],
        'player7ObjDmg': [24126],
        'player7GoldPS': [7.86],
        'player7CSPM': [5.56],
        'player7DmgDPS': [95.4],
        'player7DmgTPS': [13.7],
        'player7HealingPS': [2.52],
        'player7VisionScorePM': [2.3],
        'player7SecDead': [95],
        'player8Champ': ['Miss Fortune'],
        'player8Side': ['Red'],
        'player8Kills': [17],
        'player8Deaths': [11],
        'player8Assists': [8],
        'player8KDA': [2.27],
        'player8ObjDmg': [1958],
        'player8GoldPS': [4.76],
        'player8CSPM': [5.2],
        'player8DmgDPS': [95.272],
        'player8DmgTPS': [13.73],
        'player8HealingPS': [2.86],
        'player8VisionScorePM': [.7],
        'player8SecDead': [605],
        'player9Champ': ['Milio'],
        'player9Side': ['Red'],
        'player9Kills': [1],
        'player9Deaths': [7],
        'player9Assists': [32],
        'player9KDA': [4.71],
        'player9ObjDmg': [2370],
        'player9GoldPS': [4.76],
        'player9CSPM': [.90],
        'player9DmgDPS': [15.3],
        'player9DmgTPS': [10.6],
        'player9HealingPS': [9.3],
        'player9VisionScorePM': [1.4],
        'player9SecDead': [374]
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
    gamesdf.to_csv('FinalData.csv')
    end = datetime.now()
    print(end-start)


def match_logger(match: cass.Match, entries: int):
    # TODO Log Matches into a DB
    red = 'Red'
    blue = 'Blue'
    match_seconds = match.duration.seconds
    if entries < 100000:
        gameregion = match.region.value
        gamequeue = match.queue.name
        gamewinner = blue if match.blue_team.win else red
        dragonwinner = blue if match.blue_team.first_dragon else red
        towerwinner = blue if match.blue_team.first_tower else red
        bloodwinner = blue if match.blue_team.first_blood else red
        inhibwinner = blue if match.blue_team.first_inhibitor else red
        baronwinner = blue if match.blue_team.first_baron else red
        riftwinner = blue if match.blue_team.first_rift_herald else red
        matchdf = pd.DataFrame({
            'Result': [gamewinner],
            'queue': [gamequeue],
            'region': [gameregion],
            'firstDragon': [dragonwinner],
            'firstTower': [towerwinner],
            'firstBlood': [bloodwinner],
            'firstInhibitor': [inhibwinner],
            'firstBaron': [baronwinner],
            'firstRiftHerald': [riftwinner],
            'player0Champ': [match.participants[0].champion.name],
            'player0Side': [match.participants[0].team.side.name],
            'player0Kills': [match.participants[0].stats.kills],
            'player0Deaths': [match.participants[0].stats.deaths],
            'player0Assists': [match.participants[0].stats.assists],
            'player0KDA': [match.participants[0].stats.kda],
            'player0ObjDmg': [match.participants[0].stats.damage_dealt_to_objectives],
            'player0GoldPS': [match.participants[0].stats.gold_earned / match_seconds],
            'player0CSPM': [(match.participants[0].stats.neutral_minions_killed +
                             match.participants[0].stats.total_minions_killed) / (match_seconds / 60)],
            'player0DmgDPS': [match.participants[0].stats.total_damage_dealt / match_seconds],
            'player0DmgTPS': [match.participants[0].stats.total_damage_taken / match_seconds],
            'player0HealingPS': [match.participants[0].stats.total_heal / match_seconds],
            'player0VisionScorePM': [match.participants[0].stats.vision_score / (match_seconds / 60)],
            'player0SecDead': [match.participants[0].stats.total_time_spent_dead],
            'player1Champ': [match.participants[1].champion.name],
            'player1Side': [match.participants[1].team.side.name],
            'player1Kills': [match.participants[1].stats.kills],
            'player1Deaths': [match.participants[1].stats.deaths],
            'player1Assists': [match.participants[1].stats.assists],
            'player1KDA': [match.participants[1].stats.kda],
            'player1ObjDmg': [match.participants[1].stats.damage_dealt_to_objectives],
            'player1GoldPS': [match.participants[1].stats.gold_earned / match_seconds],
            'player1CSPM': [(match.participants[1].stats.neutral_minions_killed +
                             match.participants[1].stats.total_minions_killed) / (match_seconds / 60)],
            'player1DmgDPS': [match.participants[1].stats.total_damage_dealt / match_seconds],
            'player1DmgTPS': [match.participants[1].stats.total_damage_taken / match_seconds],
            'player1HealingPS': [match.participants[1].stats.total_heal / match_seconds],
            'player1VisionScorePM': [match.participants[1].stats.vision_score / (match_seconds / 60)],
            'player1SecDead': [match.participants[1].stats.total_time_spent_dead],
            'player2Champ': [match.participants[2].champion.name],
            'player2Side': [match.participants[2].team.side.name],
            'player2Kills': [match.participants[2].stats.kills],
            'player2Deaths': [match.participants[2].stats.deaths],
            'player2Assists': [match.participants[2].stats.assists],
            'player2KDA': [match.participants[2].stats.kda],
            'player2ObjDmg': [match.participants[2].stats.damage_dealt_to_objectives],
            'player2GoldPS': [match.participants[2].stats.gold_earned / match_seconds],
            'player2CSPM': [(match.participants[2].stats.neutral_minions_killed +
                             match.participants[2].stats.total_minions_killed) / (match_seconds / 60)],
            'player2DmgDPS': [match.participants[2].stats.total_damage_dealt / match_seconds],
            'player2DmgTPS': [match.participants[2].stats.total_damage_taken / match_seconds],
            'player2HealingPS': [match.participants[2].stats.total_heal / match_seconds],
            'player2VisionScorePM': [match.participants[2].stats.vision_score / (match_seconds / 60)],
            'player2SecDead': [match.participants[2].stats.total_time_spent_dead],
            'player3Champ': [match.participants[3].champion.name],
            'player3Side': [match.participants[3].team.side.name],
            'player3Kills': [match.participants[3].stats.kills],
            'player3Deaths': [match.participants[3].stats.deaths],
            'player3Assists': [match.participants[3].stats.assists],
            'player3KDA': [match.participants[3].stats.kda],
            'player3ObjDmg': [match.participants[3].stats.damage_dealt_to_objectives],
            'player3GoldPS': [match.participants[3].stats.gold_earned / match_seconds],
            'player3CSPM': [(match.participants[3].stats.neutral_minions_killed +
                             match.participants[3].stats.total_minions_killed) / (match_seconds / 60)],
            'player3DmgDPS': [match.participants[3].stats.total_damage_dealt / match_seconds],
            'player3DmgTPS': [match.participants[3].stats.total_damage_taken / match_seconds],
            'player3HealingPS': [match.participants[3].stats.total_heal / match_seconds],
            'player3VisionScorePM': [match.participants[3].stats.vision_score / (match_seconds / 60)],
            'player3SecDead': [match.participants[3].stats.total_time_spent_dead],
            'player4Champ': [match.participants[4].champion.name],
            'player4Side': [match.participants[4].team.side.name],
            'player4Kills': [match.participants[4].stats.kills],
            'player4Deaths': [match.participants[4].stats.deaths],
            'player4Assists': [match.participants[4].stats.assists],
            'player4KDA': [match.participants[4].stats.kda],
            'player4ObjDmg': [match.participants[4].stats.damage_dealt_to_objectives],
            'player4GoldPS': [match.participants[4].stats.gold_earned / match_seconds],
            'player4CSPM': [(match.participants[4].stats.neutral_minions_killed +
                             match.participants[4].stats.total_minions_killed) / (match_seconds / 60)],
            'player4DmgDPS': [match.participants[4].stats.total_damage_dealt / match_seconds],
            'player4DmgTPS': [match.participants[4].stats.total_damage_taken / match_seconds],
            'player4HealingPS': [match.participants[4].stats.total_heal / match_seconds],
            'player4VisionScorePM': [match.participants[4].stats.vision_score / (match_seconds / 60)],
            'player4SecDead': [match.participants[4].stats.total_time_spent_dead],
            'player5Champ': [match.participants[5].champion.name],
            'player5Side': [match.participants[5].team.side.name],
            'player5Kills': [match.participants[5].stats.kills],
            'player5Deaths': [match.participants[5].stats.deaths],
            'player5Assists': [match.participants[5].stats.assists],
            'player5KDA': [match.participants[5].stats.kda],
            'player5ObjDmg': [match.participants[5].stats.damage_dealt_to_objectives],
            'player5GoldPS': [match.participants[5].stats.gold_earned / match_seconds],
            'player5CSPM': [(match.participants[5].stats.neutral_minions_killed +
                             match.participants[5].stats.total_minions_killed) / (match_seconds / 60)],
            'player5DmgDPS': [match.participants[5].stats.total_damage_dealt / match_seconds],
            'player5DmgTPS': [match.participants[5].stats.total_damage_taken / match_seconds],
            'player5HealingPS': [match.participants[5].stats.total_heal / match_seconds],
            'player5VisionScorePM': [match.participants[5].stats.vision_score / (match_seconds / 60)],
            'player5SecDead': [match.participants[5].stats.total_time_spent_dead],
            'player6Champ': [match.participants[6].champion.name],
            'player6Side': [match.participants[6].team.side.name],
            'player6Kills': [match.participants[6].stats.kills],
            'player6Deaths': [match.participants[6].stats.deaths],
            'player6Assists': [match.participants[6].stats.assists],
            'player6KDA': [match.participants[6].stats.kda],
            'player6ObjDmg': [match.participants[6].stats.damage_dealt_to_objectives],
            'player6GoldPS': [match.participants[6].stats.gold_earned / match_seconds],
            'player6CSPM': [(match.participants[6].stats.neutral_minions_killed +
                             match.participants[6].stats.total_minions_killed) / (match_seconds / 60)],
            'player6DmgDPS': [match.participants[6].stats.total_damage_dealt / match_seconds],
            'player6DmgTPS': [match.participants[6].stats.total_damage_taken / match_seconds],
            'player6HealingPS': [match.participants[6].stats.total_heal / match_seconds],
            'player6VisionScorePM': [match.participants[6].stats.vision_score / (match_seconds / 60)],
            'player6SecDead': [match.participants[6].stats.total_time_spent_dead],
            'player7Champ': [match.participants[7].champion.name],
            'player7Side': [match.participants[7].team.side.name],
            'player7Kills': [match.participants[7].stats.kills],
            'player7Deaths': [match.participants[7].stats.deaths],
            'player7Assists': [match.participants[7].stats.assists],
            'player7KDA': [match.participants[7].stats.kda],
            'player7ObjDmg': [match.participants[7].stats.damage_dealt_to_objectives],
            'player7GoldPS': [match.participants[7].stats.gold_earned / match_seconds],
            'player7CSPM': [(match.participants[7].stats.neutral_minions_killed +
                             match.participants[7].stats.total_minions_killed) / (match_seconds / 60)],
            'player7DmgDPS': [match.participants[7].stats.total_damage_dealt / match_seconds],
            'player7DmgTPS': [match.participants[7].stats.total_damage_taken / match_seconds],
            'player7HealingPS': [match.participants[7].stats.total_heal / match_seconds],
            'player7VisionScorePM': [match.participants[7].stats.vision_score / (match_seconds / 60)],
            'player7SecDead': [match.participants[7].stats.total_time_spent_dead],
            'player8Champ': [match.participants[8].champion.name],
            'player8Side': [match.participants[8].team.side.name],
            'player8Kills': [match.participants[8].stats.kills],
            'player8Deaths': [match.participants[8].stats.deaths],
            'player8Assists': [match.participants[8].stats.assists],
            'player8KDA': [match.participants[8].stats.kda],
            'player8ObjDmg': [match.participants[8].stats.damage_dealt_to_objectives],
            'player8GoldPS': [match.participants[8].stats.gold_earned / match_seconds],
            'player8CSPM': [(match.participants[8].stats.neutral_minions_killed +
                             match.participants[8].stats.total_minions_killed) / (match_seconds / 60)],
            'player8DmgDPS': [match.participants[8].stats.total_damage_dealt / match_seconds],
            'player8DmgTPS': [match.participants[8].stats.total_damage_taken / match_seconds],
            'player8HealingPS': [match.participants[8].stats.total_heal / match_seconds],
            'player8VisionScorePM': [match.participants[8].stats.vision_score / (match_seconds / 60)],
            'player8SecDead': [match.participants[8].stats.total_time_spent_dead],
            'player9Champ': [match.participants[9].champion.name],
            'player9Side': [match.participants[9].team.side.name],
            'player9Kills': [match.participants[9].stats.kills],
            'player9Deaths': [match.participants[9].stats.deaths],
            'player9Assists': [match.participants[9].stats.assists],
            'player9KDA': [match.participants[9].stats.kda],
            'player9ObjDmg': [match.participants[9].stats.damage_dealt_to_objectives],
            'player9GoldPS': [match.participants[9].stats.gold_earned / match_seconds],
            'player9CSPM': [(match.participants[9].stats.neutral_minions_killed +
                             match.participants[9].stats.total_minions_killed) / (match_seconds / 60)],
            'player9DmgDPS': [match.participants[9].stats.total_damage_dealt / match_seconds],
            'player9DmgTPS': [match.participants[9].stats.total_damage_taken / match_seconds],
            'player9HealingPS': [match.participants[9].stats.total_heal / match_seconds],
            'player9VisionScorePM': [match.participants[9].stats.vision_score / (match_seconds / 60)],
            'player9SecDead': [match.participants[9].stats.total_time_spent_dead]

        })
        if entries < 1000:
            ins = insert(games).values(match_id=str(match.id), result=gamewinner, queue=gamequeue, region=gameregion,
                                       firstDragon=dragonwinner, firstTower=towerwinner, firstBlood=bloodwinner,
                                       firstInhibitor=inhibwinner, firstBaron=baronwinner, firstRiftHerald=riftwinner)
            SQLScriptExc(ins)
            for participant in match.participants:
                ins = insert(players).values(match_id=str(match.id), champion_name=participant.champion.name,
                                             side=participant.team.side.name,
                                             kills=participant.stats.kills, deaths=participant.stats.deaths,
                                             assists=participant.stats.assists, kda=participant.stats.kda,
                                             objective_damage=participant.stats.damage_dealt_to_objectives,
                                             gold_ps=participant.stats.gold_earned / match_seconds,
                                             creep_score_pm=(participant.stats.neutral_minions_killed + participant.
                                                             stats.total_minions_killed) / (match_seconds / 60),
                                             damage_dealt_ps=participant.stats.total_damage_dealt / match_seconds,
                                             damage_taken_ps=participant.stats.total_damage_taken / match_seconds,
                                             healing_ps=participant.stats.total_heal / match_seconds,
                                             vision_bought=participant.stats.vision_wards_bought,
                                             vision_placed=participant.stats.wards_placed,
                                             vision_destroyed=participant.stats.wards_killed,
                                             vision_score_pm=participant.stats.vision_score / (match_seconds / 60),
                                             seconds_dead=participant.stats.total_time_spent_dead
                                             )
                SQLScriptExc(ins)
        return matchdf


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
    Column('queue', String(40)),
    Column('region', String(20)),
    Column('firstDragon', String(4)),
    Column('firstTower', String(4)),
    Column('firstBlood', String(4)),
    Column('firstInhibitor', String(4)),
    Column('firstBaron', String(4)),
    Column('firstRiftHerald', String(4))
)

players = Table(
    'players', meta,
    Column('playerId', Integer, primary_key=True),
    Column('match_id', String(20)),  # Note, could not get Foreign keys to work (temporary)
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
