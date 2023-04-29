import cassiopeia as cass
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Float, MetaData, insert
import json
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
    entries = 0
    seed_player = cass.Summoner(name=seed_player_name, region=region)
    current_id = seed_player.match_history[0].id
    for i in range(limit):
        match = cass.get_match(id=int(current_id), region=region)
        if is_match_valid(match):
            match_logger(match)
            entries += 1
            print(entries)
        current_id -= step
    end = datetime.now()
    print(end - start)


def match_logger(match: cass.Match):
    # TODO Log Matches into a DB
    red = 'Red'
    blue = 'Blue'
    match_seconds = match.duration.seconds
    gameregion = match.region.value
    gamequeue = match.queue.name
    gamewinner = blue if match.blue_team.win else red
    dragonwinner = blue if match.blue_team.first_dragon else red
    towerwinner = blue if match.blue_team.first_tower else red
    bloodwinner = blue if match.blue_team.first_blood else red
    inhibwinner = blue if match.blue_team.first_inhibitor else red
    baronwinner = blue if match.blue_team.first_baron else red
    riftwinner = blue if match.blue_team.first_rift_herald else red
    ins = insert(match_data).values(match_id=str(match.id), result=gamewinner, match_length=match_seconds,
                                    queue=gamequeue, region=gameregion, first_dragon=dragonwinner,
                                    first_tower=towerwinner, first_blood=bloodwinner, first_inhibitor=inhibwinner,
                                    first_baron=baronwinner, first_rift_herald=riftwinner,
                                    player0_champ=match.participants[0].champion.name,
                                    player0_side=match.participants[0].team.side.name,
                                    player0_kills=match.participants[0].stats.kills,
                                    player0_deaths=match.participants[0].stats.deaths,
                                    player0_assists=match.participants[0].stats.assists,
                                    player0_kda=match.participants[0].stats.kda,
                                    player0_objective_damage=match.participants[0].stats.damage_dealt_to_objectives,
                                    player0_gold_ps=match.participants[0].stats.gold_earned / match_seconds,
                                    player0_creep_score_pm=(match.participants[0].stats.neutral_minions_killed +
                                                            match.participants[0].stats.total_minions_killed) / (match_seconds / 60),
                                    player0_damage_dealt_ps=match.participants[0].stats.total_damage_dealt / match_seconds,
                                    player0_damage_taken_ps=match.participants[0].stats.total_damage_taken / match_seconds,
                                    player0_healing_ps=match.participants[0].stats.total_heal / match_seconds,
                                    player0_vision_score_pm=match.participants[0].stats.vision_score / (match_seconds / 60),
                                    player0_seconds_dead=match.participants[0].stats.total_time_spent_dead,
                                    player1_champ=match.participants[1].champion.name,
                                    player1_side=match.participants[1].team.side.name,
                                    player1_kills=match.participants[1].stats.kills,
                                    player1_deaths=match.participants[1].stats.deaths,
                                    player1_assists=match.participants[1].stats.assists,
                                    player1_kda=match.participants[1].stats.kda,
                                    player1_objective_damage=match.participants[1].stats.damage_dealt_to_objectives,
                                    player1_gold_ps=match.participants[1].stats.gold_earned / match_seconds,
                                    player1_creep_score_pm=(match.participants[1].stats.neutral_minions_killed +
                                                            match.participants[1].stats.total_minions_killed) / (match_seconds / 60),
                                    player1_damage_dealt_ps=match.participants[1].stats.total_damage_dealt / match_seconds,
                                    player1_damage_taken_ps=match.participants[1].stats.total_damage_taken / match_seconds,
                                    player1_healing_ps=match.participants[1].stats.total_heal / match_seconds,
                                    player1_vision_score_pm=match.participants[1].stats.vision_score / (match_seconds / 60),
                                    player1_seconds_dead=match.participants[1].stats.total_time_spent_dead,
                                    player2_champ=match.participants[2].champion.name,
                                    player2_side=match.participants[2].team.side.name,
                                    player2_kills=match.participants[2].stats.kills,
                                    player2_deaths=match.participants[2].stats.deaths,
                                    player2_assists=match.participants[2].stats.assists,
                                    player2_kda=match.participants[2].stats.kda,
                                    player2_objective_damage=match.participants[2].stats.damage_dealt_to_objectives,
                                    player2_gold_ps=match.participants[2].stats.gold_earned / match_seconds,
                                    player2_creep_score_pm=(match.participants[2].stats.neutral_minions_killed +
                                                            match.participants[2].stats.total_minions_killed) / (match_seconds / 60),
                                    player2_damage_dealt_ps=match.participants[2].stats.total_damage_dealt / match_seconds,
                                    player2_damage_taken_ps=match.participants[2].stats.total_damage_taken / match_seconds,
                                    player2_healing_ps=match.participants[2].stats.total_heal / match_seconds,
                                    player2_vision_score_pm=match.participants[2].stats.vision_score / (match_seconds / 60),
                                    player2_seconds_dead=match.participants[2].stats.total_time_spent_dead,
                                    player3_champ=match.participants[3].champion.name,
                                    player3_side=match.participants[3].team.side.name,
                                    player3_kills=match.participants[3].stats.kills,
                                    player3_deaths=match.participants[3].stats.deaths,
                                    player3_assists=match.participants[3].stats.assists,
                                    player3_kda=match.participants[3].stats.kda,
                                    player3_objective_damage=match.participants[3].stats.damage_dealt_to_objectives,
                                    player3_gold_ps=match.participants[3].stats.gold_earned / match_seconds,
                                    player3_creep_score_pm=(match.participants[3].stats.neutral_minions_killed +
                                                            match.participants[3].stats.total_minions_killed) / (match_seconds / 60),
                                    player3_damage_dealt_ps=match.participants[3].stats.total_damage_dealt / match_seconds,
                                    player3_damage_taken_ps=match.participants[3].stats.total_damage_taken / match_seconds,
                                    player3_healing_ps=match.participants[3].stats.total_heal / match_seconds,
                                    player3_vision_score_pm=match.participants[3].stats.vision_score / (match_seconds / 60),
                                    player3_seconds_dead=match.participants[3].stats.total_time_spent_dead,
                                    player4_champ=match.participants[4].champion.name,
                                    player4_side=match.participants[4].team.side.name,
                                    player4_kills=match.participants[4].stats.kills,
                                    player4_deaths=match.participants[4].stats.deaths,
                                    player4_assists=match.participants[4].stats.assists,
                                    player4_kda=match.participants[4].stats.kda,
                                    player4_objective_damage=match.participants[4].stats.damage_dealt_to_objectives,
                                    player4_gold_ps=match.participants[4].stats.gold_earned / match_seconds,
                                    player4_creep_score_pm=(match.participants[4].stats.neutral_minions_killed +
                                                            match.participants[4].stats.total_minions_killed) / (match_seconds / 60),
                                    player4_damage_dealt_ps=match.participants[4].stats.total_damage_dealt / match_seconds,
                                    player4_damage_taken_ps=match.participants[4].stats.total_damage_taken / match_seconds,
                                    player4_healing_ps=match.participants[4].stats.total_heal / match_seconds,
                                    player4_vision_score_pm=match.participants[4].stats.vision_score / (match_seconds / 60),
                                    player4_seconds_dead=match.participants[4].stats.total_time_spent_dead,
                                    player5_champ=match.participants[5].champion.name,
                                    player5_side=match.participants[5].team.side.name,
                                    player5_kills=match.participants[5].stats.kills,
                                    player5_deaths=match.participants[5].stats.deaths,
                                    player5_assists=match.participants[5].stats.assists,
                                    player5_kda=match.participants[5].stats.kda,
                                    player5_objective_damage=match.participants[5].stats.damage_dealt_to_objectives,
                                    player5_gold_ps=match.participants[5].stats.gold_earned / match_seconds,
                                    player5_creep_score_pm=(match.participants[5].stats.neutral_minions_killed +
                                                            match.participants[5].stats.total_minions_killed) / (match_seconds / 60),
                                    player5_damage_dealt_ps=match.participants[5].stats.total_damage_dealt / match_seconds,
                                    player5_damage_taken_ps=match.participants[5].stats.total_damage_taken / match_seconds,
                                    player5_healing_ps=match.participants[5].stats.total_heal / match_seconds,
                                    player5_vision_score_pm=match.participants[5].stats.vision_score / (match_seconds / 60),
                                    player5_seconds_dead=match.participants[5].stats.total_time_spent_dead,
                                    player6_champ=match.participants[6].champion.name,
                                    player6_side=match.participants[6].team.side.name,
                                    player6_kills=match.participants[6].stats.kills,
                                    player6_deaths=match.participants[6].stats.deaths,
                                    player6_assists=match.participants[6].stats.assists,
                                    player6_kda=match.participants[6].stats.kda,
                                    player6_objective_damage=match.participants[6].stats.damage_dealt_to_objectives,
                                    player6_gold_ps=match.participants[6].stats.gold_earned / match_seconds,
                                    player6_creep_score_pm=(match.participants[6].stats.neutral_minions_killed +
                                                            match.participants[6].stats.total_minions_killed) / (match_seconds / 60),
                                    player6_damage_dealt_ps=match.participants[6].stats.total_damage_dealt / match_seconds,
                                    player6_damage_taken_ps=match.participants[6].stats.total_damage_taken / match_seconds,
                                    player6_healing_ps=match.participants[6].stats.total_heal / match_seconds,
                                    player6_vision_score_pm=match.participants[6].stats.vision_score / (match_seconds / 60),
                                    player6_seconds_dead=match.participants[6].stats.total_time_spent_dead,
                                    player7_champ=match.participants[7].champion.name,
                                    player7_side=match.participants[7].team.side.name,
                                    player7_kills=match.participants[7].stats.kills,
                                    player7_deaths=match.participants[7].stats.deaths,
                                    player7_assists=match.participants[7].stats.assists,
                                    player7_kda=match.participants[7].stats.kda,
                                    player7_objective_damage=match.participants[7].stats.damage_dealt_to_objectives,
                                    player7_gold_ps=match.participants[7].stats.gold_earned / match_seconds,
                                    player7_creep_score_pm=(match.participants[7].stats.neutral_minions_killed +
                                                            match.participants[7].stats.total_minions_killed) / (match_seconds / 60),
                                    player7_damage_dealt_ps=match.participants[7].stats.total_damage_dealt / match_seconds,
                                    player7_damage_taken_ps=match.participants[7].stats.total_damage_taken / match_seconds,
                                    player7_healing_ps=match.participants[7].stats.total_heal / match_seconds,
                                    player7_vision_score_pm=match.participants[7].stats.vision_score / (match_seconds / 60),
                                    player7_seconds_dead=match.participants[7].stats.total_time_spent_dead,
                                    player8_champ=match.participants[8].champion.name,
                                    player8_side=match.participants[8].team.side.name,
                                    player8_kills=match.participants[8].stats.kills,
                                    player8_deaths=match.participants[8].stats.deaths,
                                    player8_assists=match.participants[8].stats.assists,
                                    player8_kda=match.participants[8].stats.kda,
                                    player8_objective_damage=match.participants[8].stats.damage_dealt_to_objectives,
                                    player8_gold_ps=match.participants[8].stats.gold_earned / match_seconds,
                                    player8_creep_score_pm=(match.participants[8].stats.neutral_minions_killed +
                                                            match.participants[8].stats.total_minions_killed) / (match_seconds / 60),
                                    player8_damage_dealt_ps=match.participants[8].stats.total_damage_dealt / match_seconds,
                                    player8_damage_taken_ps=match.participants[8].stats.total_damage_taken / match_seconds,
                                    player8_healing_ps=match.participants[8].stats.total_heal / match_seconds,
                                    player8_vision_score_pm=match.participants[8].stats.vision_score / (match_seconds / 60),
                                    player8_seconds_dead=match.participants[8].stats.total_time_spent_dead,
                                    player9_champ=match.participants[9].champion.name,
                                    player9_side=match.participants[9].team.side.name,
                                    player9_kills=match.participants[9].stats.kills,
                                    player9_deaths=match.participants[9].stats.deaths,
                                    player9_assists=match.participants[9].stats.assists,
                                    player9_kda=match.participants[9].stats.kda,
                                    player9_objective_damage=match.participants[9].stats.damage_dealt_to_objectives,
                                    player9_gold_ps=match.participants[9].stats.gold_earned / match_seconds,
                                    player9_creep_score_pm=(match.participants[9].stats.neutral_minions_killed +
                                                            match.participants[9].stats.total_minions_killed) / (match_seconds / 60),
                                    player9_damage_dealt_ps=match.participants[9].stats.total_damage_dealt / match_seconds,
                                    player9_damage_taken_ps=match.participants[9].stats.total_damage_taken / match_seconds,
                                    player9_healing_ps=match.participants[9].stats.total_heal / match_seconds,
                                    player9_vision_score_pm=match.participants[9].stats.vision_score / (match_seconds / 60),
                                    player9_seconds_dead=match.participants[9].stats.total_time_spent_dead,
                                    )
    SQLScriptExc(ins)


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
match_data = Table(
    'match_data', meta,
    Column('index', Integer, primary_key=True),
    Column('match_id', String(20), unique=True),
    Column('result', String(4)),
    Column('match_length', Integer),
    Column('queue', String(40)),
    Column('region', String(20)),
    Column('first_dragon', String(4)),
    Column('first_tower', String(4)),
    Column('first_blood', String(4)),
    Column('first_inhibitor', String(4)),
    Column('first_baron', String(4)),
    Column('first_rift_herald', String(4)),
    Column('player0_champ', String(20)),
    Column('player0_side', String(4)),
    Column('player0_kills', Integer),
    Column('player0_deaths', Integer),
    Column('player0_assists', Integer),
    Column('player0_kda', Float),
    Column('player0_objective_damage', Integer),
    Column('player0_gold_ps', Float),
    Column('player0_creep_score_pm', Float),
    Column('player0_damage_dealt_ps', Float),
    Column('player0_damage_taken_ps', Float),
    Column('player0_healing_ps', Float),
    Column('player0_vision_score_pm', Float),
    Column('player0_seconds_dead', Integer),
    Column('player1_champ', String(20)),
    Column('player1_side', String(4)),
    Column('player1_kills', Integer),
    Column('player1_deaths', Integer),
    Column('player1_assists', Integer),
    Column('player1_kda', Float),
    Column('player1_objective_damage', Integer),
    Column('player1_gold_ps', Float),
    Column('player1_creep_score_pm', Float),
    Column('player1_damage_dealt_ps', Float),
    Column('player1_damage_taken_ps', Float),
    Column('player1_healing_ps', Float),
    Column('player1_vision_score_pm', Float),
    Column('player1_seconds_dead', Integer),
    Column('player2_champ', String(20)),
    Column('player2_side', String(4)),
    Column('player2_kills', Integer),
    Column('player2_deaths', Integer),
    Column('player2_assists', Integer),
    Column('player2_kda', Float),
    Column('player2_objective_damage', Integer),
    Column('player2_gold_ps', Float),
    Column('player2_creep_score_pm', Float),
    Column('player2_damage_dealt_ps', Float),
    Column('player2_damage_taken_ps', Float),
    Column('player2_healing_ps', Float),
    Column('player2_vision_score_pm', Float),
    Column('player2_seconds_dead', Integer),
    Column('player3_champ', String(20)),
    Column('player3_side', String(4)),
    Column('player3_kills', Integer),
    Column('player3_deaths', Integer),
    Column('player3_assists', Integer),
    Column('player3_kda', Float),
    Column('player3_objective_damage', Integer),
    Column('player3_gold_ps', Float),
    Column('player3_creep_score_pm', Float),
    Column('player3_damage_dealt_ps', Float),
    Column('player3_damage_taken_ps', Float),
    Column('player3_healing_ps', Float),
    Column('player3_vision_score_pm', Float),
    Column('player3_seconds_dead', Integer),
    Column('player4_champ', String(20)),
    Column('player4_side', String(4)),
    Column('player4_kills', Integer),
    Column('player4_deaths', Integer),
    Column('player4_assists', Integer),
    Column('player4_kda', Float),
    Column('player4_objective_damage', Integer),
    Column('player4_gold_ps', Float),
    Column('player4_creep_score_pm', Float),
    Column('player4_damage_dealt_ps', Float),
    Column('player4_damage_taken_ps', Float),
    Column('player4_healing_ps', Float),
    Column('player4_vision_score_pm', Float),
    Column('player4_seconds_dead', Integer),
    Column('player5_champ', String(20)),
    Column('player5_side', String(4)),
    Column('player5_kills', Integer),
    Column('player5_deaths', Integer),
    Column('player5_assists', Integer),
    Column('player5_kda', Float),
    Column('player5_objective_damage', Integer),
    Column('player5_gold_ps', Float),
    Column('player5_creep_score_pm', Float),
    Column('player5_damage_dealt_ps', Float),
    Column('player5_damage_taken_ps', Float),
    Column('player5_healing_ps', Float),
    Column('player5_vision_score_pm', Float),
    Column('player5_seconds_dead', Integer),
    Column('player6_champ', String(20)),
    Column('player6_side', String(4)),
    Column('player6_kills', Integer),
    Column('player6_deaths', Integer),
    Column('player6_assists', Integer),
    Column('player6_kda', Float),
    Column('player6_objective_damage', Integer),
    Column('player6_gold_ps', Float),
    Column('player6_creep_score_pm', Float),
    Column('player6_damage_dealt_ps', Float),
    Column('player6_damage_taken_ps', Float),
    Column('player6_healing_ps', Float),
    Column('player6_vision_score_pm', Float),
    Column('player6_seconds_dead', Integer),
    Column('player7_champ', String(20)),
    Column('player7_side', String(4)),
    Column('player7_kills', Integer),
    Column('player7_deaths', Integer),
    Column('player7_assists', Integer),
    Column('player7_kda', Float),
    Column('player7_objective_damage', Integer),
    Column('player7_gold_ps', Float),
    Column('player7_creep_score_pm', Float),
    Column('player7_damage_dealt_ps', Float),
    Column('player7_damage_taken_ps', Float),
    Column('player7_healing_ps', Float),
    Column('player7_vision_score_pm', Float),
    Column('player7_seconds_dead', Integer),
    Column('player8_champ', String(20)),
    Column('player8_side', String(4)),
    Column('player8_kills', Integer),
    Column('player8_deaths', Integer),
    Column('player8_assists', Integer),
    Column('player8_kda', Float),
    Column('player8_objective_damage', Integer),
    Column('player8_gold_ps', Float),
    Column('player8_creep_score_pm', Float),
    Column('player8_damage_dealt_ps', Float),
    Column('player8_damage_taken_ps', Float),
    Column('player8_healing_ps', Float),
    Column('player8_vision_score_pm', Float),
    Column('player8_seconds_dead', Integer),
    Column('player9_champ', String(20)),
    Column('player9_side', String(4)),
    Column('player9_kills', Integer),
    Column('player9_deaths', Integer),
    Column('player9_assists', Integer),
    Column('player9_kda', Float),
    Column('player9_objective_damage', Integer),
    Column('player9_gold_ps', Float),
    Column('player9_creep_score_pm', Float),
    Column('player9_damage_dealt_ps', Float),
    Column('player9_damage_taken_ps', Float),
    Column('player9_healing_ps', Float),
    Column('player9_vision_score_pm', Float),
    Column('player9_seconds_dead', Integer)
)

meta.create_all(engine)

match_crawler(seed_player_name="treyhest", limit=5, region="NA")
