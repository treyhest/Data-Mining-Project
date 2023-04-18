import cassiopeia as cass
import sqlalchemy
import json

# Cassiopeia is a Python library for interacting with the Riot Games API.
# Here is a link to the documentation.
# https://cassiopeia.readthedocs.io/en/latest/

cass.apply_settings("config.json") # You will need to set this up individually.

#Tentative MySQL Stuff, disabled until implementation.
"""with open("dbinfo.json", "r") as read_file:
    info = json.load(read_file)
    username = info["username"]
    password = info["password"]
    host = info["host"]
    port = info["port"]
    database = info["database"]

print(sqlalchemy.__version__)


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

engine = get_connection()"""

def match_crawler(seed_player_name: str, limit: int, region: str, step=1):
    # The thought process is, with the correct players that play daily (like streamers), we can assuredly get seed IDs that are relatively recent This is NOT future proof. 
    """
    (Naively) Crawls through matches by match ID sequentially, from the seed_player's first match, decrementing through earlier matches at a given step, (default is 1). 
    """
    seed_player = cass.Summoner(name=seed_player_name, region=region)
    current_id = seed_player.match_history[0].id
    for i in range(limit):
        match = cass.get_match(id=current_id, region=region)
        if is_match_valid(match):
            match_logger(match)
        current_id -= step

def match_logger(match: cass.Match):
    #TODO Log Matches into a DB

    # These are some random match stats for example use.
    print([participant.champion.name for participant in match.participants])
    print("Blue team won") if match.blue_team.win else print("Red team won")
    print("Blue team got first inhibitor") if match.blue_team.first_inhibitor else print("Red team got first inhibitor")
    print("Blue team got first tower") if match.blue_team.first_tower else print("Red team got first tower")
    print("Blue team got first blood") if match.blue_team.first_blood else print("Red team got first blood")
    print("Blue team got first dragon") if match.blue_team.first_dragon else print("Red team got first dragon")

def is_match_valid(match: cass.Match):
    """Returns true if a a given Match is acceptable for data gathering, false otherwise."""
    # TODO Implement fully

    is_valid = False
    try:
        # A try statement to catch "recall error" and related errors which are prone to occur for some matches.
        if ( not match.is_remake and
             match.mode.value == "CLASSIC"): 
            is_valid = True           
    finally:
        return is_valid

match_crawler(seed_player_name="treyhest", limit=5, region="NA")