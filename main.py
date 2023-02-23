import cassiopeia as cass
import sqlalchemy
import json

# Cassiopeia is a Python library for interacting with the Riot Games API.
# Here is a link to the documentation.
# https://cassiopeia.readthedocs.io/en/latest/

cass.apply_settings("config.json")
with open("dbinfo.json", "r") as read_file:
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


engine = get_connection()
player = cass.Summoner(name="EvahnJM", region="NA")
good_with = player.champion_masteries.filter(lambda cm: cm.level >= 6)
print([cm.champion.name for cm in good_with])

for match in player.match_history[0:3]:
    print("\n")
    for participant in match.participants:
        print(participant.champion.name)
    print("Blue team won") if match.blue_team.win else print("Red team won")
    print("Blue team got first inhibitor") if match.blue_team.first_inhibitor else print("Red team got first inhibitor")
    print("Blue team got first tower") if match.blue_team.first_tower else print("Red team got first tower")
    print("Blue team got first blood") if match.blue_team.first_blood else print("Red team got first blood")
    print("Blue team got first dragon") if match.blue_team.first_dragon else print("Red team got first dragon")
