import cassiopeia as cass

cass.apply_settings("config.json")

player = cass.Summoner(name="treyhest", region="NA")
good_with = player.champion_masteries.filter(lambda cm: cm.level >= 6)
print([cm.champion.name for cm in good_with])

for match in player.match_history[0:3]:
    print("\n")
    for participant in match.participants:
        print(participant.champion.name)