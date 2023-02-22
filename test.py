import cassiopeia as cass

cass.set_riot_api_key("Your Key Here")

kalturi = cass.Summoner(name="treyhest", region="NA")
good_with = kalturi.champion_masteries.filter(lambda cm: cm.level >= 6)
print([cm.champion.name for cm in good_with])

# At the time of writing this, this prints:

['Heimerdinger', 'Morgana', 'Bard', 'Zyra', 'Zilean', "Vel'Koz"]