drop table if exists Games;
drop table if exists Players;

CREATE table Games (
    matchId int not null primary key, 
    result varchar(4) not null, 
    queue varchar(8) not null, 
    region varchar(20) not null, 
    dragonSoul varchar(10),
    firstDragon varchar(4),
    firstTower varchar(4),
    firstBlood varchar(4),
    firstInhibitor varchar(4)
);
 
 CREATE table Players (
    playerId int not null primary key,
    matchId int not null,
    foreign key(matchId) references Games(matchId),
    champion varchar(50) not null, 
    role varchar(7) not null,
    spells varchar(255) not null, 
    kills int not null, 
    deaths int not null, 
    assists int not null
);