import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="3w918OsH"
)

mycursor = mydb.cursor()

mycursor.execute("USE fantasymbeciles;")

mycursor.execute("SELECT count(*) FROM players;")
count = mycursor.fetchall()
print(count[0][0])
mydb.close()


"""
SELECT players.name, teams.team_name, players.game
FROM players
INNER JOIN teams ON players.team_id_fk=teams.ID;

SELECT positions.position, positions.game, positions_join.player_id_fk
FROM positions
INNER JOIN positions_join ON positions.ID=positions_join.position_id_fk;

SELECT players.name, pos.position, players.game
FROM players
INNER JOIN (SELECT positions.position, positions.game, positions_join.player_id_fk
FROM positions
INNER JOIN positions_join ON positions.ID=positions_join.position_id_fk) AS pos
ON pos.player_id_fk=players.ID;

SELECT table_schema "DB Name",
        ROUND(SUM(data_length + index_length) / 1024 / 1024, 1) "DB Size in MB"
FROM information_schema.tables
GROUP BY table_schema;
"""