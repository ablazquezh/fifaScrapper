import mysql.connector
import pandas as pd
import numpy as np

game_name = ["fifa13", "fifa19"]

file_order = {"teams":0, "players":0, "positions":0, "positions_join":0}

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="3w918OsH"
)

mycursor = mydb.cursor()

mycursor.execute("USE fantasymbeciles;")

mycursor.execute("SET autocommit=0;")

insert_st = ["""INSERT INTO teams(team_name,game,team_league,team_country) 
             VALUES(
             %(team_name)s,
             %(game)s,
             %(team_league)s,
             %(team_country)s)""",
            """INSERT INTO players(name,nickname,country_code,age,height,average,global_position,value,wage,best_foot,weak_foot_5stars,heading,jump,long_pass,short_pass,dribbling,acceleration,speed,shot_power,long_shot,stamina,defense,interception,team_id_fk,game) 
            VALUES ( 
            %(name)s,
            %(nickname)s,
            %(country_code)s,
            %(age)s,
            %(height)s,
            %(average)s,
            %(global_position)s,
            %(value)s,
            %(wage)s,
            %(best_foot)s,
            %(weak_foot_5stars)s,
            %(heading)s,
            %(jump)s,
            %(long_pass)s,
            %(short_pass)s,
            %(dribbling)s,
            %(acceleration)s,
            %(speed)s,
            %(shot_power)s
            ,%(long_shot)s,
            %(stamina)s,
            %(defense)s,
            %(interception)s,
            %(team_id_fk)s,
            %(game)s)""",
            """INSERT INTO positions(position,game) 
            VALUES(
            %(position)s,
            %(game)s)""",
            """INSERT INTO positions_join(position_id_fk,player_id_fk,game) 
            VALUES(
            %(position_id_fk)s,
            %(player_id_fk)s,
            %(game)s)"""]

for game in game_name:
    print(game)
    for file, insert_command in zip(list(file_order.keys()), insert_st):
        print(file)
        df = pd.read_csv(f'fifaScrapper/output/{game}/{file}.csv', sep=',')
        df = df.replace({np.nan: None})
        if file == "players":
            df["team_id_fk"] = df["team_id_fk"] + 1 + file_order["teams"]
        if file == "positions_join":
            df["position_id_fk"] = df["position_id_fk"] + 1 + file_order["positions"]
            df["player_id_fk"] = df["player_id_fk"] + 1 + file_order["players"]

        if file == "teams":
            df = df[["team_name", "game", "team_league", "team_country"]]
            df.to_csv(f'fifaScrapper/output/{game}/teams.csv', index=False, sep=',')

        df = df.replace({np.nan: None})

        rows = df.to_dict('records')
        
        mycursor.executemany(insert_command, rows)

        mydb.commit()

    for file in file_order.keys():
        mycursor.execute(f"SELECT count(*) FROM {file};")
        count = mycursor.fetchall()
        file_order[file] = count[0][0]
        
    print("*****")

user_st = """INSERT INTO users (user_name)
        VALUES
            ('Zetunio'),
            ('Alosero13'),
            ('Laidden'),
            ('Javi4511'),
            ('Spartan');
        """

try:
    mycursor.execute(user_st)
except Exception as e:
    print(e)
    
mydb.commit()

mydb.close()