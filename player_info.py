# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 22:00:02 2020

@author: Alberto
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
import requests


def find_global_position(pos):
    gpos = "Portero"

    for key in position_map.keys():
        if pos in position_map[key]:
            gpos = key

    return gpos


game_name = ["fifa13", "fifa19"]
base_url = ["https://sofifa.com/?r=130034&set=true&showCol%5B%5D=ae&showCol%5B%5D=oa&showCol%5B%5D=pt&showCol%5B%5D=bo&showCol%5B%5D=bp&showCol%5B%5D=vl&showCol%5B%5D=wg&showCol%5B%5D=sp&showCol%5B%5D=hi&showCol%5B%5D=pf&showCol%5B%5D=he&showCol%5B%5D=sh&showCol%5B%5D=lo&showCol%5B%5D=dr&showCol%5B%5D=ac&showCol%5B%5D=ju&showCol%5B%5D=so&showCol%5B%5D=st&showCol%5B%5D=ln&showCol%5B%5D=in&showCol%5B%5D=td&showCol%5B%5D=wk&hl=es-ES",
            "https://sofifa.com/?r=190075&set=true&showCol%5B%5D=ae&showCol%5B%5D=oa&showCol%5B%5D=pt&showCol%5B%5D=bo&showCol%5B%5D=bp&showCol%5B%5D=vl&showCol%5B%5D=wg&showCol%5B%5D=sp&showCol%5B%5D=hi&showCol%5B%5D=pf&showCol%5B%5D=he&showCol%5B%5D=sh&showCol%5B%5D=lo&showCol%5B%5D=dr&showCol%5B%5D=ac&showCol%5B%5D=ju&showCol%5B%5D=so&showCol%5B%5D=st&showCol%5B%5D=ln&showCol%5B%5D=in&showCol%5B%5D=td&showCol%5B%5D=wk&hl=es-ES"]

player_avg_stop = 71

options = webdriver.ChromeOptions()
#options.add_argument("--headless=new")

for game_name_current, base_url_current in zip(game_name, base_url):

    position_map = {
        "Delantero": ["EI", "DC", "ED", "SDI", "SD", "SDD"],
        "Centrocampista": ["MCO", "MI", "MC", "MD", "MCD"],
        "Defensa": ["CAR", "LI", "DFC", "LD", "CAR"]
    }

    def save_image(image_url, code, type):
        img_data = requests.get(image_url).content
        with open(f"output/{game_name_current}/img/{type}/{code}.png", 'wb') as handler:
            handler.write(img_data)

    player_df = pd.DataFrame(columns=['name','nickname', 'country_code', 'age', 'height', 'average', 'global_position', 'value', 'wage', 'best_foot',
                            'weak_foot_5stars', 'heading', 'jump', 'long_pass', 'short_pass', 'dribbling', 'acceleration',
                                'speed', 'shot_power', 'long_shot', 'stamina', 'defense', 'interception', 'team_id_fk', 'game'])

    baseteam_df = pd.DataFrame(columns=['team_name', 'game'])

    positions_df = pd.DataFrame(columns=['position', 'game'])
    positions_join_df = pd.DataFrame(columns=['position_id_fk', 'player_id_fk', 'game'])


    driver = webdriver.Chrome(options=options)
    driver.get(base_url_current)

    content = driver.page_source
    soup = BeautifulSoup(content, 'html.parser')

    processed_teams = []

    stoppage = False

    modal = driver.find_element(By.CLASS_NAME, 'fc-footer-buttons')
    links = modal.find_elements(By.CSS_SELECTOR, "button[class='fc-button fc-cta-consent fc-primary-button']")   
    links[0].click()

    idx_offset = 0

    while stoppage is False:

        content = driver.page_source
        soup = BeautifulSoup(content, 'html.parser')

        table = soup.find('article')
        table_rows = table.find_all('tr')
        
        for idx, i in enumerate(table_rows[1:]):

            # BASE INFO

            player_img_section = i.find_next('td')
            player_img = player_img_section.find('img')['data-src']    
            save_image(player_img, idx + idx_offset, type="player")

            ##################
            player_name_section = player_img_section.find_next('td')

            player_name = player_name_section.find('a')['data-tippy-content'].strip()
            player_nickname = player_name_section.find('a').contents[0].strip()

            print(player_name)

            #player_country = player_name_section.find('img')['src']
            country_code = player_name_section.find('img')['data-src'].split("/")[-1].split(".png")[0].strip()

            positions_content = player_name_section.find_all('a', {'rel': 'nofollow'})
            positions = [x.find('span').contents[0].strip() for x in positions_content]

            for pos in positions:

                if pos not in positions_df.loc[:, "position"].unique():
                    positions_df.loc[len(positions_df)] = [pos, game_name_current] 
                
                poslist = list(positions_df.loc[:, "position"].unique())
                
                positions_join_df.loc[len(positions_join_df)] = [poslist.index(pos), idx + idx_offset, game_name_current]

            ##################
            player_age = int(i.find('td', {'data-col': 'ae'}).contents[0].strip())

            player_avg = int(i.find('td', {'data-col': 'oa'}).find('em').contents[0].strip())

            player_club_section = i.find('td', {'data-col': 'pt'}).find_next('td')
            player_club_name = player_club_section.find('a').contents[0]
            player_club_img = player_club_section.find('img')['data-src']

            player_club_status = player_club_section.find('div', {'class': 'sub'}).contents[0]
            if player_club_status.strip() == "Gratis":
                player_club_name = None
                player_club_img = None
            else:
                player_club_name = player_club_name.strip()
                if player_club_name not in processed_teams:
                    processed_teams.append(player_club_name)  
                save_image(player_club_img, player_club_name, type="team")

            # FORMAT DECISION
            player_value = i.find('td', {'data-col': 'vl'}).contents[0]
            player_value = player_value.replace("€", "")
            if "M" in player_value:
                player_value = float(player_value.replace("M", "").strip())
                player_value = player_value * 1000000
            elif "K" in player_value:
                player_value = float(player_value.replace("K", "").strip())
                player_value = player_value * 1000

            player_wage = i.find('td', {'data-col': 'wg'}).contents[0]
            player_wage = player_wage.replace("€", "")
            if "M" in player_wage:
                player_wage = float(player_wage.replace("M", "").strip())
                player_wage = player_wage * 1000000
            elif "K" in player_wage:
                player_wage = float(player_wage.replace("K", "").strip())
                player_wage = player_wage * 1000

            player_height = int(i.find('td', {'data-col': 'hi'}).contents[0].split("cm /")[0].strip())

            player_bfoot = i.find('td', {'data-col': 'pf'}).contents[0]
            player_bfoot = "Zurdo" if player_bfoot == "Izq." else "Diestro"
            player_weakfoot5stars = int(i.find('td', {'data-col': 'wk'}).contents[0].strip())

            # EXTRA SPECS

            player_head = int(i.find('td', {'data-col': 'he'}).find('em').contents[0].strip())
            player_jump = int(i.find('td', {'data-col': 'ju'}).find('em').contents[0].strip())

            player_spass = int(i.find('td', {'data-col': 'sh'}).find('em').contents[0].strip())
            player_lpass = int(i.find('td', {'data-col': 'lo'}).find('em').contents[0].strip())

            player_drib = int(i.find('td', {'data-col': 'dr'}).find('em').contents[0].strip())

            player_acc = int(i.find('td', {'data-col': 'ac'}).find('em').contents[0].strip())
            player_speed = int(i.find('td', {'data-col': 'sp'}).find('em').contents[0].strip())

            player_shotpw = int(i.find('td', {'data-col': 'so'}).find('em').contents[0].strip())
            player_lshot = int(i.find('td', {'data-col': 'ln'}).find('em').contents[0].strip())

            player_stamina = int(i.find('td', {'data-col': 'st'}).find('em').contents[0].strip())

            player_def = int(i.find('td', {'data-col': 'td'}).find('em').contents[0].strip())
            player_interc = int(i.find('td', {'data-col': 'in'}).find('em').contents[0].strip())

            player_df.loc[len(player_df)] = [player_name, player_nickname, country_code, player_age, player_height, player_avg, find_global_position(positions[0]), player_value,
                                            player_wage, player_bfoot, player_weakfoot5stars, player_head, player_jump, player_lpass, player_spass, player_drib, player_acc,
                                            player_speed, player_shotpw, player_lshot, player_stamina, player_def, player_interc, processed_teams.index(player_club_name) if player_club_name is not None else None, 
                                            game_name_current] 

            if int(player_avg) < player_avg_stop: 
                stoppage = True

        
        modal = driver.find_element(By.CLASS_NAME, 'pagination')
        links = modal.find_elements(By.CLASS_NAME, 'button')   

        if len(links) > 1:
            driver.execute_script("arguments[0].click();", links[1])
        else:
            driver.execute_script("arguments[0].click();", links[0])

        idx_offset = idx_offset + len(table_rows) -1 

    driver.quit()

  
    for team in processed_teams:
        baseteam_df.loc[len(baseteam_df)] = [team, game_name_current]
        

    player_df.to_csv(f'output/{game_name_current}/players.csv', index=False, sep=',')
    baseteam_df.to_csv(f'output/{game_name_current}/teams.csv', index=False, sep=',')
    positions_df.to_csv(f'output/{game_name_current}/positions.csv', index=False, sep=',')
    positions_join_df.to_csv(f'output/{game_name_current}/positions_join.csv', index=False, sep=',')
