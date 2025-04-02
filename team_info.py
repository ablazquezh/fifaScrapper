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

game_name = ["fifa13", "fifa19"]
base_url = ["https://sofifa.com/teams?type=club&r=130034&set=true&hl=es-ES",
            "https://sofifa.com/teams?type=club&r=190075&set=true&hl=es-ES"]

options = webdriver.ChromeOptions()
#options.add_argument("--headless=new")

for game_name_current, base_url_current in zip(game_name, base_url):

    team_df = pd.read_csv(f'fifaScrapper/output/{game_name_current}/teams.csv', sep=',')
    team_df["team_league"] = ""
    team_df["team_country"] = ""

    driver = webdriver.Chrome(options=options)
    driver.get(base_url_current)

    content = driver.page_source
    soup = BeautifulSoup(content, 'html.parser')

    try:
        modal = driver.find_element(By.CLASS_NAME, 'fc-footer-buttons')
        links = modal.find_elements(By.CSS_SELECTOR, "button[class='fc-button fc-cta-consent fc-primary-button']")   
        links[0].click()
    except Exception as e:
        print(f"******\nSomething went wrong when passing through modal popup.\n{e}\n*********")

    stoppage = 0

    while stoppage < 2:

        content = driver.page_source
        soup = BeautifulSoup(content, 'html.parser')

        table = soup.find('article')
        table_rows = table.find_all('tr')
        
        for idx, i in enumerate(table_rows[1:]):

            team_img_section = i.find_next('td')
            team_info_section = team_img_section.find_next('td')

            all_team_info = team_info_section.find_all('a')
        
            team_name = all_team_info[0].contents[0].strip()
            team_country = all_team_info[1].find("img")["data-src"].split("/")[-1].split(".png")[0].strip()
            team_league= all_team_info[1].contents[1].strip()
            try:
                team_df.loc[team_df['team_name'] == team_name, 'team_league'] = team_league
                team_df.loc[team_df['team_name'] == team_name, 'team_country'] = team_country
            except: 
                print(f"No match for team {team_name}")
            
        modal = driver.find_element(By.CLASS_NAME, 'pagination')
        links = modal.find_elements(By.CLASS_NAME, 'button')   

        if len(links) > 1:
            driver.execute_script("arguments[0].click();", links[1])
        else:
            driver.execute_script("arguments[0].click();", links[0])
            stoppage += 1

    
    team_df.to_csv(f'fifaScrapper/output/{game_name_current}/teams.csv', index=False, sep=',')

    driver.quit()

