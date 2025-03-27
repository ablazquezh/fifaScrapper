import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="3w918OsH"
)

mycursor = mydb.cursor()

try:
    mycursor.execute("CREATE DATABASE fantasymbeciles")
except Exception as e:
    print("DB already exists")

mycursor.execute("USE fantasymbeciles;")

creation_queries = ["CREATE TABLE teams (ID INT NOT NULL AUTO_INCREMENT, team_name VARCHAR(100),game VARCHAR(50),team_league VARCHAR(50),team_country VARCHAR(50), PRIMARY KEY (ID));",
                    "CREATE TABLE players (ID INT NOT NULL AUTO_INCREMENT, name VARCHAR(255),nickname VARCHAR(255),country_code VARCHAR(10),age INT ,height INT,average INT,global_position VARCHAR(50),value FLOAT,wage FLOAT,best_foot VARCHAR(50),weak_foot_5stars INT,heading INT,jump INT,long_pass INT,short_pass INT,dribbling INT,acceleration INT,speed INT,shot_power INT,long_shot INT,stamina INT,defense INT,interception INT,team_id_fk INT ,game VARCHAR(50), PRIMARY KEY (ID), FOREIGN KEY (team_id_fk) REFERENCES teams(ID));",
                    "CREATE TABLE positions (ID INT NOT NULL AUTO_INCREMENT, position VARCHAR(10), game VARCHAR(50), PRIMARY KEY (ID));",
                    "CREATE TABLE positions_join (ID INT NOT NULL AUTO_INCREMENT, position_id_fk INT, player_id_fk INT, game VARCHAR(50), PRIMARY KEY (ID), FOREIGN KEY (position_id_fk) REFERENCES positions(ID), FOREIGN KEY (player_id_fk) REFERENCES players(ID));",
                    """
                    CREATE TABLE leagues (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        league_name VARCHAR(255),
                        type ENUM('raw', 'pro'),
                        winter_market_enabled BOOLEAN DEFAULT 0,
                        yellow_cards_suspension INT DEFAULT 0,
                        player_avg_limit INT DEFAULT 0,
                        budget_calculation_type ENUM('restrictive', 'default'),
                        game VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """,
                    """
                    CREATE TABLE users (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        user_name VARCHAR(255)
                    );
                    """,
                    """
                    CREATE TABLE league_participants (
                        ID INT NOT NULL AUTO_INCREMENT, 
                        league_ID_fk INT, 
                        user_ID_fk INT, 
                        team_ID_fk INT,
                        PRIMARY KEY (ID), 
                        FOREIGN KEY (league_ID_fk) REFERENCES leagues(ID), 
                        FOREIGN KEY (user_ID_fk) REFERENCES users(ID),
                        FOREIGN KEY (team_ID_fk) REFERENCES teams(ID)
                    );
                    """,
                    """
                    CREATE TABLE market (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        league_id_fk INT,
                        player_id_fk INT,
                        player_market_status ENUM('sold', 'available'),
                        FOREIGN KEY (league_id_fk) REFERENCES leagues(ID),
                        FOREIGN KEY (player_id_fk) REFERENCES players(ID)
                    );
                    """,
                    """
                    CREATE TABLE player_transfers (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        player_id_fk INT,
                        team_id_fk INT,
                        league_id_fk INT,
                        player_status ENUM('ok', 'suspended', 'injured'),
                        transfer_type ENUM('base', 'transfer_window'),
                        transferred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (player_id_fk) REFERENCES players(ID),
                        FOREIGN KEY (league_id_fk) REFERENCES leagues(ID),
                        FOREIGN KEY (team_id_fk) REFERENCES teams(ID)
                    );
                    """,
                    """
                    CREATE VIEW transfer_history AS
                    SELECT 
                        a.id AS transfer_id,
                        p.id AS player_id,
                        p.name,
                        t_old.team_name AS original_team,
                        t_new.team_name AS reassigned_team,
                        l.league_name AS league_name,
                        a.transferred_at
                    FROM player_transfers a
                    JOIN players p ON a.player_id_fk = p.id
                    JOIN teams t_new ON a.team_id_fk = t_new.id
                    JOIN teams t_old ON p.team_id_fk = t_old.id
                    JOIN leagues l ON a.league_id_fk = l.id
                    ORDER BY a.transferred_at DESC;
                    """,
                    """
                    CREATE VIEW latest_team_from_player AS
                    SELECT 
                        p.id ,
                        p.name,
                        l.id AS league_id,
                        l.league_name AS league_name,
                        COALESCE(t_new.team_name, t_original.team_name) AS current_team
                    FROM players p
                    JOIN leagues l ON l.id IN (SELECT DISTINCT league_id_fk FROM player_transfers)
                    LEFT JOIN teams t_original ON p.team_id_fk = t_original.id
                    LEFT JOIN player_transfers a 
                        ON p.id = a.player_id_fk 
                        AND a.league_id_fk = l.id
                        AND a.transferred_at = (
                            SELECT MAX(transferred_at) 
                            FROM player_transfers 
                            WHERE player_id_fk = p.id 
                            AND league_id_fk = l.id
                        )
                    LEFT JOIN teams t_new ON a.team_id_fk = t_new.id
                    ORDER BY l.id, p.id;
                    """,
                    """
                    CREATE TABLE matches (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        local_team_id_fk INT,
                        visitor_team_id_fk INT,
                        league_id_fk INT,
                        matchday INT,
                        FOREIGN KEY (local_team_id_fk) REFERENCES teams(ID),
                        FOREIGN KEY (visitor_team_id_fk) REFERENCES teams(ID),
                        FOREIGN KEY (league_id_fk) REFERENCES leagues(ID)
                    );
                    """,
                    """
                    CREATE TABLE goals (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        match_id_fk INT,
                        player_id_fk INT,
                        team_id_fk INT,
                        FOREIGN KEY (match_id_fk) REFERENCES matches(ID),
                        FOREIGN KEY (player_id_fk) REFERENCES players(ID),
                        FOREIGN KEY (team_id_fk) REFERENCES teams(ID)
                    );
                    """,
                    """
                    CREATE TABLE cards (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        match_id_fk INT,
                        player_id_fk INT,
                        team_id_fk INT,
                        type ENUM('yellow', 'red'),
                        FOREIGN KEY (match_id_fk) REFERENCES matches(ID),
                        FOREIGN KEY (player_id_fk) REFERENCES players(ID),
                        FOREIGN KEY (team_id_fk) REFERENCES teams(ID)
                    );
                    """,
                    """
                    CREATE VIEW played_matches AS
                    SELECT 
                        m.ID AS match_id,
                        m.matchday,
                        l.ID AS league_id,
                        l.league_name AS league_name,
                        t1.team_name AS local_team,
                        t2.team_name AS visitor_team,
                        (SELECT COUNT(*) FROM goals g WHERE g.match_id_fk = m.ID AND g.team_id_fk = m.local_team_id_fk) AS goals_local,
                        (SELECT COUNT(*) FROM goals g WHERE g.match_id_fk = m.ID AND g.team_id_fk = m.visitor_team_id_fk) AS goals_visitor,
                        (SELECT COUNT(*) FROM cards c WHERE c.match_id_fk = m.ID AND c.team_id_fk = m.local_team_id_fk AND c.type = 'yellow') AS yellow_cards_local,
                        (SELECT COUNT(*) FROM cards c WHERE c.match_id_fk = m.ID AND c.team_id_fk = m.visitor_team_id_fk AND c.type = 'yellow') AS yellow_cards_visitor,
                        (SELECT COUNT(*) FROM cards c WHERE c.match_id_fk = m.ID AND c.team_id_fk = m.local_team_id_fk AND c.type = 'red') AS red_cards_local,
                        (SELECT COUNT(*) FROM cards c WHERE c.match_id_fk = m.ID AND c.team_id_fk = m.visitor_team_id_fk AND c.type = 'red') AS red_cards_visitor
                    FROM matches m
                    JOIN leagues l ON m.league_id_fk = l.ID
                    JOIN teams t1 ON m.local_team_id_fk = t1.ID
                    JOIN teams t2 ON m.visitor_team_id_fk = t2.ID;
                    """,
                    """
                    CREATE VIEW league_table AS
                    SELECT 
                        l.id AS league_id,
                        l.league_name AS league_name,
                        t.ID AS team_id,
                        t.team_name,
                        
                        COUNT(m.id) AS n_played_matches,
                        
                        SUM(CASE 
                            WHEN g.team_goals > g.opponent_goals THEN 1 ELSE 0 
                        END) AS victories,

                        SUM(CASE 
                            WHEN g.team_goals = g.opponent_goals THEN 1 ELSE 0 
                        END) AS draws,

                        SUM(CASE 
                            WHEN g.team_goals < g.opponent_goals THEN 1 ELSE 0 
                        END) AS loses,

                        SUM(CASE 
                            WHEN g.team_goals > g.opponent_goals THEN 3 
                            WHEN g.team_goals = g.opponent_goals THEN 1
                            ELSE 0 
                        END) AS points,

                        SUM(g.team_goals) AS goals_favor,
                        SUM(g.opponent_goals) AS goals_against,
                        SUM(g.team_goals - g.opponent_goals) AS goal_diff,

                        SUM(CASE WHEN c.type = 'yellow' THEN 1 ELSE 0 END) AS yellow_cards,
                        SUM(CASE WHEN c.type = 'red' THEN 1 ELSE 0 END) AS red_cards

                    FROM teams t
                    JOIN matches m ON t.id = m.local_team_id_fk OR t.id = m.visitor_team_id_fk
                    JOIN leagues l ON m.league_id_fk = l.id

                    -- Optimized goal calculation:
                    LEFT JOIN (
                        SELECT 
                            g.match_id_fk,
                            g.team_id_fk,
                            COUNT(g.id) AS team_goals,
                            (SELECT COUNT(*) FROM goals g2 WHERE g2.match_id_fk = g.match_id_fk AND g2.team_id_fk != g.team_id_fk) AS opponent_goals
                        FROM goals g
                        GROUP BY g.match_id_fk, g.team_id_fk
                    ) g ON g.match_id_fk = m.id AND g.team_id_fk = t.id

                    -- Optimized card calculation:
                    LEFT JOIN cards c ON c.match_id_fk = m.id AND c.team_id_fk = t.id

                    GROUP BY l.id, l.league_name, t.ID, t.team_name
                    ORDER BY points DESC, goal_diff DESC, goals_favor DESC;
                    """,
                    """
                    CREATE VIEW team_stats AS
                    WITH player_assignments AS (
                        -- Jugadores en su equipo original
                        SELECT 
                            p.id AS player_id,
                            p.team_id_fk,
                            NULL AS league_id_fk,
                            p.average,
                            p.global_position
                        FROM players p
                        UNION ALL
                        -- Jugadores reasignados en `player_transfers`
                        SELECT 
                            p.id AS player_id,
                            pt.team_id_fk,
                            pt.league_id_fk,
                            p.average,
                            p.global_position
                        FROM players p
                        JOIN player_transfers pt ON p.id = pt.player_id_fk
                    ), 
                    team_base AS (
                        SELECT 
                            team_id_fk,
                            league_id_fk,
                            AVG(average) AS team_avg,
                            STDDEV(average) AS team_std
                        FROM player_assignments
                        GROUP BY team_id_fk, league_id_fk
                    ),
                    filtered_avg AS (
                        SELECT 
                            pa.team_id_fk,
                            pa.league_id_fk,
                            AVG(average) AS team_avg_std
                        FROM player_assignments pa
                        JOIN team_base tb ON pa.team_id_fk = tb.team_id_fk AND pa.league_id_fk <=> tb.league_id_fk
                        WHERE pa.average >= (tb.team_avg - tb.team_std)
                        GROUP BY pa.team_id_fk, pa.league_id_fk
                    )
                    SELECT 
                        pa.team_id_fk, 
                        t.team_name,
                        tb.league_id_fk,
                        l.league_name AS league_name,
                        tb.team_avg,
                        fa.team_avg_std,
                        pa.global_position,
                        AVG(pa.average) AS position_avg,
                        AVG(CASE 
                                WHEN pa.average >= (tb.team_avg - tb.team_std) 
                                THEN pa.average 
                            END) AS position_avg_std
                    FROM player_assignments pa
                    JOIN teams t ON pa.team_id_fk = t.id
                    LEFT JOIN leagues l ON pa.league_id_fk = l.id
                    JOIN team_base tb ON pa.team_id_fk = tb.team_id_fk AND pa.league_id_fk <=> tb.league_id_fk
                    JOIN filtered_avg fa ON pa.team_id_fk = fa.team_id_fk AND pa.league_id_fk <=> fa.league_id_fk
                    GROUP BY pa.team_id_fk, pa.global_position, t.team_name, tb.league_id_fk, l.league_name, tb.team_avg, fa.team_avg_std
                    ORDER BY l.league_name, t.team_name, pa.global_position;
                    """,
                    """
                    CREATE VIEW top_scorers_by_league AS
                    SELECT 
                        l.ID AS league_id,
                        l.league_name AS league_name,
                        p.ID AS player_id,
                        p.name AS player_name,
                        t.team_name AS team_name,
                        COUNT(g.ID) AS goals
                    FROM goals g
                    JOIN players p ON g.player_id_fk = p.ID
                    JOIN teams t ON g.team_id_fk = t.ID
                    JOIN matches m ON g.match_id_fk = m.ID
                    JOIN leagues l ON m.league_id_fk = l.ID
                    GROUP BY l.ID, l.league_name, p.ID, p.name, t.team_name
                    ORDER BY l.ID, goals DESC;
                    """,
                    """
                    CREATE VIEW user_historic_stats AS
                    SELECT 
                        u.ID AS user_id,
                        u.user_name AS user_name,
                        COALESCE(l.type, 'both') AS league_type,

                        -- Total stats across all teams managed by the user
                        SUM(lt.points) AS total_points,
                        SUM(lt.goals_favor) AS total_goals_scored,
                        SUM(lt.goals_against) AS total_goals_received,
                        SUM(lt.goal_diff) AS total_goal_difference,

                        -- Count times user finished in specific positions
                        SUM(CASE WHEN lt.final_rank = 1 THEN 1 ELSE 0 END) AS times_first_place,
                        SUM(CASE WHEN lt.final_rank = 2 THEN 1 ELSE 0 END) AS times_second_place,
                        SUM(CASE WHEN lt.final_rank = 3 THEN 1 ELSE 0 END) AS times_third_place

                    FROM users u
                    JOIN league_participants lp ON lp.user_ID_fk = u.ID
                    JOIN leagues l ON lp.league_ID_fk = l.ID
                    JOIN (
                        -- Get final league rankings
                        SELECT 
                            league_id,
                            team_id,
                            points,
                            goals_favor,
                            goals_against,
                            goal_diff,
                            RANK() OVER (PARTITION BY league_id ORDER BY points DESC, goal_diff DESC, goals_favor DESC) AS final_rank
                        FROM league_table  
                    ) lt ON lt.team_id = lp.team_ID_fk  -- Link to the correct teams from league_participants

                    GROUP BY u.ID, u.user_name, l.type WITH ROLLUP
                    ORDER BY total_points DESC;
                    """
                    ]

for query in creation_queries:
    try:
        mycursor.execute(query)
    except Exception as e:
        print(e)


mydb.close()