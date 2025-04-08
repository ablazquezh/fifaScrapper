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
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (league_name)
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
                            
                            -- Replace NULL with 0 when no matches are found
                            COALESCE(COUNT(m.id), 0) AS n_played_matches,
                            
                            -- Replace NULL with 0 when no victories are found
                            COALESCE(SUM(CASE 
                                WHEN g.team_goals > g.opponent_goals THEN 1 ELSE 0 
                            END), 0) AS victories,

                            -- Replace NULL with 0 when no draws are found
                            COALESCE(SUM(CASE 
                                WHEN g.team_goals = g.opponent_goals THEN 1 ELSE 0 
                            END), 0) AS draws,

                            -- Replace NULL with 0 when no losses are found
                            COALESCE(SUM(CASE 
                                WHEN g.team_goals < g.opponent_goals THEN 1 ELSE 0 
                            END), 0) AS loses,

                            -- Replace NULL with 0 when no points are found
                            COALESCE(SUM(CASE 
                                WHEN g.team_goals > g.opponent_goals THEN 3 
                                WHEN g.team_goals = g.opponent_goals THEN 1
                                ELSE 0 
                            END), 0) AS points,

                            -- Replace NULL with 0 when no goals are found
                            COALESCE(SUM(g.team_goals), 0) AS goals_favor,

                            -- Replace NULL with 0 when no opponent goals are found
                            COALESCE(SUM(g.opponent_goals), 0) AS goals_against,

                            -- Replace NULL with 0 when no goal difference is calculated
                            COALESCE(SUM(g.team_goals - g.opponent_goals), 0) AS goal_diff,

                            -- Replace NULL with 0 for yellow cards
                            COALESCE(SUM(CASE WHEN c.type = 'yellow' THEN 1 ELSE 0 END), 0) AS yellow_cards,

                            -- Replace NULL with 0 for red cards
                            COALESCE(SUM(CASE WHEN c.type = 'red' THEN 1 ELSE 0 END), 0) AS red_cards

                        FROM teams t
                        -- Join with league_participants_view to ensure only teams assigned to a league are included
                        JOIN league_participants_view lpv ON t.id = lpv.team_id
                        JOIN leagues l ON lpv.league_ID_fk = l.id
                        
                        LEFT JOIN matches m ON t.id = m.local_team_id_fk OR t.id = m.visitor_team_id_fk
                        LEFT JOIN (
                            SELECT 
                                g.match_id_fk,
                                g.team_id_fk,
                                COUNT(g.id) AS team_goals,
                                (SELECT COUNT(*) FROM goals g2 WHERE g2.match_id_fk = g.match_id_fk AND g2.team_id_fk != g.team_id_fk) AS opponent_goals
                            FROM goals g
                            GROUP BY g.match_id_fk, g.team_id_fk
                        ) g ON g.match_id_fk = m.id AND g.team_id_fk = t.id
                        
                        LEFT JOIN cards c ON c.match_id_fk = m.id AND c.team_id_fk = t.id
                        GROUP BY l.id, l.league_name, t.ID, t.team_name
                        ORDER BY points DESC, goal_diff DESC, goals_favor DESC;
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
                    """,
                    """
                    CREATE VIEW player_positions AS
                    SELECT 
                        p.id AS player_id,
                        p.name AS player_name,
                        pos.id AS position_id,
                        pos.position AS position_name,
                        pj.game
                    FROM players p
                    JOIN positions_join pj ON p.id = pj.player_id_fk
                    JOIN positions pos ON pj.position_id_fk = pos.id;
                    """,
                    """
                    CREATE VIEW league_participants_view AS
                    SELECT 
                    lp.ID AS participant_id,
                    lp.league_ID_fk,
                    u.id AS user_id,       -- Assuming `id` is the primary key in `user` table
                    u.user_name AS user_name,   -- Assuming the `user` table has a `name` column
                    t.id AS team_id,       -- Assuming `id` is the primary key in `team` table
                    t.team_name AS team_name,  -- Assuming the `team` table has a `team_name` column
                    t.game
                    FROM 
                    league_participants lp
                    JOIN 
                    users u ON lp.user_ID_fk = u.id   -- Join with the `user` table
                    JOIN 
                    teams t ON lp.team_ID_fk = t.id;
                    """,
                    """
                    CREATE VIEW pro_league_teams AS
                    SELECT 
                        p.id AS player_id,
                        p.name AS player_name,
                        COALESCE(t_new.id, t_original.id) AS team_id,
                        COALESCE(t_new.team_name, t_original.team_name) AS team_name,
                        latest_transfer.league_id_fk as league_id
                    FROM players p
                    LEFT JOIN teams t_original ON p.team_id_fk = t_original.id
                    LEFT JOIN player_transfers latest_transfer
                        ON p.id = latest_transfer.player_id_fk
                        AND latest_transfer.transferred_at = (
                            SELECT MAX(transferred_at) 
                            FROM player_transfers 
                            WHERE player_id_fk = p.id
                        )
                    LEFT JOIN teams t_new ON latest_transfer.team_id_fk = t_new.id;
                    """,
                    """
                    CREATE VIEW raw_league_teams AS
                    SELECT 
                        p.id AS player_id,
                        p.name AS player_name,
                        t_new.id AS team_id,
                        t_new.team_name AS team_name,
                        latest_transfer.league_id_fk as league_id
                    FROM players p
                    JOIN player_transfers latest_transfer
                        ON p.id = latest_transfer.player_id_fk
                        AND latest_transfer.transferred_at = (
                            SELECT MAX(transferred_at) 
                            FROM player_transfers 
                            WHERE player_id_fk = p.id
                        )
                    JOIN teams t_new ON latest_transfer.team_id_fk = t_new.id;
                    """
                    ]

for query in creation_queries:
    try:
        mycursor.execute(query)
    except Exception as e:
        print(e)


mydb.close()