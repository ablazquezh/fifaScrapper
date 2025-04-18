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
                        market_enabled BOOLEAN DEFAULT 0,
                        market_type ENUM('season', 'winter'),
                        card_suspension BOOLEAN DEFAULT 0,
                        card_suspension_amount INT,
                        card_reset_amount INT,
                        card_reset_injury BOOLEAN DEFAULT 1,
                        card_reset_red BOOLEAN DEFAULT 1,
                        big_team_multiplier INT,
                        medium_team_multiplier INT,
                        small_team_multiplier INT,
                        win_bonus INT,
                        draw_bonus INT,
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
                        UNIQUE (player_id_fk, team_id_fk, league_id_fk, transferred_at),
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
                        ID CHAR(36) NOT NULL PRIMARY KEY,
                        local_team_id_fk INT,
                        visitor_team_id_fk INT,
                        league_id_fk INT,
                        matchday INT,
                        played BOOL,
                        UNIQUE (local_team_id_fk, visitor_team_id_fk, league_id_fk),
                        FOREIGN KEY (local_team_id_fk) REFERENCES teams(ID),
                        FOREIGN KEY (visitor_team_id_fk) REFERENCES teams(ID),
                        FOREIGN KEY (league_id_fk) REFERENCES leagues(ID)
                    );
                    """,
                    """
                    CREATE TABLE goals (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        match_id_fk CHAR(36),
                        player_id_fk INT,
                        team_id_fk INT,
                        quantity INT,
                        UNIQUE (match_id_fk, player_id_fk),
                        FOREIGN KEY (match_id_fk) REFERENCES matches(ID),
                        FOREIGN KEY (player_id_fk) REFERENCES players(ID),
                        FOREIGN KEY (team_id_fk) REFERENCES teams(ID)
                    );
                    """,
                    """
                    CREATE TABLE cards (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        match_id_fk CHAR(36),
                        player_id_fk INT,
                        team_id_fk INT,
                        type ENUM('yellow', 'red'),
                        UNIQUE (match_id_fk, player_id_fk),
                        FOREIGN KEY (match_id_fk) REFERENCES matches(ID),
                        FOREIGN KEY (player_id_fk) REFERENCES players(ID),
                        FOREIGN KEY (team_id_fk) REFERENCES teams(ID)
                    );
                    """,
                    """
                    CREATE TABLE injuries (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        match_id_fk CHAR(36),
                        player_id_fk INT,
                        team_id_fk INT,
                        UNIQUE (match_id_fk, player_id_fk),
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
                        CAST((SELECT COUNT(*) FROM goals g WHERE g.match_id_fk = m.ID AND g.team_id_fk = m.local_team_id_fk) AS CHAR) AS goals_local,
                        CAST((SELECT COUNT(*) FROM goals g WHERE g.match_id_fk = m.ID AND g.team_id_fk = m.visitor_team_id_fk) AS CHAR) AS goals_visitor,
                        CAST((SELECT COUNT(*) FROM cards c WHERE c.match_id_fk = m.ID AND c.team_id_fk = m.local_team_id_fk AND c.type = 'yellow') AS CHAR) AS yellow_cards_local,
                        CAST((SELECT COUNT(*) FROM cards c WHERE c.match_id_fk = m.ID AND c.team_id_fk = m.visitor_team_id_fk AND c.type = 'yellow') AS CHAR) AS yellow_cards_visitor,
                        CAST((SELECT COUNT(*) FROM cards c WHERE c.match_id_fk = m.ID AND c.team_id_fk = m.local_team_id_fk AND c.type = 'red') AS CHAR) AS red_cards_local,
                        CAST((SELECT COUNT(*) FROM cards c WHERE c.match_id_fk = m.ID AND c.team_id_fk = m.visitor_team_id_fk AND c.type = 'red') AS CHAR) AS red_cards_visitor
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

                        -- Count of played matches
                        COALESCE(CAST(COUNT(m.id) AS CHAR), '0') AS n_played_matches,

                        -- Victories, draws, losses
                        COALESCE(CAST(SUM(CASE 
                            WHEN g.team_goals > g.opponent_goals THEN 1 ELSE 0 
                        END) AS CHAR), '0') AS victories,

                        COALESCE(CAST(SUM(CASE 
                            WHEN g.team_goals = g.opponent_goals THEN 1 ELSE 0 
                        END) AS CHAR), '0') AS draws,

                        COALESCE(CAST(SUM(CASE 
                            WHEN g.team_goals < g.opponent_goals THEN 1 ELSE 0 
                        END) AS CHAR), '0') AS loses,

                        -- Points
                        COALESCE(CAST(SUM(CASE 
                            WHEN g.team_goals > g.opponent_goals THEN 3
                            WHEN g.team_goals = g.opponent_goals THEN 1
                            ELSE 0 
                        END) AS CHAR), '0') AS points,

                        -- Goal stats
                        COALESCE(CAST(SUM(g.team_goals) AS CHAR), '0') AS goals_favor,
                        COALESCE(CAST(SUM(g.opponent_goals) AS CHAR), '0') AS goals_against,
                        COALESCE(CAST(SUM(g.team_goals - g.opponent_goals) AS CHAR), '0') AS goal_diff,

                        -- Cards
                        COALESCE(CAST(SUM(CASE WHEN c.type = 'yellow' THEN 1 ELSE 0 END) AS CHAR), '0') AS yellow_cards,
                        COALESCE(CAST(SUM(CASE WHEN c.type = 'red' THEN 1 ELSE 0 END) AS CHAR), '0') AS red_cards

                    FROM teams t
                    JOIN league_participants_view lpv ON t.id = lpv.team_id
                    JOIN leagues l ON lpv.league_ID_fk = l.id

                    -- Only include played matches from the current league
                    LEFT JOIN matches m 
                        ON (t.id = m.local_team_id_fk OR t.id = m.visitor_team_id_fk) 
                        AND m.played = TRUE 
                        AND m.league_id_fk = l.id

                    -- Goal subquery scoped to the same league
                    LEFT JOIN (
                        SELECT 
                            m.id AS match_id_fk,
                            team.id AS team_id_fk,
                            COALESCE(SUM(g.quantity), 0) AS team_goals,
                            (
                                SELECT COALESCE(SUM(g2.quantity), 0)
                                FROM goals g2
                                WHERE g2.match_id_fk = m.id AND g2.team_id_fk != team.id
                            ) AS opponent_goals
                        FROM matches m
                        JOIN (
                            SELECT id FROM teams
                        ) team ON team.id = m.local_team_id_fk OR team.id = m.visitor_team_id_fk
                        LEFT JOIN goals g ON g.match_id_fk = m.id AND g.team_id_fk = team.id
                        WHERE m.played = TRUE AND m.league_id_fk IS NOT NULL -- you may add: AND m.league_id_fk = ??
                        GROUP BY m.id, team.id
                    ) g ON g.match_id_fk = m.id AND g.team_id_fk = t.id

                    -- Only cards from matches in the current league
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
                        p.nickname AS player_name,
                        latest_team.team_name AS team_name,
                        CAST(SUM(g.quantity) AS CHAR) AS goals
                    FROM goals g
                    JOIN players p ON g.player_id_fk = p.ID
                    JOIN matches m ON g.match_id_fk = m.ID
                    JOIN leagues l ON m.league_id_fk = l.ID

                    -- Join to latest team based on latest transfer
                    LEFT JOIN (
                        SELECT 
                            pt.player_id_fk,
                            pt.league_id_fk,
                            t.team_name
                        FROM player_transfers pt
                        JOIN teams t ON pt.team_id_fk = t.ID
                        WHERE pt.transferred_at = (
                            SELECT MAX(sub_pt.transferred_at)
                            FROM player_transfers sub_pt
                            WHERE sub_pt.player_id_fk = pt.player_id_fk
                            AND sub_pt.league_id_fk = pt.league_id_fk
                        )
                    ) AS latest_team ON latest_team.player_id_fk = p.ID AND latest_team.league_id_fk = l.ID

                    GROUP BY l.ID, l.league_name, p.ID, p.nickname, latest_team.team_name
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
                    p.nickname as player_name,
                    p.game AS game,
                    p.team_id_fk AS team_id,
                    t.team_name as team_name
                    FROM players p
                    JOIN teams t
                    ON p.team_id_fk = t.id
                    AND p.game = t.game;
                    """,
                    """
                    CREATE VIEW raw_league_teams AS
                    SELECT 
                        p.id AS player_id,
                        p.nickname AS player_name,
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

                        

                        -- Estadísticas PIVOTEADAS por posición:

                        AVG(CASE WHEN pa.global_position = 'Portero' THEN pa.average END) AS GK_avg,

                        AVG(CASE WHEN pa.global_position = 'Defensa' THEN pa.average END) AS DEF_avg,

                        AVG(CASE WHEN pa.global_position = 'Centrocampista' THEN pa.average END) AS MID_avg,

                        AVG(CASE WHEN pa.global_position = 'Delantero' THEN pa.average END) AS FWD_avg,



                        -- Media solo de los jugadores que cumplen el filtro de desviación estándar

                        AVG(CASE WHEN pa.global_position = 'Portero' AND pa.average >= (tb.team_avg - tb.team_std) THEN pa.average END) AS GK_avg_std,

                        AVG(CASE WHEN pa.global_position = 'Defensa' AND pa.average >= (tb.team_avg - tb.team_std) THEN pa.average END) AS DEF_avg_std,

                        AVG(CASE WHEN pa.global_position = 'Centrocampista' AND pa.average >= (tb.team_avg - tb.team_std) THEN pa.average END) AS MID_avg_std,

                        AVG(CASE WHEN pa.global_position = 'Delantero' AND pa.average >= (tb.team_avg - tb.team_std) THEN pa.average END) AS FWD_avg_std,



                        t.game,

                        t.team_league,

                        t.team_country

                    FROM player_assignments pa

                    JOIN teams t ON pa.team_id_fk = t.id

                    LEFT JOIN leagues l ON pa.league_id_fk = l.id

                    JOIN team_base tb ON pa.team_id_fk = tb.team_id_fk AND pa.league_id_fk <=> tb.league_id_fk

                    JOIN filtered_avg fa ON pa.team_id_fk = fa.team_id_fk AND pa.league_id_fk <=> fa.league_id_fk

                    GROUP BY pa.team_id_fk, t.team_name, tb.league_id_fk, l.league_name, tb.team_avg, fa.team_avg_std

                    ORDER BY l.league_name, t.team_name;

                    """,
                    """
                    CREATE TABLE team_budget (
                        ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        team_id INT,
                        team_name VARCHAR(100),
                        team_avg_std DECIMAL(10, 2),
                        budget INT,  
                        restricted_budget INT,
                        game VARCHAR(10),
                        league_id_fk INT,
                        UNIQUE (team_name, game, league_id_fk),
                        FOREIGN KEY (league_ID_fk) REFERENCES leagues(ID)
                    );
                    """,
                    """
                    INSERT INTO team_budget (team_id, team_name, team_avg_std, budget, restricted_budget, game)
                    WITH team_avg_std_cte AS (
                        SELECT 
                            t.id AS team_id,
                            t.team_name,
                            t.game,
                            FLOOR(ts.team_avg_std) AS team_avg_std
                        FROM teams t
                        JOIN team_stats ts ON t.id = ts.team_id_fk
                        WHERE ts.league_id_fk IS NULL
                    ),
                    distinct_std_game AS (
                        SELECT DISTINCT
                            game,
                            team_avg_std
                        FROM team_avg_std_cte
                    ),
                    qualified_players_grouped AS (
                        SELECT
                            sg.game,
                            sg.team_avg_std,
                            p.value
                        FROM players p
                        JOIN distinct_std_game sg 
                        ON p.game = sg.game AND (
                        (sg.team_avg_std >= 81 AND p.average >= sg.team_avg_std - 4) OR
                        (sg.team_avg_std < 81 AND p.average >= sg.team_avg_std)  OR
                        (sg.team_avg_std > 75 AND p.average >= sg.team_avg_std - 2) 
                    )
                    ),
                    restricted_players_grouped AS (
                        SELECT
                            sg.game,
                            sg.team_avg_std,
                            p.value
                        FROM players p
                        JOIN distinct_std_game sg 
                        ON p.game = sg.game AND (
                        (sg.team_avg_std > 81 AND p.average >= sg.team_avg_std - 4) OR
                        (sg.team_avg_std <= 81 AND p.average >= sg.team_avg_std)  OR
                        (sg.team_avg_std > 75 AND p.average >= sg.team_avg_std - 2) 
                    )
                    ),
                    budget_calc AS (
                        SELECT
                            game,
                            team_avg_std,
                            ROUND(AVG(value), 0) AS budget
                        FROM qualified_players_grouped
                        GROUP BY game, team_avg_std
                    ),
                    restricted_budget_calc AS (
                        SELECT
                            game,
                            team_avg_std,
                            ROUND(AVG(value), 0) AS restricted_budget
                        FROM restricted_players_grouped
                        GROUP BY game, team_avg_std
                    )
                    SELECT
                        t.team_id,
                        t.team_name,
                        t.team_avg_std,
                        b.budget,
                        rb.restricted_budget,
                        t.game
                    FROM team_avg_std_cte t
                    LEFT JOIN budget_calc b 
                    ON b.team_avg_std = t.team_avg_std AND b.game = t.game
                    LEFT JOIN restricted_budget_calc rb 
                    ON rb.team_avg_std = t.team_avg_std AND rb.game = t.game;
                    """
                    ]

for query in creation_queries:
    try:
        mycursor.execute(query)
    except Exception as e:
        print(e)


mydb.close()