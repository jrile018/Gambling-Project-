import sqlite3
import requests
from bs4 import BeautifulSoup, Comment
import time

def ensure_boxscores_table():
    """
    Creates a table 'box_scores_per_game' if it doesn't exist.
    We'll store each player's box score line in one row.
    Adjust columns as needed for the stats you want.
    """
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS box_scores_per_game (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            box_score_link TEXT,    -- e.g. https://www.basketball-reference.com/boxscores/202410220BOS.html
            team TEXT,             -- e.g. 'Boston Celtics'
            player_name TEXT,      -- e.g. 'Jayson Tatum'
            starter INTEGER,       -- 1 if starter, 0 if bench
            mp TEXT,               -- minutes played (string, e.g. '34:12')
            fg INTEGER,
            fga INTEGER,
            fg_pct REAL,
            threep INTEGER,        -- 3P
            threepa INTEGER,       -- 3PA
            threep_pct REAL,       -- 3P%
            ft INTEGER,
            fta INTEGER,
            ft_pct REAL,
            orb INTEGER,
            drb INTEGER,
            trb INTEGER,
            ast INTEGER,
            stl INTEGER,
            blk INTEGER,
            tov INTEGER,
            pf INTEGER,
            pts INTEGER,
            plus_minus INTEGER,    -- Some box scores have +/- as integer
            UNIQUE(box_score_link, team, player_name)
        )
    """)
    
    conn.commit()
    conn.close()
    print("[DB] Ensured 'box_scores_per_game' table exists.")

def parse_box_score_table(soup, box_score_link, team_name):
    """
    Given the BeautifulSoup for a single team's "basic" box-score table,
    parse each player's row, skipping 'Totals' or empty rows.
    
    Return a list of dicts, each with columns that match our DB schema.
    """
    # The table might have <table id="box-BOS-game-basic"> or similar.
    # We'll parse each <tr> that has a 'data-row' or 'class' we can use.
    # Usually, each <tbody> has players, plus a final "Team Totals" row.
    
    # We'll gather rows into a list of dictionaries.
    players_data = []
    
    tbody = soup.find("tbody")
    if not tbody:
        return players_data
    
    rows = tbody.find_all("tr")
    for row in rows:
        # Sometimes the row for "Team Totals" or empty rows can be skipped
        if 'class' in row.attrs and 'thead' in row['class']:
            continue
        # Or if the row is labeled as "did not play" or "did not dress" we might skip.
        # We'll do a quick check if there's a "data-stat='player'" cell with a name.
        
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        
        # If there's a 'Totals' or 'Reserves' or something, we might skip.
        # We'll skip if the 'data-stat="player"' cell is empty or says 'Team Totals'
        player_cell = row.find(attrs={"data-stat": "player"})
        if not player_cell:
            continue
        
        player_name = player_cell.get_text(strip=True)
        if not player_name or "Totals" in player_name:
            # skip 'Team Totals'
            continue
        
        # Some row might be a sub-header. If there's no numeric stats, skip.
        # We'll attempt to parse the numeric columns:
        # data-stat="mp", "fg", "fga", "fg3", "fg3a", "ft", "fta", etc.
        
        # We'll build a dictionary for each row.
        # If any columns are missing, we default to 0 or None.
        row_dict = {
            "box_score_link": box_score_link,
            "team": team_name,
            "player_name": player_name,
            "starter": 1 if ('class' in row.attrs and 'starter' in row['class']) else 0,
            # We'll parse each stat. For example, data-stat="mp" for minutes:
            "mp": row.find(attrs={"data-stat": "mp"}).get_text(strip=True) if row.find(attrs={"data-stat": "mp"}) else None,
            "fg":  int(row.find(attrs={"data-stat": "fg"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "fg"}) else 0,
            "fga": int(row.find(attrs={"data-stat": "fga"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "fga"}) else 0,
            "fg_pct": float(row.find(attrs={"data-stat": "fg_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "fg_pct"}) else 0,
            "threep":  int(row.find(attrs={"data-stat": "fg3"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "fg3"}) else 0,
            "threepa": int(row.find(attrs={"data-stat": "fg3a"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "fg3a"}) else 0,
            "threep_pct": float(row.find(attrs={"data-stat": "fg3_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "fg3_pct"}) else 0,
            "ft":   int(row.find(attrs={"data-stat": "ft"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "ft"}) else 0,
            "fta":  int(row.find(attrs={"data-stat": "fta"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "fta"}) else 0,
            "ft_pct": float(row.find(attrs={"data-stat": "ft_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "ft_pct"}) else 0,
            "orb":  int(row.find(attrs={"data-stat": "orb"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "orb"}) else 0,
            "drb":  int(row.find(attrs={"data-stat": "drb"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "drb"}) else 0,
            "trb":  int(row.find(attrs={"data-stat": "trb"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "trb"}) else 0,
            "ast":  int(row.find(attrs={"data-stat": "ast"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "ast"}) else 0,
            "stl":  int(row.find(attrs={"data-stat": "stl"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "stl"}) else 0,
            "blk":  int(row.find(attrs={"data-stat": "blk"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "blk"}) else 0,
            "tov":  int(row.find(attrs={"data-stat": "tov"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "tov"}) else 0,
            "pf":   int(row.find(attrs={"data-stat": "pf"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "pf"}) else 0,
            "pts":  int(row.find(attrs={"data-stat": "pts"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "pts"}) else 0,
            "plus_minus":  int(row.find(attrs={"data-stat": "plus_minus"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "plus_minus"}) else 0,
        }
        
        players_data.append(row_dict)
    
    return players_data

def scrape_single_box_score(link):
    """
    Scrapes the box-score link for the game, returning a list of row-dicts
    for both teams. We'll attempt to parse each team's basic box-score table,
    e.g. <table id="box-<TEAM>-game-basic">.
    
    We'll also try to figure out the home/away teams from the linescore, 
    or from the page structure. For demonstration, we'll guess by 
    scanning the two "box-???-game-basic" tables and reading the <caption>.
    """
    results = []
    if not link:
        return results
    
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(link, headers=headers, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [BoxScore] Request failed for {link}: {e}")
        return results
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Typically we might see tables with ids like:
    #   "box-NYK-game-basic" for the away team
    #   "box-BOS-game-basic" for the home team
    # or vice versa. We can find all tables that match "box-.*-game-basic"
    # Then parse each one separately.
    
    all_tables = soup.find_all("table")
    for tbl in all_tables:
        tbl_id = tbl.get("id", "")
        if not tbl_id.startswith("box-") or not tbl_id.endswith("-game-basic"):
            continue
        
        # Attempt to read the <caption> or something that indicates the team name
        # For example: <caption>New York Knicks Basic and Advanced Stats</caption>
        caption = tbl.find("caption")
        team_name = caption.get_text(strip=True).replace(" Basic and Advanced Stats", "") if caption else tbl_id
        
        # parse the table
        data_rows = parse_box_score_table(tbl, link, team_name)
        results.extend(data_rows)
    
    return results

def store_box_score_rows(rows):
    """
    Insert each row (player box-score line) into 'box_scores_per_game',
    skipping duplicates if (box_score_link, team, player_name) is already present.
    """
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    insert_sql = """
        INSERT OR IGNORE INTO box_scores_per_game (
            box_score_link,
            team,
            player_name,
            starter,
            mp,
            fg,
            fga,
            fg_pct,
            threep,
            threepa,
            threep_pct,
            ft,
            fta,
            ft_pct,
            orb,
            drb,
            trb,
            ast,
            stl,
            blk,
            tov,
            pf,
            pts,
            plus_minus
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    for r in rows:
        cursor.execute(insert_sql, (
            r["box_score_link"],
            r["team"],
            r["player_name"],
            r["starter"],
            r["mp"],
            r["fg"],
            r["fga"],
            r["fg_pct"],
            r["threep"],
            r["threepa"],
            r["threep_pct"],
            r["ft"],
            r["fta"],
            r["ft_pct"],
            r["orb"],
            r["drb"],
            r["trb"],
            r["ast"],
            r["stl"],
            r["blk"],
            r["tov"],
            r["pf"],
            r["pts"],
            r["plus_minus"]
        ))
    
    conn.commit()
    conn.close()

def scrape_box_scores_for_all_games():
    """
    1) Ensure the box_scores_per_game table exists
    2) Grab all box_score_links from gamelogs table
    3) For each link, parse the box score, store each player's line
    """
    ensure_boxscores_table()
    
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    # We'll get all the distinct box_score_link from gamelogs
    cursor.execute("SELECT DISTINCT box_score_link FROM gamelogs WHERE box_score_link IS NOT NULL")
    all_links = cursor.fetchall()
    conn.close()
    
    print(f"[BoxScore] Found {len(all_links)} distinct box_score_links.")
    
    for (link,) in all_links:
        if not link:
            continue
        
        print(f"  [BoxScore] Processing {link} ...")
        rows = scrape_single_box_score(link)
        if rows:
            store_box_score_rows(rows)
            print(f"    [BoxScore] Inserted {len(rows)} player lines.")
        else:
            print("    [BoxScore] No data or request failed.")
        
        # optional short delay to avoid hammering the site
        time.sleep(20)

if __name__ == "__main__":
    scrape_box_scores_for_all_games()
