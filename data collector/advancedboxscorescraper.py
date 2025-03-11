import sqlite3
import requests
from bs4 import BeautifulSoup, Comment
import time

def ensure_advanced_boxscores_table():
    """
    Creates (if needed) a table 'advanced_box_scores_per_game' 
    to store advanced box-score stats for each player.
    """
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    # Example columns for advanced stats. 
    # Adjust or add columns to match what the advanced table actually provides.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS advanced_box_scores_per_game (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            box_score_link TEXT,
            team TEXT,
            player_name TEXT,
            starter INTEGER,
            mp TEXT,             -- minutes (string, e.g. '34:12' or '36')
            ts_pct REAL,
            efg_pct REAL,
            fg3a_per_fga_pct REAL,  -- '3PAr'
            fta_per_fga_pct REAL,   -- 'FTr'
            orb_pct REAL,
            drb_pct REAL,
            trb_pct REAL,
            ast_pct REAL,
            stl_pct REAL,
            blk_pct REAL,
            tov_pct REAL,
            usg_pct REAL,
            ortg INTEGER,
            drtg INTEGER,
            UNIQUE(box_score_link, team, player_name)
        )
    """)
    
    conn.commit()
    conn.close()
    print("[DB] Ensured 'advanced_box_scores_per_game' table exists.")

def parse_advanced_box_score_table(soup, box_score_link, team_name):
    """
    Given the BeautifulSoup for a single team's "advanced" box-score table,
    parse each player's advanced-stat row.
    
    Return a list of dicts, each matching the columns in advanced_box_scores_per_game.
    """
    players_data = []
    tbody = soup.find("tbody")
    if not tbody:
        return players_data
    
    rows = tbody.find_all("tr")
    for row in rows:
        # Skip header or totals rows
        if 'class' in row.attrs and 'thead' in row['class']:
            continue
        
        player_cell = row.find(attrs={"data-stat": "player"})
        if not player_cell:
            continue
        
        player_name = player_cell.get_text(strip=True)
        if not player_name or "Totals" in player_name:
            # skip 'Team Totals' or similar
            continue
        
        # We'll parse the advanced columns. 
        # For example, data-stat="ts_pct", "efg_pct", "fg3a_per_fga_pct", "fta_per_fga_pct", etc.
        # If the cell is missing, we default to 0 or None.
        row_dict = {
            "box_score_link": box_score_link,
            "team": team_name,
            "player_name": player_name,
            "starter": 1 if ('class' in row.attrs and 'starter' in row['class']) else 0,
            "mp": row.find(attrs={"data-stat": "mp"}).get_text(strip=True) if row.find(attrs={"data-stat": "mp"}) else None,
            "ts_pct": float(row.find(attrs={"data-stat": "ts_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "ts_pct"}) else 0,
            "efg_pct": float(row.find(attrs={"data-stat": "efg_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "efg_pct"}) else 0,
            "fg3a_per_fga_pct": float(row.find(attrs={"data-stat": "fg3a_per_fga_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "fg3a_per_fga_pct"}) else 0,
            "fta_per_fga_pct": float(row.find(attrs={"data-stat": "fta_per_fga_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "fta_per_fga_pct"}) else 0,
            "orb_pct": float(row.find(attrs={"data-stat": "orb_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "orb_pct"}) else 0,
            "drb_pct": float(row.find(attrs={"data-stat": "drb_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "drb_pct"}) else 0,
            "trb_pct": float(row.find(attrs={"data-stat": "trb_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "trb_pct"}) else 0,
            "ast_pct": float(row.find(attrs={"data-stat": "ast_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "ast_pct"}) else 0,
            "stl_pct": float(row.find(attrs={"data-stat": "stl_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "stl_pct"}) else 0,
            "blk_pct": float(row.find(attrs={"data-stat": "blk_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "blk_pct"}) else 0,
            "tov_pct": float(row.find(attrs={"data-stat": "tov_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "tov_pct"}) else 0,
            "usg_pct": float(row.find(attrs={"data-stat": "usg_pct"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "usg_pct"}) else 0,
            "ortg": int(row.find(attrs={"data-stat": "off_rtg"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "off_rtg"}) else 0,
            "drtg": int(row.find(attrs={"data-stat": "def_rtg"}).get_text(strip=True) or 0) if row.find(attrs={"data-stat": "def_rtg"}) else 0,
        }
        
        players_data.append(row_dict)
    
    return players_data

def scrape_single_box_score_advanced(link):
    """
    Scrapes the 'advanced' box-score tables for a given box_score_link.
    Returns a list of row dicts with advanced stats for both teams.
    """
    results = []
    if not link:
        return results
    
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(link, headers=headers, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [AdvBoxScore] Request failed for {link}: {e}")
        return results
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # We look for tables with IDs like "box-XXX-game-advanced"
    all_tables = soup.find_all("table")
    for tbl in all_tables:
        tbl_id = tbl.get("id", "")
        if not tbl_id.startswith("box-") or not tbl_id.endswith("-game-advanced"):
            continue
        
        # The <caption> often has the team name, e.g. "Boston Celtics Advanced Stats"
        caption = tbl.find("caption")
        team_name = caption.get_text(strip=True).replace(" Advanced Stats", "") if caption else tbl_id
        
        data_rows = parse_advanced_box_score_table(tbl, link, team_name)
        results.extend(data_rows)
    
    return results

def store_advanced_box_score_rows(rows):
    """
    Insert each advanced-stats row into 'advanced_box_scores_per_game',
    skipping duplicates (via UNIQUE constraint).
    """
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    insert_sql = """
        INSERT OR IGNORE INTO advanced_box_scores_per_game (
            box_score_link,
            team,
            player_name,
            starter,
            mp,
            ts_pct,
            efg_pct,
            fg3a_per_fga_pct,
            fta_per_fga_pct,
            orb_pct,
            drb_pct,
            trb_pct,
            ast_pct,
            stl_pct,
            blk_pct,
            tov_pct,
            usg_pct,
            ortg,
            drtg
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    for r in rows:
        cursor.execute(insert_sql, (
            r["box_score_link"],
            r["team"],
            r["player_name"],
            r["starter"],
            r["mp"],
            r["ts_pct"],
            r["efg_pct"],
            r["fg3a_per_fga_pct"],
            r["fta_per_fga_pct"],
            r["orb_pct"],
            r["drb_pct"],
            r["trb_pct"],
            r["ast_pct"],
            r["stl_pct"],
            r["blk_pct"],
            r["tov_pct"],
            r["usg_pct"],
            r["ortg"],
            r["drtg"]
        ))
    
    conn.commit()
    conn.close()

def scrape_advanced_box_scores_for_all_games():
    """
    1) Ensure the advanced_box_scores_per_game table exists.
    2) Loop over all box_score_links from 'gamelogs'.
    3) For each link, parse advanced stats and store them.
    """
    ensure_advanced_boxscores_table()
    
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    # We'll get all distinct box_score_links from the gamelogs table
    cursor.execute("SELECT DISTINCT box_score_link FROM gamelogs WHERE box_score_link IS NOT NULL")
    all_links = cursor.fetchall()
    conn.close()
    
    print(f"[AdvBoxScore] Found {len(all_links)} distinct box_score_links.")
    
    for (link,) in all_links:
        if not link:
            continue
        
        print(f"  [AdvBoxScore] Processing {link} ...")
        rows = scrape_single_box_score_advanced(link)
        if rows:
            store_advanced_box_score_rows(rows)
            print(f"    [AdvBoxScore] Inserted {len(rows)} advanced player lines.")
        else:
            print("    [AdvBoxScore] No advanced data or request failed.")
        
        # optional short delay
        time.sleep(10)

if __name__ == "__main__":
    scrape_advanced_box_scores_for_all_games()
