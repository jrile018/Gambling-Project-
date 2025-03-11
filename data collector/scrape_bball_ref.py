import sqlite3
import time
import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
from organize_db import init_db  # Import the DB initializer

def scrape_all_teams_roster():
    TEAM_ABBRS = [
        "ATL", "BOS", "BRK", "CHO", "CHI", "CLE", "DAL", "DEN", "DET", "GSW",
        "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", "NOP", "NYK",
        "OKC", "ORL", "PHI", "PHO", "POR", "SAC", "SAS", "TOR", "UTA", "WAS"
    ]
    headers_req = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    for team in TEAM_ABBRS:
        url = f"https://www.basketball-reference.com/teams/{team}/2025.html"
        print(f"Scraping {team} from {url} ...")
        time.sleep(20)  # Wait 20 seconds before each team request
        
        try:
            response = requests.get(url, headers=headers_req)
            response.raise_for_status()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue
        
        soup = BeautifulSoup(response.text, "html.parser")
        roster_table = soup.find("table", id="roster")
        if roster_table is None:
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            for comment in comments:
                comment_soup = BeautifulSoup(comment, "html.parser")
                roster_table = comment_soup.find("table", id="roster")
                if roster_table:
                    break
        if not roster_table:
            print(f"No roster table found for team {team}. Skipping.")
            continue

        headers = [th.get_text(strip=True) for th in roster_table.find("thead").find("tr").find_all("th")]
        rows = roster_table.find("tbody").find_all("tr")
        for row in rows:
            cells = row.find_all(["th", "td"])
            row_data = [cell.get_text(strip=True) for cell in cells]
            if row_data:
                player_data = dict(zip(headers, row_data))
                player_data["Team"] = team
                # Check for duplicates using key columns (up to URL)
                cursor.execute("""
                    SELECT id FROM players 
                    WHERE name = ? 
                    AND team = ?
                    AND position = ?
                    AND height = ?
                    AND weight = ?
                    AND birth_date = ?
                    AND birth_country = ?
                    AND experience = ?
                    AND college = ?
                """, (
                    player_data.get("Player"),
                    team,
                    player_data.get("Pos"),
                    player_data.get("Ht"),
                    float(player_data.get("Wt", 0) or 0),
                    player_data.get("Birth Date"),
                    player_data.get("Birth"),
                    player_data.get("Exp"),
                    player_data.get("College")
                ))
                if cursor.fetchone():
                    print(f"Duplicate found: {player_data.get('Player')} already exists. Skipping.")
                    continue
                
                cursor.execute("""
                    INSERT INTO players (name, team, position, height, weight, birth_date, 
                        birth_country, experience, college)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    player_data.get("Player"),
                    team,
                    player_data.get("Pos"),
                    player_data.get("Ht"),
                    float(player_data.get("Wt", 0) or 0),
                    player_data.get("Birth Date"),
                    player_data.get("Birth"),
                    player_data.get("Exp"),
                    player_data.get("College")
                ))
        conn.commit()
    conn.close()
    print("Done! All roster data saved to database.")

def generate_player_urls():
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    def construct_url(player_name, existing_urls):
        parts = player_name.split()
        if len(parts) < 2:
            return None
        first_name = parts[0]
        last_name = parts[-1]
        folder = last_name[0].lower()
        last_part = (last_name[:5] if len(last_name) >= 5 else 
                     last_name + first_name[:5-len(last_name)]).lower()
        first_part = first_name[:2].lower()
        suffix = 1
        while True:
            filename = f"{last_part}{first_part}{suffix:02d}.html"
            url = f"https://www.basketball-reference.com/players/{folder}/{filename}"
            if url not in existing_urls:
                return url
            suffix += 1
            if suffix > 99:
                return None

    cursor.execute("SELECT url FROM players WHERE url IS NOT NULL")
    existing_urls = set(row[0] for row in cursor.fetchall())
    cursor.execute("SELECT id, name FROM players WHERE url IS NULL")
    players = cursor.fetchall()
    for player_id, player_name in players:
        url = construct_url(player_name, existing_urls)
        if url:
            try:
                cursor.execute("UPDATE players SET url = ? WHERE id = ?", (url, player_id))
                existing_urls.add(url)
            except sqlite3.Error as e:
                print(f"Error updating URL for {player_name}: {e}")
    conn.commit()
    conn.close()
    print("Updated database with player URLs")

def get_player_stats(url):
    headers_req = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers_req, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"      [Stats] Request failed: {e}")
        return None, None
    
    soup = BeautifulSoup(r.text, "html.parser")
    switcher_div = soup.find("div", id="switcher_per_game_stats")
    if not switcher_div:
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            if 'id="switcher_per_game_stats"' in comment:
                comment_soup = BeautifulSoup(comment, "html.parser")
                switcher_div = comment_soup.find("div", id="switcher_per_game_stats")
                if switcher_div:
                    print("      [Stats] Found switcher container in comment.")
                    break
    if not switcher_div:
        print("      [Stats] 'switcher_per_game_stats' container not found.")
        return None, None
    div_per_game = switcher_div.find("div", id="div_per_game_stats")
    if not div_per_game:
        print("      [Stats] 'div_per_game_stats' not found inside switcher container.")
        return None, None
    table = div_per_game.find("table", id="per_game_stats")
    if not table:
        print("      [Stats] Table with id='per_game_stats' not found.")
        return None, None
    thead = table.find("thead")
    header_cells = thead.find_all("th")
    table_headers = [th.get_text(strip=True) for th in header_cells]
    tbody = table.find("tbody")
    rows_data = []
    for row in tbody.find_all("tr", class_=lambda x: x != "thead"):
        cells = row.find_all(["th", "td"])
        row_text = [cell.get_text(strip=True) for cell in cells]
        if row_text:
            rows_data.append(row_text)
    if not rows_data:
        print("      [Stats] No rows found in table body.")
        return table_headers, None
    latest_row = None
    for row in reversed(rows_data):
        season_label = row[0].lower()
        if "career" in season_label or "playoffs" in season_label:
            continue
        latest_row = row
        break
    if not latest_row:
        print("      [Stats] No valid season row found.")
    return table_headers, latest_row

def scrape_latest_season_stats():
    desired_stats = ["Season", "G", "GS", "MP", "FG", "FGA", "FG%", "3P", "3PA", "3P%", 
                     "2P", "2PA", "2P%", "eFG%", "FT", "FTA", "FT%", "ORB", "DRB", "TRB",
                     "AST", "STL", "BLK", "TOV", "PF", "PTS"]
    
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    cursor.execute("SELECT name, team, position, url FROM players")
    players = cursor.fetchall()
    print(f"[Stats] Found {len(players)} players in database.")
    
    print("[Stats] Waiting 5 seconds before starting stats scraping...")
    time.sleep(5)
    
    headers_req = {"User-Agent": "Mozilla/5.0"}
    for player_name, team, position, url in players:
        print(f"  [Stats] Waiting 1 second before processing {player_name} - {url}")
        time.sleep(1)
        print(f"  [Stats] Processing {player_name} - {url}")
        if not url:
            print(f"    [Stats] No URL for {player_name}. Skipping.")
            continue
        
        table_headers, latest_row = get_player_stats(url)
        if not latest_row:
            print(f"    [Stats] Could not extract valid stats for {player_name}. Skipping.")
            continue
        
        stats = {}
        for stat in desired_stats:
            idx = None
            for i, header in enumerate(table_headers):
                if header.strip().lower() == stat.strip().lower():
                    idx = i
                    break
            if idx is not None and idx < len(latest_row):
                stats[stat] = latest_row[idx]
            else:
                stats[stat] = "0"
        print(f"    [Stats] Extracted stats for {player_name}: {stats}")
        
        update_query = """
            UPDATE players SET 
                season = ?,
                games = ?,
                games_started = ?,
                minutes_per_game = ?,
                field_goals = ?,
                field_goal_attempts = ?,
                field_goal_percentage = ?,
                three_pointers = ?,
                three_point_attempts = ?,
                three_point_percentage = ?,
                two_pointers = ?,
                two_point_attempts = ?,
                two_point_percentage = ?,
                effective_fg_percentage = ?,
                free_throws = ?,
                free_throw_attempts = ?,
                free_throw_percentage = ?,
                offensive_rebounds = ?,
                defensive_rebounds = ?,
                total_rebounds = ?,
                assists = ?,
                steals = ?,
                blocks = ?,
                turnovers = ?,
                personal_fouls = ?,
                points_per_game = ?
            WHERE url = ?
        """
        try:
            cursor.execute(update_query, (
                stats["Season"],
                float(stats["G"] or 0),
                float(stats["GS"] or 0),
                float(stats["MP"] or 0),
                float(stats["FG"] or 0),
                float(stats["FGA"] or 0),
                float(stats["FG%"] or 0),
                float(stats["3P"] or 0),
                float(stats["3PA"] or 0),
                float(stats["3P%"] or 0),
                float(stats["2P"] or 0),
                float(stats["2PA"] or 0),
                float(stats["2P%"] or 0),
                float(stats["eFG%"] or 0),
                float(stats["FT"] or 0),
                float(stats["FTA"] or 0),
                float(stats["FT%"] or 0),
                float(stats["ORB"] or 0),
                float(stats["DRB"] or 0),
                float(stats["TRB"] or 0),
                float(stats["AST"] or 0),
                float(stats["STL"] or 0),
                float(stats["BLK"] or 0),
                float(stats["TOV"] or 0),
                float(stats["PF"] or 0),
                float(stats["PTS"] or 0),
                url
            ))
            conn.commit()
            print(f"    [Stats] Stats updated for {player_name}.")
        except Exception as e:
            print(f"    [Stats] Error updating stats for {player_name}: {e}")
    conn.close()
    print("[Stats] All player stats have been stored in the database.")

if __name__ == "__main__":
    init_db()
    scrape_all_teams_roster()
    generate_player_urls()
    scrape_latest_season_stats()
