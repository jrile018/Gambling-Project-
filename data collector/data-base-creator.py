# scrape NBA player data from Basketball-Reference, store it in an SQLite database #
import sqlite3
import time
import requests
from bs4 import BeautifulSoup, Comment

def init_db():
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    # Create players table without UNIQUE constraint on url
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            team TEXT NOT NULL,
            position TEXT,
            height TEXT,
            weight REAL,
            birth_date TEXT,
            birth_country TEXT,
            experience TEXT,
            college TEXT,
            url TEXT,
            season TEXT,
            games REAL,
            games_started REAL,
            minutes_per_game REAL,
            field_goals REAL,
            field_goal_attempts REAL,
            field_goal_percentage REAL,
            three_pointers REAL,
            three_point_attempts REAL,
            three_point_percentage REAL,
            two_pointers REAL,
            two_point_attempts REAL,
            two_point_percentage REAL,
            effective_fg_percentage REAL,
            free_throws REAL,
            free_throw_attempts REAL,
            free_throw_percentage REAL,
            offensive_rebounds REAL,
            defensive_rebounds REAL,
            total_rebounds REAL,
            assists REAL,
            steals REAL,
            blocks REAL,
            turnovers REAL,
            personal_fouls REAL,
            points_per_game REAL
        )
    """)
    
    conn.commit()
    conn.close()

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
        
        time.sleep(20)  # Wait 20 seconds before each request
        
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
                
                # Check for existing player with same key attributes
                cursor.execute("""
                    SELECT id FROM players 
                    WHERE name = ? 
                    AND height = ? 
                    AND college = ? 
                    AND experience = ?
                """, (
                    player_data.get("Player"),
                    player_data.get("Ht"),
                    player_data.get("College"),
                    player_data.get("Exp")
                ))
                
                existing_player = cursor.fetchone()
                
                if existing_player:
                    print(f"Duplicate found: {player_data.get('Player')} already exists in database. Skipping.")
                    continue
                
                # Insert new player if no duplicate found
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

if __name__ == "__main__":
    init_db()
    scrape_all_teams_roster()
    generate_player_urls()