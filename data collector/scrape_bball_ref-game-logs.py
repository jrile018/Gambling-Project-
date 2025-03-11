import sqlite3
import requests
from bs4 import BeautifulSoup, Comment
import time

def ensure_gamelogs_table():
    """
    Creates the 'gamelogs' table if it doesn't already exist, with columns for
    month, season_year, game_date, start_et, visitor_team, visitor_pts, home_team,
    home_pts, box_score_link, overtime, attendance, notes.
    We do NOT drop the table, so existing data remains intact.
    """
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gamelogs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season_year INTEGER,
            month TEXT,
            game_date TEXT,
            start_et TEXT,
            visitor_team TEXT,
            visitor_pts INTEGER,
            home_team TEXT,
            home_pts INTEGER,
            box_score_link TEXT,
            overtime TEXT,
            attendance TEXT,
            notes TEXT
        )
    """)
    
    conn.commit()
    conn.close()
    print("[DB] 'gamelogs' table ensured with columns (month, season_year, etc.).")

def scrape_month_schedule_to_db(year, month):
    """
    Scrapes the NBA schedule for a given season 'year' and 'month'.
    Example: year=2025, month='october' ->
    https://www.basketball-reference.com/leagues/NBA_2025_games-october.html
    
    For each row, we parse:
      season_year, month, game_date, start_et, visitor_team, visitor_pts,
      home_team, home_pts, box_score_link, overtime, attendance, notes
      
    We check duplicates by (game_date, visitor_team, home_team) 
    (plus optional: year, month if you want to differentiate).
    If a row with the same date + teams is found, we skip insertion.
    """
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_games-{month}.html"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    
    print(f"[Scrape] year={year}, month={month.capitalize()}, URL={url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"[Error] Fetching {url} failed: {e}")
        return
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    # The schedule table typically has id="schedule"
    schedule_table = soup.find("table", id="schedule")
    if not schedule_table:
        # Check commented-out HTML
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            comment_soup = BeautifulSoup(comment, "html.parser")
            schedule_table = comment_soup.find("table", id="schedule")
            if schedule_table:
                print("  [Scrape] Found schedule table in commented HTML.")
                break
    
    if not schedule_table:
        print(f"  [Scrape] No table with id='schedule' found for {month.capitalize()} {year}. Skipping.")
        return
    
    tbody = schedule_table.find("tbody")
    if not tbody:
        print("  [Scrape] No <tbody> found in schedule table. Skipping.")
        return
    
    rows = tbody.find_all("tr", class_=lambda x: x != "thead")
    if not rows:
        print("  [Scrape] No game rows found in schedule table. Skipping.")
        return
    
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    games_inserted = 0
    for row in rows:
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        
        # Typical columns:
        #   0: Date
        #   1: Start (ET)
        #   2: Visitor Team
        #   3: Visitor PTS
        #   4: Home Team
        #   5: Home PTS
        #   6: Box Score link
        #   7: OT
        #   8: Attendance
        #   9: Notes
        if len(cells) < 9:
            continue
        
        game_date = cells[0].get_text(strip=True)
        start_et = cells[1].get_text(strip=True)
        visitor_team = cells[2].get_text(strip=True)
        visitor_pts = cells[3].get_text(strip=True) or "0"
        home_team = cells[4].get_text(strip=True)
        home_pts = cells[5].get_text(strip=True) or "0"
        
        # Box Score link, if present
        link_tag = cells[6].find("a")
        box_score_link = None
        if link_tag:
            href = link_tag.get("href", "")
            box_score_link = f"https://www.basketball-reference.com{href}"
        
        overtime = cells[7].get_text(strip=True)
        attendance = cells[8].get_text(strip=True) if len(cells) > 8 else ""
        notes = cells[9].get_text(strip=True) if len(cells) > 9 else ""
        
        # Check for duplicates by (game_date, visitor_team, home_team).
        # Optionally include (month, year) in the check if you prefer.
        cursor.execute("""
            SELECT id FROM gamelogs
             WHERE game_date = ?
               AND visitor_team = ?
               AND home_team = ?
        """, (game_date, visitor_team, home_team))
        existing = cursor.fetchone()
        if existing:
            print(f"  [Scrape] Duplicate found: {game_date} {visitor_team} @ {home_team}. Skipping.")
            continue
        
        # Insert row
        cursor.execute("""
            INSERT INTO gamelogs (
                season_year,
                month,
                game_date,
                start_et,
                visitor_team,
                visitor_pts,
                home_team,
                home_pts,
                box_score_link,
                overtime,
                attendance,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            year,
            month.capitalize(),
            game_date,
            start_et,
            visitor_team,
            int(visitor_pts),
            home_team,
            int(home_pts),
            box_score_link,
            overtime,
            attendance,
            notes
        ))
        games_inserted += 1
    
    conn.commit()
    conn.close()
    print(f"  [Scrape] Inserted {games_inserted} new games for {month.capitalize()} {year}.")

def scrape_season_schedule_to_db():
    """
    1) Ensures the 'gamelogs' table is created with the needed columns.
    2) Iterates over each year 2004..2025
    3) For each year, scrapes months from October..April
    4) Waits a bit between months to avoid spamming requests
    """
    ensure_gamelogs_table()
    
    months = ["october", "november", "december", "january", "february", "march", "april"]
    
    for year in range(2004, 2026):
        for m in months:
            scrape_month_schedule_to_db(year, m)
            time.sleep(2)  # small delay between each month to be polite to server

if __name__ == "__main__":
    scrape_season_schedule_to_db()
