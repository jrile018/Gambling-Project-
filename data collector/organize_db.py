import sqlite3

def init_db():
    """
    Creates (if needed) the players table in nba_players.db with all necessary columns.
    """
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
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

def remove_duplicates():
    """
    Scans the entire 'players' table for rows that have identical key columns:
      (name, team, position, height, weight, birth_date, birth_country, experience, college)
    If multiple rows share the same key, only the first row is kept, and the rest are deleted.
    Prints logs so you can see which rows are analyzed and removed.
    """
    conn = sqlite3.connect("nba_players.db")
    cursor = conn.cursor()
    
    # Select the rows we need to compare for duplicates
    cursor.execute("""
        SELECT 
            id, 
            name, 
            team, 
            position, 
            height, 
            weight, 
            birth_date, 
            birth_country, 
            experience, 
            college
        FROM players
        ORDER BY id
    """)
    rows = cursor.fetchall()
    
    print("[Duplicates] Checking for duplicates among all players ...")
    
    unique_map = {}   # Maps (key_columns) -> the first row's ID
    to_delete = []    # List of row IDs to remove
    
    for row in rows:
        (row_id, name, team, position, height, weight,
         birth_date, birth_country, experience, college) = row
        
        # Print a line for each row we analyze
        print(f"  [Duplicates] Analyzing row ID={row_id} | {name}, {team}, {position}")
        
        # Build the "unique key" from these columns
        key = (name, team, position, height, weight, birth_date, birth_country, experience, college)
        
        if key in unique_map:
            # We have a duplicate: keep the first, remove this one
            print(f"    [Duplicates] Found duplicate. ID={row_id} marked for removal.")
            to_delete.append(row_id)
        else:
            # This is the first row we've seen with this key
            unique_map[key] = row_id
    
    if to_delete:
        print(f"[Duplicates] Removing {len(to_delete)} duplicate rows from DB...")
        for del_id in to_delete:
            cursor.execute("DELETE FROM players WHERE id = ?", (del_id,))
        conn.commit()
    else:
        print("[Duplicates] No duplicates found.")
    
    conn.close()

if __name__ == "__main__":
    init_db()          # Make sure the table exists
    remove_duplicates() # Remove duplicates
    print("Database organized. No duplicates remain.")
