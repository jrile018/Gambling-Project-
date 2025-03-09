import time
import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd

def scrape_latest_season_stats(input_csv="player_info.csv", output_csv="player_info.csv"):
    """
    Reads 'player_info.csv' which must have a 'URL' column pointing to each player's
    Basketball Reference page. For each URL, scrapes the last (most recent) season row
    from the per-game table (using the same approach as the LeBron example).
    If scraping fails, stats are set to 0.
    The scraped stats are then appended as new columns in the CSV.
    """
    
    # The stat columns to extract (order should match the table)
    stat_columns = [
        "Season", "Age", "Tm", "Lg", "Pos", "G", "GS", "MP",
        "FG", "FGA", "FG%", "3P", "3PA", "3P%", "2P", "2PA",
        "2P%", "eFG%", "FT", "FTA", "FT%", "ORB", "DRB", "TRB",
        "AST", "STL", "BLK", "TOV", "PF", "PTS"
    ]
    
    print(f"[Main] Reading CSV file '{input_csv}' ...")
    df = pd.read_csv(input_csv)
    print(f"[Main] Read {len(df)} rows from CSV.")
    
    # Initialize new stat columns to zero
    for col in stat_columns:
        df[col] = 0
    
    def scrape_player_stats(url):
        print(f"  [Scrape] Processing URL: {url}")
        # Prepare a dictionary with all zeros (in case we need to return it)
        zero_dict = {col: 0 for col in stat_columns}
        
        # If URL is missing, return zeros
        if not url or pd.isna(url):
            print("    [Scrape] URL is missing. Returning zeros.")
            return zero_dict
        
        # Request the page with a custom User-Agent
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36"
            )
        }
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            print("    [Scrape] Request successful.")
        except Exception as e:
            print(f"    [Scrape] Request failed: {e}. Returning zeros.")
            return zero_dict
        
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Locate the switcher container (like in LeBron's code)
        switcher_div = soup.find("div", id="switcher_per_game_stats")
        if not switcher_div:
            print("    [Scrape] 'switcher_per_game_stats' not found in normal HTML; searching comments...")
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            for comment in comments:
                if 'id="switcher_per_game_stats"' in comment:
                    comment_soup = BeautifulSoup(comment, "html.parser")
                    switcher_div = comment_soup.find("div", id="switcher_per_game_stats")
                    if switcher_div:
                        print("    [Scrape] Found 'switcher_per_game_stats' in HTML comment.")
                        break
        if not switcher_div:
            print("    [Scrape] Could not find the 'switcher_per_game_stats' container. Returning zeros.")
            return zero_dict
        
        # Find the nested div that contains the per-game table
        div_per_game = switcher_div.find("div", id="div_per_game_stats")
        if not div_per_game:
            print("    [Scrape] Could not find 'div_per_game_stats' inside the switcher container. Returning zeros.")
            return zero_dict
        
        # Find the actual table (often with id="per_game_stats")
        table = div_per_game.find("table", id="per_game_stats")
        if not table:
            print("    [Scrape] Could not find table with id='per_game_stats'. Returning zeros.")
            return zero_dict
        
        print("    [Scrape] Found table 'per_game_stats'.")
        
        # Extract headers from the table
        thead = table.find("thead")
        header_cells = thead.find_all("th")
        headers_list = [th.get_text(strip=True) for th in header_cells]
        print(f"    [Scrape] Table headers: {headers_list}")
        
        # Extract rows from tbody
        tbody = table.find("tbody")
        if not tbody:
            print("    [Scrape] No tbody found in table. Returning zeros.")
            return zero_dict
        
        rows_data = []
        for row in tbody.find_all("tr", class_=lambda x: x != "thead"):
            cells = row.find_all(["th", "td"])
            row_text = [cell.get_text(strip=True) for cell in cells]
            if row_text:
                rows_data.append(row_text)
        print(f"    [Scrape] Found {len(rows_data)} rows in tbody.")
        
        if not rows_data:
            print("    [Scrape] No rows extracted from table. Returning zeros.")
            return zero_dict
        
        # Identify the last valid season row (skip "Career" or "Playoffs")
        latest_season_row = None
        for row_data in reversed(rows_data):
            season_label = row_data[0].lower()
            if "career" in season_label or "playoffs" in season_label:
                print(f"      [Scrape] Skipping row with season '{season_label}'.")
                continue
            latest_season_row = row_data
            print(f"      [Scrape] Selected latest season row: {latest_season_row}")
            break
        
        if not latest_season_row:
            print("    [Scrape] No valid season row found. Returning zeros.")
            return zero_dict
        
        # Map extracted row to stat_columns (assumes order is matching)
        row_stats = {}
        for i, col in enumerate(stat_columns):
            if i < len(latest_season_row):
                row_stats[col] = latest_season_row[i]
            else:
                row_stats[col] = "0"
        print(f"    [Scrape] Extracted stats: {row_stats}")
        return row_stats
    
    # Loop over each player and scrape their stats
    print("[Main] Starting to scrape individual player stats ...")
    for idx, row in df.iterrows():
        print(f"  [Main] Waiting 10 seconds before processing row {idx} ...")
        time.sleep(10)
        url = row.get("URL", "")
        print(f"  [Main] Processing row {idx} with URL: {url}")
        stats_dict = scrape_player_stats(url)
        # Update DataFrame with scraped stats
        for col in stat_columns:
            val = stats_dict[col]
            try:
                val_num = float(val)
                df.at[idx, col] = val_num
                print(f"    [Main] Converted {col}='{val}' to {val_num}.")
            except ValueError:
                df.at[idx, col] = val
                print(f"    [Main] Keeping {col} as string: '{val}'.")
    
    df.to_csv(output_csv, index=False)
    print(f"[Main] Done! Updated stats appended to '{output_csv}'.")

if __name__ == "__main__":
    scrape_latest_season_stats("player_info.csv", "player_info.csv")
