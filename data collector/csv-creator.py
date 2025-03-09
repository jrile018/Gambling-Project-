import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd

def scrape_all_teams_roster():
    # List of NBA team abbreviations for the 2024-25 season.
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
    
    all_data = []
    common_headers = None  # To capture the table headers once
    
    for team in TEAM_ABBRS:
        url = f"https://www.basketball-reference.com/teams/{team}/2025.html"
        print(f"Scraping {team} from {url} ...")
        try:
            response = requests.get(url, headers=headers_req)
            response.raise_for_status()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Locate the roster table; sometimes it's directly in the HTML.
        roster_table = soup.find("table", id="roster")
        if roster_table is None:
            # If not found, it might be inside HTML comments.
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            for comment in comments:
                comment_soup = BeautifulSoup(comment, "html.parser")
                roster_table = comment_soup.find("table", id="roster")
                if roster_table:
                    break

        if not roster_table:
            print(f"No roster table found for team {team}. Skipping.")
            continue

        # Extract header names from the table's thead.
        header_row = roster_table.find("thead").find("tr")
        headers_list = [th.get_text(strip=True) for th in header_row.find_all("th")]
        if common_headers is None:
            # We'll use the scraped header; note the player's name appears under the column "Player"
            common_headers = headers_list + ["Team"]

        # Extract all rows from the table's tbody.
        rows = roster_table.find("tbody").find_all("tr")
        for row in rows:
            cells = row.find_all(["th", "td"])
            row_data = [cell.get_text(strip=True) for cell in cells]
            if row_data:  # Skip empty rows.
                row_data.append(team)
                all_data.append(row_data)
    
    if all_data and common_headers:
        df = pd.DataFrame(all_data, columns=common_headers)
        df.to_csv("player_info.csv", index=False)
        print("Done! All roster data saved to player_info.csv.")
        print(df)
    else:
        print("No data was scraped.")

def generate_player_urls(input_csv="player_info.csv", output_csv="player_info.csv"):
    """
    Reads the CSV file generated from roster scraping, constructs Basketball-Reference URLs
    for each player using the following convention:
    
      Folder: first letter of the player's last name (lowercase)
      Filename: 
          - Take the first 5 letters of the last name. 
          - If the last name is shorter than 5 letters, pad it with the first letters from the first name until reaching 5 characters.
          - Append the first 2 letters of the first name.
          - Append "01.html".
    
    The constructed URL is then:
        https://www.basketball-reference.com/players/{folder}/{filename}
    """
    df = pd.read_csv(input_csv)

    def construct_url(player_name):
        # Split the name by whitespace.
        parts = player_name.split()
        if len(parts) < 2:
            return None  # If the name doesn't have at least a first and last name, skip.
        first_name = parts[0]
        last_name = parts[-1]
        # Folder: first letter of last name in lowercase.
        folder = last_name[0].lower()
        # Build the filename:
        # Use the first 5 letters of the last name, padding with first name letters if needed.
        if len(last_name) >= 5:
            last_part = last_name[:5].lower()
        else:
            needed = 5 - len(last_name)
            last_part = (last_name + first_name[:needed]).lower()
        # Append the first 2 letters of the first name.
        first_part = first_name[:2].lower()
        filename = f"{last_part}{first_part}01.html"
        return f"https://www.basketball-reference.com/players/{folder}/{filename}"

    # The scraped CSV uses "Player" as the header for the player's name.
    df["URL"] = df["Player"].apply(lambda name: construct_url(name))
    df.to_csv(output_csv, index=False)
    print(f"Updated CSV with player URLs saved to {output_csv}")
    print(df[["Player", "URL"]])

if __name__ == "__main__":
    scrape_all_teams_roster()     # Scrape and create player_info.csv
    generate_player_urls()          # Generate and add player URLs to the CSV
