import requests

def fetch_html(url):
    """
    Fetches the HTML content of the given URL and returns it as a string.
    Raises an HTTPError if the request fails.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

def save_html_to_file(html_content, output_filename):
    """
    Saves the given HTML content to a file.
    """
    with open(output_filename, "w", encoding="utf-8") as file:
        file.write(html_content)

if __name__ == "__main__":
    # URL of the target website
    url = "https://www.basketball-reference.com/boxscores/202410220BOS.html"
    
    # Fetch the HTML
    html_code = fetch_html(url)
    
    # Save the HTML to a file named "october_games.html"
    save_html_to_file(html_code, "october_games.html")
    print("HTML code saved to 'october_games.html'.")
