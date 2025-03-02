import pandas as pd
import time
import random
import os
import sqlite3
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import requests
from bs4 import BeautifulSoup
import html
import re

# Try to import lxml, use html.parser as fallback
try:
    import lxml
    DEFAULT_PARSER = 'lxml'
except ImportError:
    print("lxml parser not available, using html.parser instead")
    DEFAULT_PARSER = 'html.parser'

class RecentMatchDataScraper:
    def __init__(self, season, days_back=7, headless=True, db_path="team_model_db"):
        self.season = season
        self.days_back = days_back
        self.base_url = f"https://fbref.com/en/comps/9/{season}/schedule/{season}-Premier-League-Scores-and-Fixtures"
        self.match_data = []
        self.db_path = db_path
        self.setup_driver(headless)
        self.cutoff_date = datetime.now() - timedelta(days=days_back)
        print(f"Scraping matches played after: {self.cutoff_date.strftime('%Y-%m-%d')}")
        
    def setup_driver(self, headless):
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.maximize_window()

    def random_delay(self, min_seconds=3, max_seconds=5):
        time.sleep(random.uniform(min_seconds, max_seconds))

    def get_match_data(self, url):
        self.random_delay()
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, DEFAULT_PARSER)

            # Extract match date
            venue_time = soup.find('span', class_='venuetime')
            match_date = venue_time['data-venue-date'] if venue_time else None
            
            # Skip if match is older than cutoff date
            if match_date:
                match_datetime = datetime.strptime(match_date, '%Y-%m-%d')
                if match_datetime < self.cutoff_date:
                    print(f"Skipping match from {match_date} (older than {self.days_back} days)")
                    return None
            
            # Extract teams
            team_stats = soup.find('div', id='team_stats_extra')
            if team_stats:
                teams = team_stats.find_all('div', class_='th')
                teams = [t.text.strip() for t in teams if t.text.strip() != '']
                teams = list(dict.fromkeys(teams))  # Remove duplicates while preserving order
                home_team = teams[0] if len(teams) > 0 else None
                away_team = teams[1] if len(teams) > 1 else None
            else:
                home_team, away_team = None, None
                
            # Extract division
            division_link = soup.find('a', href=lambda x: x and '/comps/' in x and '-Stats' in x)
            division = division_link.text.strip() if division_link else None
            
            # Get shots data
            shots_table = soup.find('table', id='shots_all')
            if not shots_table:
                print(f"No shots table found for {url}")
                return None
            
            # First, get all header rows
            header_rows = shots_table.find_all('tr')[:2]  # Get both header rows
            if len(header_rows) < 2:
                print(f"Invalid shots table structure for {url}")
                return None
                
            # Extract headers from the second row (contains actual column names)
            headers = []
            for th in header_rows[1].find_all(['th', 'td']):
                header_text = th.get_text(strip=True)
                # If header is empty, use a placeholder
                headers.append(header_text if header_text else f"Column_{len(headers)}")
            
            # Make headers unique
            unique_headers = []
            header_counts = {}
            for header in headers:
                if header in header_counts:
                    header_counts[header] += 1
                    unique_headers.append(f"{header}_{header_counts[header]}")
                else:
                    header_counts[header] = 1
                    unique_headers.append(header)
            
            # Extract shot data rows
            rows_data = []
            for tr in shots_table.find_all('tbody')[0].find_all('tr'):
                cols = tr.find_all(['th', 'td'])
                row_data = []
                for col in cols:
                    value = col.get_text(strip=True)
                    if value == '':
                        value = "0"
                    row_data.append(value)
                if len(row_data) == len(unique_headers):  # Only add rows that match header count
                    rows_data.append(row_data)
            
            # Create shots DataFrame with unique headers
            shots_df = pd.DataFrame(rows_data, columns=unique_headers)
            
            # Check for penalties in player names and set event type accordingly
            player_col = [col for col in shots_df.columns if 'Player' in col][0]
            shots_df["Event Type"] = shots_df.apply(
                lambda row: "Penalty" if "(pen)" in str(row[player_col]).lower() else "Shot", 
                axis=1
            )
            
            # Get red cards data
            events = soup.find_all('div', class_=re.compile(r'^event\s'))
            
            times, scores, players, event_types, teams = [], [], [], [], []
            
            for event in events:
                # Time and score
                time_score_div = event.find('div')
                if time_score_div:
                    time_score_text = html.unescape(time_score_div.get_text(strip=True))
                    time = time_score_text.split("'")[0] + "'" if "'" in time_score_text else time_score_text
                    score = time_score_text.split("'")[1] if "'" in time_score_text else ''
                    times.append(time)
                    scores.append(score)
                    
                # Player name
                player = event.find('a').get_text(strip=True) if event.find('a') else ''
                players.append(player)
                
                # Event type
                event_type = 'Unknown'
                for div in event.find_all('div'):
                    if '—' in div.get_text():
                        event_type = div.get_text(strip=True).split('—')[-1].strip()
                        break
                event_types.append(event_type)
                
                # Team
                team_logo = event.find('img', class_='teamlogo')
                if team_logo:
                    team_name = team_logo.get('alt').replace(' Club Crest', '')
                    teams.append(team_name)
                else:
                    teams.append('Unknown')
            
            # Create events DataFrame
            events_df = pd.DataFrame({
                'Time': times,
                'Score': scores,
                'Player': players,
                'Event Type': event_types,
                'Team': teams
            })
            
            # Filter for red cards
            red_cards_df = events_df[events_df['Event Type'].isin(['Red Card', 'Second Yellow Card'])]
            red_cards_df = red_cards_df.reset_index(drop=True)
            
            # Process minute information
            red_cards_df["Minute"] = red_cards_df["Time"].str.extract(r'(\d+)').fillna('0')
            red_cards_df["Outcome"] = "Red Card"
            
            # Process shots DataFrame
            minute_col = [col for col in shots_df.columns if 'Minute' in col][0]
            squad_col = [col for col in shots_df.columns if 'Squad' in col][0]
            outcome_col = [col for col in shots_df.columns if 'Outcome' in col][0]
            xg_col = [col for col in shots_df.columns if 'xG' in col][0]
            psxg_col = [col for col in shots_df.columns if 'PSxG' in col][0]
            
            shots_df["Minute"] = shots_df[minute_col].str.extract(r'(\d+)').fillna('0')
            shots_df["Team"] = shots_df[squad_col]
            shots_df["Outcome"] = shots_df[outcome_col]
            shots_df["xG"] = shots_df[xg_col]
            shots_df["PSxG"] = shots_df[psxg_col]
            
            shots_df = shots_df[["Minute", "Team", "Player", "Event Type", "Outcome", "xG", "PSxG"]]
            
            # Clean up data
            shots_df.drop(shots_df[shots_df['Minute'] == ''].index, inplace=True)
            
            # Prepare red cards DataFrame for merging
            red_cards_df = red_cards_df[["Minute", "Team", "Player", "Event Type", "Outcome"]]
            red_cards_df["xG"] = 0
            red_cards_df["PSxG"] = 0
            
            # Combine DataFrames
            df = pd.concat([shots_df, red_cards_df], ignore_index=True)
            
            # Clean and convert data types
            df["Minute"] = pd.to_numeric(df["Minute"], errors='coerce')
            df["xG"] = pd.to_numeric(df["xG"], errors='coerce')
            df["PSxG"] = pd.to_numeric(df["PSxG"], errors='coerce')
            df.fillna(0.00, inplace=True)
            df.sort_values(by=["Minute"], inplace=True)
            
            # Add match metadata
            df["match_url"] = url
            df["match_date"] = match_date
            df["home_team"] = home_team
            df["away_team"] = away_team
            df["division"] = division
            
            # Add season column (format: 2023 for 2023-24 season, 2024 for 2024-25 season)
            if match_date:
                match_datetime = datetime.strptime(match_date, '%Y-%m-%d')
                # If match date is after August 1st, use the year of the match
                # Otherwise use previous year (for matches Jan-Jul which are part of previous season)
                if match_datetime.month >= 8:
                    df["season"] = match_datetime.year
                else:
                    df["season"] = match_datetime.year - 1
            else:
                # If no date available, extract from season string (e.g. "2023-2024" -> 2023)
                try:
                    df["season"] = int(self.season.split("-")[0])
                except:
                    df["season"] = None
                    
            df = df.reset_index(drop=True)
            
            return df
            
        except Exception as e:
            print(f"Error processing match data for {url}: {str(e)}")
            return None

    def find_fixtures_table(self):
        try:
            table_selectors = [
                f"table#sched_{self.season}_9_1",
                "table.stats_table.sortable",
                "//table[contains(@class, 'stats_table')]"
            ]
            
            for selector in table_selectors:
                try:
                    if selector.startswith("//"):
                        table = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, selector))
                        )
                    else:
                        table = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                    return table
                except TimeoutException:
                    continue
                    
            raise Exception("Could not find fixtures table")
            
        except Exception as e:
            print(f"Error finding fixtures table: {e}")
            return None

    def scrape_matches(self):
        try:
            print(f"\nStarting scrape for season {self.season}, matches from last {self.days_back} days")
            self.driver.get(self.base_url)
            self.random_delay()
            
            table = self.find_fixtures_table()
            if not table:
                raise Exception("Could not find fixtures table")
            
            rows = table.find_elements(By.TAG_NAME, "tr")
            print(f"\nFound {len(rows)-1} total matches to check")
            
            matches_processed = 0
            matches_collected = 0
            
            # Iterate in reverse order to get most recent matches first
            for index, row in enumerate(reversed(rows[1:]), 1):
                try:
                    # Check if the match has a date cell
                    date_cell = row.find_element(By.XPATH, ".//td[@data-stat='date']")
                    match_date_text = date_cell.text.strip()
                    
                    # Skip if there's no date (postponed matches, etc.)
                    if not match_date_text:
                        continue
                    
                    # Convert the date text to a datetime object
                    try:
                        match_date = datetime.strptime(match_date_text, '%Y-%m-%d')
                    except ValueError:
                        # Try alternate date formats if needed
                        try:
                            match_date = datetime.strptime(match_date_text, '%a %d/%m/%Y')
                        except ValueError:
                            print(f"Could not parse date: {match_date_text}")
                            continue
                    
                    # Check if the match is within our date range
                    if match_date < self.cutoff_date:
                        # If we've passed the cutoff date, we can stop processing
                        print(f"Reached matches older than {self.days_back} days, stopping search")
                        break
                    
                    # Try to find match report link
                    match_report = row.find_element(By.XPATH, ".//td/a[text()='Match Report']")
                    
                    if match_report:
                        match_url = match_report.get_attribute('href')
                        matches_processed += 1
                        print(f"\nProcessing match {matches_processed} from {match_date_text}: {match_url}")
                        
                        match_df = self.get_match_data(match_url)
                        if match_df is not None:
                            self.match_data.append(match_df)
                            matches_collected += 1
                            print(f"Successfully processed match data")
                        else:
                            print(f"No data retrieved for match")
                    
                except NoSuchElementException:
                    print("No Match Report link found in this row (match may not have been played yet)")
                    continue
                except Exception as e:
                    print(f"Error processing row: {str(e)}")
                    continue
                
                # Add delay between matches
                self.random_delay(2, 4)
            
            print(f"\nProcessed {matches_processed} matches within date range, collected data for {matches_collected} matches")
            return True
            
        except Exception as e:
            print(f"An error occurred during scraping: {e}")
            return False

    def save_results(self):
        if self.match_data:
            # Combine all match data into a single DataFrame
            combined_df = pd.concat(self.match_data, ignore_index=True)
            
            # Save to CSV
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'recent_matches_{self.days_back}days_{timestamp}.csv'
            
            os.makedirs('data', exist_ok=True)
            filepath = os.path.join('data', filename)
            combined_df.to_csv(filepath, index=False)
            print(f"\nResults saved to {filepath}")
            
            # Save to SQLite database
            try:
                conn = sqlite3.connect(self.db_path)
                print(f"\nConnected to database: {self.db_path}")
                
                # Append data to prem_data table
                combined_df.to_sql('prem_data', conn, if_exists='append', index=False)
                print(f"Successfully appended {len(combined_df)} rows to prem_data table")
                
                # Close connection
                conn.close()
            except Exception as e:
                print(f"Error saving to database: {str(e)}")
                
            print(f"\nTotal events collected: {len(combined_df)}")
            return True
        else:
            print("\nNo match data collected")
            return False

    def cleanup(self):
        if hasattr(self, 'driver'):
            self.driver.quit()
            
    def run(self):
        try:
            if self.scrape_matches():
                self.save_results()
        finally:
            self.cleanup()
            print("\nScript completed")

if __name__ == "__main__":
    # Set the season and number of days to look back
    season = "2024-2025"  # Update with current season
    days_back = 7  # Get matches from last 14 days
    db_path = "team_model_db"  # SQLite database file path
    
    # Check and notify about required packages
    required_packages = {
        'lxml': 'Optional but recommended: pip install lxml',
        'selenium': 'Required for web scraping: pip install selenium',
        'requests': 'Required for HTTP requests: pip install requests',
        'beautifulsoup4': 'Required for HTML parsing: pip install beautifulsoup4',
        'pandas': 'Required for data handling: pip install pandas',
        'sqlite3': 'Built-in to Python for database operations'
    }
    
    for package, install_msg in required_packages.items():
        try:
            if package != 'sqlite3':  # sqlite3 is built-in, no need to import check
                __import__(package)
                print(f"✓ {package} is installed")
        except ImportError:
            print(f"✗ {package} is not installed. {install_msg}")
    
    print("\nStarting scraper...")
    scraper = RecentMatchDataScraper(season, days_back=days_back, headless=True, db_path=db_path)
    scraper.run()