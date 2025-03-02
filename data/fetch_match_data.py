import sqlite3
import pandas as pd

def load_data():
    """
    Load shot data from the database and create match summaries.
    
    Returns:
        tuple: (shot_data, match_summaries) - Two dataframes containing:
               - shot_data: Individual shot data with added 'is_goal' column
               - match_summaries: Aggregated match statistics
    """
    # Connect to database and load shot data
    conn = sqlite3.connect(r"C:\Users\Owner\dev\team-model\data\team_model_db.db")
    shot_data = pd.read_sql_query("SELECT * FROM prem_data", conn)
    conn.close()
    
    # Add a goal column
    shot_data['is_goal'] = shot_data['Outcome'].apply(lambda x: 1 if x == 'Goal' else 0)
    
    # Split into home and away shots
    home_shots = shot_data[shot_data['Team'] == shot_data['home_team']]
    away_shots = shot_data[shot_data['Team'] == shot_data['away_team']]
    
    # Aggregate by match for home teams
    home_stats = home_shots.groupby(['match_url', 'match_date', 'home_team', 'away_team', 'season'], as_index=False).agg({
        'is_goal': 'sum',  # Total goals
        'xG': 'sum',       # Total xG
        'PSxG': 'sum'      # Total PSxG
    })
    
    # Aggregate by match for away teams
    away_stats = away_shots.groupby(['match_url', 'match_date', 'home_team', 'away_team', 'season'], as_index=False).agg({
        'is_goal': 'sum',  # Total goals
        'xG': 'sum',       # Total xG
        'PSxG': 'sum'      # Total PSxG
    })
    
    # Rename columns
    home_stats = home_stats.rename(columns={
        'is_goal': 'home_goals',
        'xG': 'home_xg',
        'PSxG': 'home_psxg'
    })
    
    away_stats = away_stats.rename(columns={
        'is_goal': 'away_goals',
        'xG': 'away_xg',
        'PSxG': 'away_psxg'
    })
    
    # Merge home and away stats
    match_summaries = pd.merge(
        home_stats, 
        away_stats, 
        on=['match_url', 'match_date', 'home_team', 'away_team', 'season'],
        how='inner'
    )
    
    return shot_data, match_summaries

