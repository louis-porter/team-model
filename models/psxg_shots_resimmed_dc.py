import numpy as np
import pandas as pd
from scipy.stats import poisson
from scipy.optimize import minimize
from sklearn.model_selection import train_test_split
from datetime import datetime, date


class PSxGShotsTeamModel:
    def __init__(self, n_simulations=25):
        # Team attack and defense strength parameters
        self.team_attack = {}
        self.team_defense = {}
        self.home_advantage = 0.0
        self.rho = 0.0  # Dixon-Coles parameter to account for low scoring games
        self.n_simulations = n_simulations

    def _get_unique_teams(self, matches):
        """Extract unique teams from matches."""
        teams = set()
        for match in matches:
            teams.add(match['home_team'])
            teams.add(match['away_team'])
        return teams
    
    def _resimulate_matches_with_xg(self, matches, shot_data):
        # Start with original matches
        expanded_matches = matches.copy()
        
        print(f"Resimulating {len(matches)} matches ({self.n_simulations} simulations each)")
        
        # For each match in the original dataset
        for match in matches:
            match_url = match.get('match_url')
            if not match_url:
                continue  # Skip matches without URL identifier
            
            # Get shots for this match
            home_team = match['home_team']
            away_team = match['away_team']
            
            # Filter shots for this specific match
            match_shots = shot_data[shot_data['match_url'] == match_url].copy()
            
            # Skip matches with no shot data
            if len(match_shots) == 0:
                continue
            
            # Separate home and away shots
            home_shots = match_shots[match_shots['Team'] == home_team]
            away_shots = match_shots[match_shots['Team'] == away_team]
            
            # Get xG values for each shot
            home_psxg_values = home_shots['PSxG'].values
            away_psxg_values = away_shots['PSxG'].values
            
            # Create n_simulations of this match
            for i in range(self.n_simulations):
                # Simulate each shot as a Bernoulli trial with probability = xG
                home_goals_sim = sum(np.random.random() < xg for xg in home_psxg_values)
                away_goals_sim = sum(np.random.random() < xg for xg in away_psxg_values)
                
                # Create a copy of the original match
                sim_match = match.copy()
                
                # Replace actual goals with simulated goals
                sim_match['home_goals'] = home_goals_sim
                sim_match['away_goals'] = away_goals_sim
                
                # Add simulation metadata
                sim_match['is_simulation'] = True
                sim_match['simulation_id'] = i
                sim_match['weight'] = 1.0 / self.n_simulations
                
                # Add to expanded dataset
                expanded_matches.append(sim_match)
        
        # Set weight for original matches
        for match in expanded_matches:
            if not match.get('is_simulation', False):
                match['weight'] = 1.0
        
        print(f"Expanded from {len(matches)} to {len(expanded_matches)} matches")
        return expanded_matches
    
    @staticmethod
    def dc_probability(home_goals, away_goals, lambda_home, lambda_away, rho):
        """Calculate Dixon-Coles adjusted probability for a match outcome."""
        # Base Poisson probabilities
        p_home = poisson.pmf(home_goals, lambda_home)
        p_away = poisson.pmf(away_goals, lambda_away)
        
        # Dixon-Coles adjustment for low-scoring dependencies
        tau = 1.0
        if home_goals == 0 and away_goals == 0:
            tau = 1 - rho
        elif home_goals == 0 and away_goals == 1:
            tau = 1 + rho * lambda_home
        elif home_goals == 1 and away_goals == 0:
            tau = 1 + rho * lambda_away
        elif home_goals == 1 and away_goals == 1:
            tau = 1 - rho * lambda_home * lambda_away
        
        return tau * p_home * p_away
    
    @staticmethod
    def dc_log_likelihood(params, matches, teams, metadata, epsilon=0.01, season_penalty=0.75):
        """Optimized log-likelihood function with season penalty."""
        # Extract parameters
        home_advantage = params[0]
        rho = params[1]
        attack_params = params[2:2+len(teams)]
        defense_params = params[2+len(teams):]
        
        # Assign attack/defense parameters to teams
        attack = {team: attack_params[i] for i, team in enumerate(teams)}
        defense = {team: defense_params[i] for i, team in enumerate(teams)}
        
        # Initialize log likelihood
        log_likelihood = 0
        
        # Get reference values from metadata
        reference_date = metadata.get('reference_date')
        current_season = metadata.get('current_season')
            
        # Calculate log-likelihood for each match
        for match in matches:
            home_team = match['home_team']
            away_team = match['away_team']
            home_goals = match['home_goals']
            away_goals = match['away_goals']
            match_season = match.get('season', current_season)
            
            # Get match weight (1.0 for real matches, fractional for simulations)
            match_weight = match.get('weight', 1.0)
            
            # Weight calculation
            # Time weight - days-based decay
            time_weight = 1.0
            if 'days_from_ref' in match:
                # If we've pre-calculated days from reference
                days_ago = match['days_from_ref']
                time_weight = 1.0 / (1.0 + epsilon * days_ago)
            elif reference_date and 'match_date' in match:
                # If we need to calculate it now
                match_date = match['match_date']
                if isinstance(match_date, str):
                    match_date = pd.Timestamp(match_date)
                days_ago = max(0, (reference_date - match_date).days)
                time_weight = 1.0 / (1.0 + epsilon * days_ago)
            
            # Apply season penalty if match is from a previous season
            seasons_ago = current_season - match_season if current_season and match_season else 0
            if seasons_ago > 0:
                time_weight *= season_penalty ** seasons_ago
            
            # Combine match_weight with time_weight
            combined_weight = time_weight * match_weight
            
            # Expected goals parameter
            lambda_home = attack[home_team] * defense[away_team] * home_advantage
            lambda_away = attack[away_team] * defense[home_team]
            
            # Calculate probability with rho adjustment
            probability = PSxGShotsTeamModel.dc_probability(home_goals, away_goals, lambda_home, lambda_away, rho)
            
            # Safeguard against log(0)
            if probability <= 0:
                probability = 1e-10
            
            log_likelihood += np.log(probability) * combined_weight
        
        # Constraint penalty
        constraint_penalty = 0
        sum_attack = sum(attack.values())
        sum_defense = sum(defense.values())
        constraint_penalty += (sum_attack - len(teams)) ** 2
        constraint_penalty += (sum_defense - len(teams)) ** 2
        
        return -log_likelihood + constraint_penalty
    
    def _preprocess_matches(self, matches, days_ago=365):
        """Preprocess matches to optimize calculations."""   
        # Convert matches to DataFrame for date processing
        matches_df = pd.DataFrame(matches)
        matches_df["match_date"] = pd.to_datetime(matches_df["match_date"])
        
        # Filter by date
        current_date = pd.Timestamp.now()
        cutoff_date = current_date - pd.Timedelta(days=days_ago)
        filtered_df = matches_df[matches_df["match_date"] >= cutoff_date]
        
        # Convert filtered DataFrame back to list of dictionaries
        matches = filtered_df.to_dict('records')
        
        # Get reference date and current season
        dates = [m.get('match_date') for m in matches if m.get('match_date') is not None]
        seasons = [m.get('season', 0) for m in matches]
        
        reference_date = None
        if dates:
            reference_date = max(dates)
            
        current_season = max(seasons) if seasons else None
        
        # Precompute days from reference for each match
        for match in matches:
            if reference_date and 'match_date' in match:
                match_date = match['match_date']
                if isinstance(match_date, str):
                    match_date = pd.Timestamp(match_date)
                
                if isinstance(match_date, (pd.Timestamp, datetime, date)):
                    # Convert datetime to pandas Timestamp if it's not already
                    if not isinstance(match_date, pd.Timestamp):
                        match_date = pd.Timestamp(match_date)
                    
                    # Calculate and store days from reference
                    match['days_from_ref'] = max(0, (reference_date - match_date).days)
        
        # Return both filtered matches and metadata
        return {
            'filtered_matches': matches,
            'reference_date': reference_date,
            'current_season': current_season
        }
        
    def fit_models(self, actual_matches, shot_data, epsilon=0.0065, season_penalty=0.75):
        # First preprocess matches to filter by date
        preprocessing_result = self._preprocess_matches(actual_matches)
        
        # Extract filtered matches and metadata
        filtered_matches = preprocessing_result.pop('filtered_matches')
        matches_metadata = preprocessing_result
        
        # Then resimulate the filtered matches using shot-by-shot data
        resimulated_matches = self._resimulate_matches_with_xg(filtered_matches, shot_data)
        
        # Get unique teams
        teams = self._get_unique_teams(resimulated_matches)
        team_list = sorted(list(teams))
        
        # Fit model with resimulated data and season penalty
        params = self._optimize_dc_parameters(
            resimulated_matches, team_list, matches_metadata, epsilon, season_penalty
        )
        
        # Extract parameters 
        self.home_advantage = params[0]
        self.rho = params[1]
        for i, team in enumerate(team_list):
            self.team_attack[team] = params[2+i]
            self.team_defense[team] = params[2+len(team_list)+i]
        
        return self

    def _optimize_dc_parameters(self, matches, team_list, metadata, epsilon=0.0065, season_penalty=0.75):
        """Optimize Dixon-Coles model parameters."""
        # Add debugging
        print(f"Optimizing for {len(matches)} matches with {len(team_list)} teams")
        
        # Check first few matches
        for i, match in enumerate(matches[:3]):
            print(f"Match {i}: {match}")

        # Initial parameter guesses
        initial_params = [1.2, 0.1]  # Home advantage, rho
        initial_params.extend([1.0] * len(team_list))  # Attack
        initial_params.extend([1.0] * len(team_list))  # Defense
        
        # Define bounds for parameters
        bounds = [(0.5, 2.0), (-0.3, 0.3)]  # Home advantage, rho
        bounds.extend([(0.1, 3.0)] * len(team_list))  # Attack
        bounds.extend([(0.1, 3.0)] * len(team_list))  # Defense
        
        # Minimize negative log-likelihood
        result = minimize(
            lambda params: PSxGShotsTeamModel.dc_log_likelihood(
                params, matches, team_list, metadata, 
                epsilon=epsilon, season_penalty=season_penalty
            ),
            initial_params,
            method='L-BFGS-B',
            bounds=bounds
        )
        
        # Print optimization results
        print(f"Optimization success: {result.success}")
        print(f"Final function value: {result.fun}")
        print(f"Number of iterations: {result.nit}")

        return result.x
    
    def print_team_strengths(self, exclude_teams=None):
        """Print team strength analysis in a formatted table with average opponent metrics."""
        if exclude_teams is None:
            exclude_teams = []

        # Get all teams from both models
        all_teams = set(self.team_attack.keys())
        all_teams = [team for team in all_teams if team not in exclude_teams]
        
        # Calculate the average attack and defense values across all teams
        total_attack = sum(self.team_attack.get(team, 0) for team in all_teams)
        total_defense = sum(self.team_defense.get(team, 0) for team in all_teams)
        num_teams = len(all_teams)
        
        avg_attack = total_attack / num_teams if num_teams > 0 else 1.0
        avg_defense = total_defense / num_teams if num_teams > 0 else 1.0
        
        print(f"League average attack: {avg_attack:.3f}")
        print(f"League average defense: {avg_defense:.3f}")
        
        # Create a list of team data
        team_data = []
        for team in all_teams:
            std_attack = self.team_attack.get(team, float('nan'))
            std_defense = self.team_defense.get(team, float('nan'))
            overall_log_strength = np.log(std_attack) - np.log(std_defense)
            overall_abs_strength = std_attack - std_defense
            
            # Calculate expected goals against average opponent (ignoring home/away)
            xg_vs_avg = std_attack * avg_defense  # Expected goals for vs average defense
            xga_vs_avg = avg_attack * std_defense  # Expected goals against vs average attack
            
            team_data.append({
                'team': team,
                'std_attack': std_attack,
                'std_defense': std_defense,
                'overall_log_strength': overall_log_strength,
                'overall_abs_strength': overall_abs_strength,
                'xg_vs_avg': xg_vs_avg,
                'xga_vs_avg': xga_vs_avg,
                'goal_diff_vs_avg': xg_vs_avg - xga_vs_avg
            })
        
        # Sort by overall strength (descending)
        team_data = sorted(team_data, key=lambda x: x['goal_diff_vs_avg'], reverse=True)
        
        # Print header
        print("\n{:<20} {:^20} {:^20} {:^30}".format('', 'Team Metrics', 'Strength', 'Expected vs Average'))
        print("{:<20} {:^10} {:^10} {:^10} {:^10} {:^10} {:^10} {:^10}".format(
            'Team', 'Attack', 'Defence', 'Log', 'Abs', 'For', 'Against', 'Diff'))
        print("-" * 100)
        
        # Print team data
        for team in team_data:
            print("{:<20} {:^10.2f} {:^10.2f} {:^10.2f} {:^10.2f} {:^10.2f} {:^10.2f} {:^10.2f}".format(
                team['team'],
                team['std_attack'],
                team['std_defense'],
                team['overall_log_strength'],
                team['overall_abs_strength'],
                team['xg_vs_avg'],
                team['xga_vs_avg'],
                team['goal_diff_vs_avg']
            ))
        
        # Print model parameters
        print("\nModel Parameters:")
        print(f"Home Advantage: {self.home_advantage:.3f}")
        print(f"Rho Parameter: {self.rho:.3f}")
        print(f"Simulations per match: {self.n_simulations}")
        
        return team_data
    
    def predict_match(self, home_team, away_team, max_goals=10):
        # Check if teams exist in the model
        if home_team not in self.team_attack or away_team not in self.team_attack:
            raise ValueError(f"Teams not found in the model. Available teams: {sorted(self.team_attack.keys())}")
        
        # Calculate expected goals
        lambda_home = self.team_attack[home_team] * self.team_defense[away_team] * self.home_advantage
        lambda_away = self.team_attack[away_team] * self.team_defense[home_team]
        
        return {"home_team": home_team, "away_team": away_team, "home_goals": lambda_home, "away_goals": lambda_away}