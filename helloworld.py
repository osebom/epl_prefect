from prefect import flow
import requests
import pandas as pd
import datetime

@flow(log_prints=True)
def epl_flow():

    # Get the current year using datetime
    current_year = datetime.datetime.now().year

    # Fetch team information from the bootstrap-static endpoint
    def fetch_teams():
        url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
        response = requests.get(url)
        data = response.json()
        teams = {team['id']: team['name'] for team in data['teams']}
        return teams

    # Fetch fixtures data
    def fetch_fixtures():
        url = 'https://fantasy.premierleague.com/api/fixtures/'
        response = requests.get(url)
        fixtures = response.json()
        return fixtures

    # Main data processing function
    def process_fixtures():
        teams = fetch_teams()
        fixtures = fetch_fixtures()
        data = []

        for fixture in fixtures:
            if 'kickoff_time' in fixture and fixture['team_h_score'] is not None and fixture['team_a_score'] is not None:
                game = {
                    'Season_End_Year': current_year if pd.to_datetime(fixture['kickoff_time']).year >= current_year else pd.to_datetime(fixture['kickoff_time']).year + 1,
                    'Wk': fixture.get('event'),
                    'Date': pd.to_datetime(fixture['kickoff_time']).date(),
                    'Home': teams.get(fixture['team_h'], 'Unknown'),
                    'HomeGoals': fixture['team_h_score'],
                    'AwayGoals': fixture['team_a_score'],
                    'Away': teams.get(fixture['team_a'], 'Unknown'),
                    'FTR': 'H' if fixture['team_h_score'] > fixture['team_a_score'] else ('A' if fixture['team_h_score'] < fixture['team_a_score'] else 'D')
                }
                data.append(game)

        return pd.DataFrame(data)

    # Execute the processing function and print the DataFrame
    df = process_fixtures()

    # Define a dictionary with the names you want to replace
    replacement_dict = {
        'Man City': 'Manchester City',
        'Man Utd': 'Manchester Utd',
        'Newcastle': 'Newcastle Utd',
        "Nott'm Forest": "Nott'ham Forest",
        'Spurs': 'Tottenham'
    }

    # Replace the names in the "Home" and "Away" columns
    df['Home'] = df['Home'].replace(replacement_dict, regex=True)
    df['Away'] = df['Away'].replace(replacement_dict, regex=True)

    df = df[df.Home == 'Chelsea']
    df['Date'] = df['Date'].astype(str)

    df.to_gbq(destination_table='eplanalysis.epl_data',
                   project_id='eplanalysis',
                   if_exists='append')


if __name__ == "__main__":
    epl_flow.serve(name="first_epl",
                      tags=["epl_testing"],
                      interval=60)