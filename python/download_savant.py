import pandas as pd
import sqlite3
from tqdm import tqdm
import time
import urllib3.exceptions

savant = sqlite3.connect('BaseballSavant.db')

teams = ['LAA', 'HOU', 'OAK', 'TOR', 'ATL', 'MIL', 'STL',
         'CHC', 'ARI', 'LAD', 'SF', 'CLE', 'SEA', 'MIA',
         'NYM', 'WSH', 'BAL', 'SD', 'PHI', 'PIT', 'TEX',
         'TB', 'BOS', 'CIN', 'COL', 'KC', 'DET', 'MIN',
         'CWS', 'NYY']

for year in tqdm(range(2008, 2018), desc = 'Years'):
    for team in tqdm(teams, desc = 'Teams', leave = False):
        # Query link is based on loop
        link = 'https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=&hfC=&hfSea=' + str(year) + '%7C&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&team=' + team + '&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name-event&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&min_abs=0&type=details&'
        successful = False
        backoff_time = 30
        while not successful:
            try:
                data = pd.read_csv(link, low_memory=False)
                # Rename player_name to denote that it is the pitcher
                data.rename(columns={'player_name': 'pitcher_name'}, inplace=True)
                # Insert to table
                pd.io.sql.to_sql(data, name='statcast', con=savant, if_exists='append')
                successful = True
            except (urllib3.exceptions.HTTPError, sqlite3.OperationalError) as e:
                # If there is an error backoff exponentially until there is no longer an error
                for i in tqdm(range(1, backoff_time), desc="Backing off " + str(backoff_time) + " seconds", leave=False):
                    time.sleep(1)
                backoff_time = min(backoff_time * 2, 60*60)

savant.commit()
savant.execute("DELETE FROM statcast WHERE rowid NOT IN (SELECT MIN(rowid) FROM statcast GROUP BY sv_id, batter, pitcher)")
savant.commit()
savant.close()