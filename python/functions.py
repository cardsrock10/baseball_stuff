import pandas as pd
import sqlite3

def get_savant_data(year):
    savant = sqlite3.connect('BaseballSavant.db')
    cur = savant.cursor()
    cur.execute('SELECT * FROM statcast WHERE game_date >= "%s-01-01" AND game_date <= "%s-12-31"' % (str(year), str(year)))
    #cur.execute('SELECT * FROM statcast WHERE game_date >= "%s-01-01" AND game_date <= "%s-12-31" AND des != "null"' % (str(year), str(year)))
    data = pd.DataFrame(cur.fetchall())
    data.columns = [i[0] for i in cur.description]
    #data = data[data.launch_speed != 'null']
    #data = data[data.events != 'null']
    return data
    
def get_bref_data(year):
    tot_batting = pd.read_csv('Batting.csv')
    year_batting = tot_batting[tot_batting.yearID == year]
    year_batting.rename(columns={'2B':'D'}, inplace=True)
    year_batting.rename(columns={'3B':'TR'}, inplace=True)
    return year_batting
    
def get_player_data():
    return pd.read_csv('Master.csv')
    
def get_weight_data(year):
    weights = pd.read_csv('FanGraphs Leaderboard.csv')
    return weights.set_index('Season').loc[year, :]
    
def set_PA(batting):
    batting['PA'] = batting.AB + batting.BB + batting.SF + batting.HBP
    
def set_1B(batting):
    batting['S'] = batting.H - batting.HR - batting.TR - batting.D
    
def set_wOBA(batting, weights):
    #rint weights.wBB 
    batting['wOBA'] = ((weights.wBB*(batting.BB-batting.IBB)) + (weights.wHBP*batting.HBP) + (weights.w1B*batting.S) + (weights.w2B*batting.D) + (weights.w3B*batting.TR) + (weights.wHR*batting.HR)) / (batting.AB+batting.BB-batting.IBB+batting.SF+batting.HBP)
    #batting['wOBA'] = ((weights.wBB*(batting.BB-batting.IBB)) )
    
def set_up_batting(batting, weights, pa_limit):
    batting = batting.groupby(['playerID', 'yearID']).sum().reset_index()
    set_PA(batting)
    batting = batting[batting.PA >= pa_limit]
    set_1B(batting)
    set_wOBA(batting, weights)
    return batting
    
def set_barrel(savant):
    savant = savant[savant.launch_speed != 'null']
    savant = savant[savant.launch_angle != 'null']
    ls = pd.to_numeric(savant.launch_speed)
    la = pd.to_numeric(savant.launch_angle)
    savant.launch_speed = ls
    savant.launch_angle = la
    savant['barrel'] = ((savant['launch_speed'] * 1.5 - savant['launch_angle']) >= 117) & ((savant['launch_speed'] + savant['launch_angle']) >= 124) & (savant['launch_speed'] >= 98) & (savant['launch_angle'] >= 4) & (savant['launch_angle'] <= 50)
    savant['super_barrel'] = ((savant['launch_speed'] * 1.5 - savant['launch_angle']) >= 129) & ((savant['launch_speed'] + savant['launch_angle'] * 2) >= 156) & (savant['launch_speed'] >=106) & (savant['launch_angle'] >= 4) & (savant['launch_angle'] <= 48)
    return savant
        
def set_up_savant(savant):
    savant = set_barrel(savant)
    
    savant['barrel_minor_and_HR'] = (savant.events == 'home_run') & (savant.barrel) & (~savant.super_barrel)
    savant['barrel_major_and_HR'] = (savant.events == 'home_run') & (savant.barrel) & (savant.super_barrel)
    savant['barrel_major_and_notHR'] = (savant.events != 'home_run') & (savant.barrel) & (savant.super_barrel)
    savant['barrel_minor_and_notHR'] = (savant.events != 'home_run') & (savant.barrel) & (~savant.super_barrel)
    savant['HR_not_barrel'] = (savant.events == 'home_run') & (~savant.barrel)
    return savant
    
def get_all_counts(savant, batting):
    batting = add_count(savant[savant.barrel].batter.value_counts(), batting, 'barrel')
    batting = add_count(savant[savant.super_barrel].batter.value_counts(), batting, 'super_barrel')
    batting = add_count(savant[savant.barrel_minor_and_HR].batter.value_counts(), batting, 'barrel_minor_and_HR')
    batting = add_count(savant[savant.barrel_major_and_HR].batter.value_counts(), batting, 'barrel_major_and_HR')
    batting = add_count(savant[savant.barrel_major_and_notHR].batter.value_counts(), batting, 'barrel_major_and_notHR')
    batting = add_count(savant[savant.barrel_minor_and_notHR].batter.value_counts(), batting, 'barrel_minor_and_notHR')
    batting = add_count(savant[savant.HR_not_barrel].batter.value_counts(), batting, 'HR_not_barrel')
    return batting.fillna(0)
    
def get_all_rates(batting):
    batting['barrel'] = batting.barrel 
    batting['super_barrel'] = batting.super_barrel 
    batting['barrel_minor_and_HR'] = batting.barrel_minor_and_HR 
    batting['barrel_major_and_HR'] = batting.barrel_major_and_HR 
    batting['barrel_major_and_notHR'] = batting.barrel_major_and_notHR
    batting['barrel_minor_and_notHR'] = batting.barrel_minor_and_notHR
    batting['HR_not_barrel'] = batting.HR_not_barrel 
    batting['test'] = (batting.SO - batting.BB) / batting.PA
    #batting['barrel'] = (batting.barrel / batting.H) * 100
    #batting['super_barrel'] = batting.super_barrel / batting.H
    #batting['barrel_minor_and_HR'] = batting.barrel_minor_and_HR / batting.H
    #batting['barrel_major_and_HR'] = batting.barrel_major_and_HR / batting.H
    #batting['barrel_major_and_notHR'] = batting.barrel_major_and_notHR / batting.H
    #batting['barrel_minor_and_notHR'] = batting.barrel_minor_and_notHR / batting.H
    #batting['HR_not_barrel'] = batting.HR_not_barrel / batting.H
    #batting['test'] = (batting.SO - batting.BB) / batting.PA
    return batting
    
def add_count(count, batting, key):
    count = count.to_frame(key)
    count['mlb_id'] = count.index
    return pd.merge(batting, count, how='left')
    
def merge_playerID(players, batting):
    players = players[['mlb_id', 'bref_id', 'bref_name']]
    return pd.merge(players, batting, left_on='bref_id', right_on='playerID')
    
def year_data(year, pa_limit):
    year_savant = get_savant_data(year)
    year_savant = set_up_savant(year_savant)
    
    year_batting = get_bref_data(year)
    year_weights = get_weight_data(year)
    year_batting = set_up_batting(year_batting, year_weights, pa_limit)
    
    players = get_player_data()
    year_batting = merge_playerID(players, year_batting)
    
    year_batting = get_all_counts(year_savant, year_batting)
    year_batting = get_all_rates(year_batting)
           
    return year_batting

if __name__ == "__main__":
    fifteen_batting = year_data(2015, 100)
    sixteen_batting = year_data(2016, 100)
    seventeen_batting = year_data(2017, 100)

    
    test_players = pd.DataFrame({'playerID':list(set(fifteen_batting.playerID) & set(sixteen_batting.playerID) & set(seventeen_batting.playerID))})
    fifteen_batting = pd.merge(test_players, fifteen_batting)
    sixteen_batting = pd.merge(test_players, sixteen_batting)
    seventeen_batting = pd.merge(test_players, seventeen_batting)
    
    wOBA = pd.DataFrame({'playerID': fifteen_batting.playerID, 'wOBA_2015': fifteen_batting.wOBA, 'wOBA_2016': sixteen_batting.wOBA, 'wOBA_2017': seventeen_batting.wOBA})
    fifteen_batting = pd.merge(fifteen_batting, wOBA)
    sixteen_batting = pd.merge(fifteen_batting, wOBA)
    seventeen_batting = pd.merge(seventeen_batting, wOBA)


    