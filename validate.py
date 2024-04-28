import pandas as pd
from aggregate import aggregate

y_player_hitting = (1134, 1136, 1141, 1134, 1173, 1188, 1156, 1157, 1173, 1185, 1202, 1212, 1252, 1248, 1229, 1270, 1287, 620, 1374, 794, 769)
y_player_pitching = (612, 632, 606, 635, 666, 651, 664, 635, 662, 662, 679, 692, 735, 742, 755, 799, 831, 735, 909, 871, 863)
y_teams = ([30] * 21)

if __name__ == "__main__":
    assert len(y_player_hitting) == len(y_player_pitching) == len(y_teams), "Lengths of the arrays do not match"
    aggregate()
    player_hitting = pd.read_csv('player_hitting_combined.csv')
    player_pitching = pd.read_csv('player_pitching_combined.csv')
    team_hitting = pd.read_csv('team_hitting_combined.csv')
    team_pitching = pd.read_csv('team_pitching_combined.csv')
    # Verify data for each year
    years = [year for year in range(2003, 2024)]
    for year in years:
        assert len(player_hitting[player_hitting['year'] == year]) == y_player_hitting[year-2003], f"Data mismatch for year {year} in player_hitting"
        assert len(player_pitching[player_pitching['year'] == year]) == y_player_pitching[year-2003], f"Data mismatch for year {year} in player_pitching"
        assert len(team_hitting[team_hitting['year'] == year]) == y_teams[year-2003], f"Data mismatch for year {year} in team_hitting"
        assert len(team_pitching[team_pitching['year'] == year]) == y_teams[year-2003], f"Data mismatch for year {year} in team_pitching"
    print("All data validated successfully!")
