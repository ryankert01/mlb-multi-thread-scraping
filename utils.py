def get_url(year: str, mode: str, page=1) -> str:
    base = "https://www.mlb.com/stats"
    if mode == "player_hitting":
        return f"{base}/{year}?page={page}&playerPool=ALL"
    elif mode == "player_pitching":
        return f"{base}/pitching/{year}?page={page}&playerPool=ALL&sortState=asc"
    elif mode == "team_hitting":
        return f"{base}/team/{year}"
    elif mode == "team_pitching":
        return f"{base}/team/pitching/{year}?sortState=asc"
    else:
        raise ValueError(f"Invalid mode: {mode}")

def aggregate_data(header1, data1, header2, data2, shift):
    assert len(data1) == len(data2), f"data1 and data2 must have the same length, got {len(data1)} and {len(data2)} respectively"
    j = 4 if shift < 2 else 3
    header, data = header1 + header2[j:], [data1[i] + data2[i][j:] for i in range(len(data1))]
    if shift == 0:
        assert len(header) == 35
    elif shift == 1:
        assert len(header) == 41
    elif shift == 2:
        assert len(header) == 30
    elif shift == 3:
        assert len(header) == 37
    return header, data