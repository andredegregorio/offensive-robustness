from pybaseball import statcast, cache
import pandas as pd

cache.enable()

years = [2021, 2022, 2023, 2024, 2025]
dates = {
    2021: ('2021-01-01', '2021-12-31'),
    2022: ('2022-01-01', '2022-12-31'),
    2023: ('2023-01-01', '2023-12-31'),
    2024: ('2024-01-01', '2024-12-31'),
    2025: ('2025-01-01', '2025-12-31')
}

for year in years:
    print(f"Pulling {year}...")
    df = statcast(start_dt=dates[year][0], end_dt=dates[year][1])
    # Filter to regular season only
    df = df[df['game_type'] == 'R']
    df.to_parquet(f'statcast_{year}_raw.parquet')
    print(f"{year}: {len(df):,} pitches\n")

# Combine training years
dfs = [pd.read_parquet(f'statcast_{y}_raw.parquet') for y in [2021, 2022, 2023, 2024]]
df_train = pd.concat(dfs, ignore_index=True)
df_train.to_parquet('statcast_2021_2024_train.parquet')
print(f"Training: {len(df_train):,} pitches")