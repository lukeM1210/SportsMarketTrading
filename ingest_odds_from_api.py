import os
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
import requests

from snowflake_session import get_session

#----CONFIG-----#
API_KEY = os.environ.get("ODDS_API_KEY", "037e3751a9631be4140099754f117cc4")
API_URL = os.environ.get("ODDS_API_URL", "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds/?apiKey={API_KEY}&regions=us&markets=h2h,spreads,totals&oddsFormat=american")
#----------------

def fetch_odds_from_api() -> List[Dict[str,Any]]:
  """
  Call your odds API and return JSON data.
  Replace params with whatever your provider requires
  """
  headers = {"Accept", "application/json"}
  response = requests.get(API_URL, headers=headers)
  response.raise_for_status()
  return response.json()

def flatten_odds(json_data: List[Dict[str,Any]]) -> pd.DataFrame:
  """
  Turn the nested odds JSON into a flat DataFrame
  that matches the DEMO.NFL_ODDS tables.
  """
  odds_rows = []
  events_rows = {}
  bookmakers_rows = {}

  for event in json_data:
    event_id = event["id"]
    sport_key = event["sport_key"]
    sport_title = event.get("sport_title")
    commence_time = event.get("commence_time")
    home_team = event.get("home_team")
    away_team = event.get("away_team")

    # unique event row
    events_rows[event_id] = {
      "event_id": event_id,
      "sport_key": sport_key,
      "sport_title": sport_title,
      "commence_time_utc": commence_time,
      "home_team": home_team,
      "away_team": away_team,
    }

    for book in event.get("bookmakers", []):
      bookmaker_key = book["key"]
      bookmaker_title = book.get("title")
      bookmaker_last_update = book.get("last_update")

      # unique bookmaker row
      bookmakers_rows[bookmaker_key] = {
        "bookmaker_key": bookmaker_key,
        "bookmaker_title": bookmaker_title,
      }

      for market in book.get("markets", []):
        market_key = market["key"] # h2h, spreads, totals
        market_last_update = market.get("last_update", bookmaker_last_update);

        for outcome in market.get("outcomes", []):
          outcome_name = outcome.get("name")
          price = outcome.get("price")
          point = outcome.get("point") # may be none for h2h

          is_home_team = None
          if outcome_name and home_team:
            if outcome_name == home_team:
              is_home_team = True
            elif outcome_name == away_team:
              is_home_team = False

          odds_rows.append(
            {
              "event_id": event_id,
              "bookmaker_key": bookmaker_key,
              "market_key": market_key,
              "outcome_name": outcome_name,
              "is_home_team": is_home_team,
              "price_american": price,
              "line_point": point,
              "event_commence_utc": commence_time,
              "market_last_update": market_last_update,
            }
          )
  
  events_df = pd.DataFrame.from_dict(events_rows, orient="index")
  bookmakers_df = pd.DataFrame.from_dict(bookmakers_rows, orient="index")
  odds_df = pd.DataFrame(odds_rows)

  # Convert time strings to datetime if present
  for col in ["commence_time_utc", "event_commence_utc", "market_last_update"]:
    if col in events_df.columns:
      events_df[col] = pd.to_datetime(events_df[col], errors="coerce", utc=True)
    if col in odds_df.columnsz:
      odds_df[col] = pd.to_datetime(odds_df[col], errors="coerce", utc=True)

  return events_df, bookmakers_df, odds_df



def upsert_dimension_table(session, df: pd.DataFrame, table_name: str, key_column: str):
    """
    Simple upsert: insert new keys, ignore duplicates.
    (For a real system you might do MERGE; this keeps it simple.)
    """
    if df.empty:
        return

    temp_table = f"TEMP_{table_name}_{int(datetime.utcnow().timestamp())}"

    # Write to a temporary table
    session.write_pandas(df, temp_table, auto_create_table=True, overwrite=True)

    # Insert rows whose key doesn't already exist
    session.sql(f"""
        INSERT INTO {table_name}
        SELECT t.*
        FROM {temp_table} t
        LEFT JOIN {table_name} d
          ON t.{key_column} = d.{key_column}
        WHERE d.{key_column} IS NULL
    """).collect()

    session.sql(f"DROP TABLE {temp_table}").collect()


def load_into_snowflake(json_data: List[Dict[str, Any]]):
    session = get_session()

    events_df, bookmakers_df, odds_df = flatten_odds(json_data)

    # 1) Upsert events & bookmakers (dimensions)
    upsert_dimension_table(session, events_df, "EVENTS", "EVENT_ID")
    upsert_dimension_table(session, bookmakers_df, "BOOKMAKERS", "BOOKMAKER_KEY")

    # 2) Append into odds_snapshots (fact)
    if not odds_df.empty:
        session.write_pandas(
            odds_df,
            table_name="ODDS_SNAPSHOTS",
            database="DEMO",
            schema="NFL_ODDS",
            overwrite=False
        )

    print(f"Loaded {len(events_df)} events, {len(bookmakers_df)} bookmakers, {len(odds_df)} odds rows.")


if __name__ == "__main__":
    # Option A: call the real API
    # data = fetch_odds_from_api()

    # Option B: paste your JSON into a variable for testing
    import json
    from pathlib import Path

    # For quick testing: put your big JSON into data/sample_odds.json
    sample_path = Path(__file__).parent.parent / "data" / "sample_odds.json"
    if sample_path.exists():
        with open(sample_path, "r") as f:
            data = json.load(f)
    else:
        raise SystemExit("Put your JSON into data/sample_odds.json before running.")

    load_into_snowflake(data)