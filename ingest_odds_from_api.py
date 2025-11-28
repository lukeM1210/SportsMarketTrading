import os
from datetime import datetime, UTC
from typing import List, Dict, Any, Tuple

import pandas as pd
import requests

# ---- CONFIG ----
API_KEY = os.environ.get("API_KEY")
API_URL = f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds/?apiKey={API_KEY}&regions=us&markets=h2h,spreads,totals&oddsFormat=american"
# ----------------

def fetch_odds_from_api() -> List[Dict[str, Any]]:
    """
    Call the odds API and return the JSON list.
    """
    #headers = {"Accept": "application/json"}
    
    print(f"[{datetime.now(UTC)}] Fetching odds from API...")
    resp = requests.get(API_URL)
    resp.raise_for_status()
    data = resp.json()
    print(f"Got {len(data)} events from API.")
    return data


def flatten_odds(json_data: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Turn the nested odds JSON into three flat DataFrames:
      - events_df
      - bookmakers_df
      - odds_df
    """
    odds_rows: list[dict[str, Any]] = []
    events_rows: dict[str, dict[str, Any]] = {}
    bookmakers_rows: dict[str, dict[str, Any]] = {}

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
                market_key = market["key"]  # h2h, spreads, totals
                market_last_update = market.get("last_update", bookmaker_last_update)

                for outcome in market.get("outcomes", []):
                    outcome_name = outcome.get("name")
                    price = outcome.get("price")
                    point = outcome.get("point")  # may be None for h2h

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
        if col in odds_df.columns:
            odds_df[col] = pd.to_datetime(odds_df[col], errors="coerce", utc=True)

    return events_df, bookmakers_df, odds_df


def main():
    # Option A: call the real API
    try:
        data = fetch_odds_from_api()
    except Exception as e:
        print(f"API call failed: {e}")
        print("Falling back to local sample_odds.json (if present)...")
        from pathlib import Path
        import json

        sample_path = Path(__file__).parent.parent / "data" / "sample_odds.json"
        if sample_path.exists():
            with open(sample_path, "r") as f:
                data = json.load(f)
        else:
            print("No local sample_odds.json found. Exiting.")
            return

    events_df, bookmakers_df, odds_df = flatten_odds(data)

    print("\n=== events_df (first 5 rows) ===")
    print(events_df.head())

    print("\n=== bookmakers_df (first 5 rows) ===")
    print(bookmakers_df.head())

    print("\n=== odds_df (first 5 rows) ===")
    print(odds_df.head())

    # Optional: write to CSVs
    out_dir = "output"
    os.makedirs(out_dir, exist_ok=True)
    events_df.to_csv(os.path.join(out_dir, "events.csv"), index=False)
    bookmakers_df.to_csv(os.path.join(out_dir, "bookmakers.csv"), index=False)
    odds_df.to_csv(os.path.join(out_dir, "odds.csv"), index=False)

    print(f"\nSaved CSVs to ./{out_dir}/")


if __name__ == "__main__":
    main()
