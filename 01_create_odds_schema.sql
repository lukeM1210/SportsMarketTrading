-- Use your database
USE ROLE SYSADMIN;
USE DATABASE demo;

-- Schema for your odds data
CREATE SCHEMA IF NOT EXISTS nfl_odds;

USE SCHEMA nfl_odds;

-- One row per game/event
CREATE OR REPLACE TABLE events (
    event_id           STRING      NOT NULL,  -- "id" in JSON
    sport_key          STRING      NOT NULL,  -- "americanfootball_nfl"
    sport_title        STRING,
    commence_time_utc  TIMESTAMP_NTZ,
    home_team          STRING,
    away_team          STRING,
    created_at_utc     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

-- One row per bookmaker (dimension)
CREATE OR REPLACE TABLE bookmakers (
    bookmaker_key   STRING      NOT NULL,  -- "draftkings"
    bookmaker_title STRING,
    created_at_utc  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (bookmaker_key)
);

-- Core odds table (normalized & flexible)
-- One row per (event, bookmaker, market, outcome, snapshot)
CREATE OR REPLACE TABLE odds_snapshots (
    event_id            STRING      NOT NULL,   -- FK to events
    bookmaker_key       STRING      NOT NULL,   -- FK to bookmakers
    market_key          STRING      NOT NULL,   -- "h2h", "spreads", "totals"
    outcome_name        STRING      NOT NULL,   -- "Chicago Bears", "Over", etc.
    is_home_team        BOOLEAN,               -- true/false if matches home_team
    price_american      NUMBER(10,0) NOT NULL, -- e.g. 270, -340
    line_point          FLOAT,                 -- spread or total line, nullable
    event_commence_utc  TIMESTAMP_NTZ,         -- denorm for easy querying
    market_last_update  TIMESTAMP_NTZ,
    ingest_ts_utc       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    -- You can add a surrogate key if you want:
    -- , odds_id NUMBER AUTOINCREMENT
);

-- Optional helper view: latest snapshot per event/book/market/outcome
CREATE OR REPLACE VIEW latest_odds AS
SELECT o.*
FROM odds_snapshots o
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id, bookmaker_key, market_key, outcome_name
    ORDER BY market_last_update DESC, ingest_ts_utc DESC
) = 1;
