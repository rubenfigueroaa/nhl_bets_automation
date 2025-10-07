import requests
import xml.etree.ElementTree as ET
import time
from math import exp
import sqlite3

# API keys
SPORTRADAR_API_KEY = "GQppjVSPg23RiFYOHcGXYH4dMZZgfMV2VCitzZa1"
ODDS_API_KEY = "e7bf242b0d90e8a04d095c4dec52714c"

# Headers
HEADERS = {
    "accept": "application/json",
    "x-api-key": SPORTRADAR_API_KEY
}

# Namespaces
schedule_ns = {'sr': 'http://feed.elasticstats.com/schema/hockey/schedule-v7.0.xsd'}
analytics_ns = {'ns': 'http://feed.elasticstats.com/schema/hockey/analytics-v6.0.xsd'}

def normalize(name):
    return name.lower().replace(" ", "").replace("-", "")

def softmax(a, b):
    ea = exp(a)
    eb = exp(b)
    total = ea + eb
    return ea / total * 100, eb / total * 100

def implied_probability(odds):
    if odds > 0:
        return 100 / (odds + 100) * 100
    else:
        return abs(odds) / (abs(odds) + 100) * 100

# Classification function
def classify_bet(win_pct, opp_win_pct, value):
    if value is None or value <= 0:
        return "No Bet 🚫"
    elif win_pct > opp_win_pct:
        return "Strong Bet ✅"
    else:
        return "Smart Bet 💡"

# ===== SQLite setup with schema migration =====
DB_PATH = "nhl_bets.db"

def ensure_schema(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_date TEXT,
        home_team TEXT,
        away_team TEXT,
        source TEXT,
        home_win_pct REAL,
        away_win_pct REAL,
        home_odds REAL,
        away_odds REAL,
        home_implied_pct REAL,
        away_implied_pct REAL,
        home_value REAL,
        away_value REAL,
        home_classification TEXT,
        away_classification TEXT,
        outcome TEXT
    )
    """)
    conn.commit()

    # Verify columns exist; add any missing ones
    cursor.execute("PRAGMA table_info(bets)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    expected = {
        "game_date": "TEXT",
        "home_team": "TEXT",
        "away_team": "TEXT",
        "source": "TEXT",
        "home_win_pct": "REAL",
        "away_win_pct": "REAL",
        "home_odds": "REAL",
        "away_odds": "REAL",
        "home_implied_pct": "REAL",
        "away_implied_pct": "REAL",
        "home_value": "REAL",
        "away_value": "REAL",
        "home_classification": "TEXT",
        "away_classification": "TEXT",
        "outcome": "TEXT",
    }
    for col_name, col_type in expected.items():
        if col_name not in existing_cols:
            cursor.execute(f"ALTER TABLE bets ADD COLUMN {col_name} {col_type}")
            print(f"🔧 Added missing column: {col_name} ({col_type})")
    conn.commit()

conn = sqlite3.connect(DB_PATH)
ensure_schema(conn)
cursor = conn.cursor()

# ===== Odds API fetch (Bet365 & FanDuel) =====
odds_url = f"https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds?regions=us,eu&markets=h2h&oddsFormat=american&dateFormat=iso&apiKey={ODDS_API_KEY}"
try:
    odds_response = requests.get(odds_url, timeout=20)
    odds_data = odds_response.json()
except Exception as e:
    print("Failed to parse Odds API response:", e)
    odds_data = []

# Build odds lookup using normalized home/away team names
bookmaker_odds = {}
for game in odds_data:
    if isinstance(game, dict) and "home_team" in game and "away_team" in game and "bookmakers" in game:
        home = normalize(game["home_team"])
        away = normalize(game["away_team"])
        matchup_key = tuple(sorted([home, away]))

        for bookmaker in game.get("bookmakers", []):
            title = bookmaker["title"].lower()
            if title in ["bet365", "fanduel"]:
                for market in bookmaker.get("markets", []):
                    if market.get("key") == "h2h":
                        outcomes = market.get("outcomes", [])
                        if len(outcomes) == 2:
                            odds_map = {normalize(o["name"]): o.get("price") for o in outcomes if "name" in o}
                            if len(odds_map) == 2 and all(v is not None for v in odds_map.values()):
                                bookmaker_odds.setdefault(matchup_key, {})[title] = odds_map

# ===== Schedule fetch =====
schedule_url = "https://api.sportradar.com/nhl/trial/v7/en/games/2025/10/07/schedule.xml"
schedule_response = requests.get(schedule_url, headers=HEADERS, timeout=20)
schedule_root = ET.fromstring(schedule_response.text)

# Derive game_date from URL path
parts = schedule_url.split("/")
year, month, day = parts[-4], parts[-3], parts[-2]
game_date = f"{year}-{month}-{day}"

games = schedule_root.findall(".//sr:game", schedule_ns)
print(f"Games found: {len(games)}")

# ===== Process each game =====
for game in games:
    home = game.find("sr:home", schedule_ns)
    away = game.find("sr:away", schedule_ns)
    if home is None or away is None:
        continue

    home_id = home.get("id")
    away_id = away.get("id")
    home_name = home.get("name")
    away_name = away.get("name")

    print(f"\n🏒 Game: {home_name} vs {away_name}")

    team_stats = {}

    for team_id, team_label, role in [
        (home_id, home_name, "Home"),
        (away_id, away_name, "Away")
    ]:
        analytics_url = f"https://api.sportradar.com/nhl/trial/v7/en/seasons/2024/REG/teams/{team_id}/analytics.xml"
        print(f"\nFetching analytics for {role} Team: {team_label} (ID: {team_id})")
        analytics_response = requests.get(analytics_url, headers=HEADERS, timeout=20)

        if analytics_response.status_code == 429:
            print("Rate limit hit. Retrying after delay...")
            time.sleep(3)
            analytics_response = requests.get(analytics_url, headers=HEADERS, timeout=20)

        print(f"Status code: {analytics_response.status_code}")
        if analytics_response.status_code != 200:
            print("Failed to fetch analytics.")
            continue

        root = ET.fromstring(analytics_response.text)
        team = root.find('.//ns:team', analytics_ns)
        if team is None:
            print("No team data found.")
            continue

        team_records = team.find('.//ns:team_records', analytics_ns)
        overall = team_records.find('.//ns:overall', analytics_ns) if team_records is not None else None
        statistics = overall.find('.//ns:statistics', analytics_ns) if overall is not None else None

        print(f"{role} Team: {team_label}")
        if statistics is not None:
            total = statistics.find('.//ns:total', analytics_ns)
            if total is not None:
                corsi_pct = float(total.get('corsi_pct', 0))
                fenwick_pct = float(total.get('fenwick_pct', 0))
                shots_diff = float(total.get('on_ice_shots_differential', 0))
                pdo = float(total.get('pdo', 0))

                print(f"Corsi Percentage: {corsi_pct}")
                print(f"Fenwick Percentage: {fenwick_pct}")
                print(f"On Ice Shots Differential: {shots_diff}")
                print(f"PDO: {pdo}")

                team_stats[role] = {
                    "name": team_label,
                    "corsi_pct": corsi_pct,
                    "fenwick_pct": fenwick_pct,
                    "shots_diff": shots_diff,
                    "pdo": pdo
                }
        time.sleep(1.5)

    # Predict winner and compare to odds
    if "Home" in team_stats and "Away" in team_stats:
        def calculate_score(stats, is_home):
            return (
                2 * stats["corsi_pct"] +
                2 * stats["fenwick_pct"] +
                1 * (stats["shots_diff"] / 100) +
                1 * (stats["pdo"] / 10) +
                (0.5 if is_home else 0)
            )

        home_score = calculate_score(team_stats["Home"], True)
        away_score = calculate_score(team_stats["Away"], False)
        home_pct, away_pct = softmax(home_score, away_score)

        print(f"\n🏁 Prediction:")
        print(f"{team_stats['Home']['name']} → Win Probability: {home_pct:.1f}%")
        print(f"{team_stats['Away']['name']} → Win Probability: {away_pct:.1f}%")

        # Compare to Bet365 or FanDuel odds
        matchup_key = tuple(sorted([normalize(home_name), normalize(away_name)]))
        odds_entry = bookmaker_odds.get(matchup_key)

        if odds_entry:
            for source in ["bet365", "fanduel"]:
                if source in odds_entry:
                    odds = odds_entry[source]
                    home_key = normalize(home_name)
                    away_key = normalize(away_name)
                    home_odds = odds.get(home_key)
                    away_odds = odds.get(away_key)

                    if home_odds is not None and away_odds is not None:
                        home_implied = implied_probability(home_odds)
                        away_implied = implied_probability(away_odds)

                        print(f"\n💰 {source.title()} Odds:")
                        print(f"{home_name}: {home_odds} → Implied Probability: {home_implied:.1f}%")
                        print(f"{away_name}: {away_odds} → Implied Probability: {away_implied:.1f}%")

                        home_value = home_pct - home_implied
                        away_value = away_pct - away_implied

                        print(f"\n📈 Betting Value:")
                        print(f"{home_name}: {home_value:+.1f}% {'✅' if home_value > 0 else '❌'}")
                        print(f"{away_name}: {away_value:+.1f}% {'✅' if away_value > 0 else '❌'}")

                        # Classification
                        home_class = classify_bet(home_pct, away_pct, home_value)
                        away_class = classify_bet(away_pct, home_pct, away_value)

                        print(f"\n🧠 Bet Classification:")
                        print(f"{home_name}: {home_class}")
                        print(f"{away_name}: {away_class}")

                        # Insert results into SQLite
                        try:
                            cursor.execute("""
                            INSERT INTO bets (
                                game_date, home_team, away_team, source,
                                home_win_pct, away_win_pct,
                                home_odds, away_odds,
                                home_implied_pct, away_implied_pct,
                                home_value, away_value,
                                home_classification, away_classification,
                                outcome
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                game_date,
                                home_name, away_name, source,
                                home_pct, away_pct,
                                home_odds, away_odds,
                                home_implied, away_implied,
                                home_value, away_value,
                                home_class, away_class,
                                None  # to be filled after the game ends
                            ))
                            conn.commit()
                            print("✅ Inserted into database.")
                        except Exception as db_err:
                            print("❌ Database insert error:", db_err)
                    else:
                        print("⚠️ Skipping insert because odds are missing for one side.")
        else:
            print("No Bet365 or FanDuel odds available for this matchup.")

# Close SQLite connection
conn.close()
print("\n📦 Database closed:", DB_PATH)