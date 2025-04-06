import json
import os
import time
import random
from pathlib import Path
import logging
from datetime import datetime, timedelta
import urllib.parse as urlparse
import psycopg2
import psycopg2.extras # For JSONB support

logger = logging.getLogger(__name__)

# --- Database Setup ---
DATABASE_URL = os.getenv('DATABASE_URL')
_db_conn = None # Simple connection caching

def get_db_connection():
    """Establishes or reuses a database connection."""
    global _db_conn
    if not DATABASE_URL:
        logger.critical("DATABASE_URL environment variable not set!")
        raise ConnectionError("Database URL not configured.")

    # Check if connection is alive, otherwise reconnect
    if _db_conn is None or _db_conn.closed != 0:
        try:
            logger.info("Attempting to connect to the database...")
            _db_conn = psycopg2.connect(DATABASE_URL, sslmode='require')
            psycopg2.extras.register_default_jsonb(conn_or_curs=_db_conn, globally=True) # Ensure JSONB is handled correctly
            logger.info("Database connection successful.")
        except psycopg2.DatabaseError as e:
            logger.critical(f"Database connection failed: {e}", exc_info=True)
            _db_conn = None # Reset on failure
            raise # Re-raise the exception
    return _db_conn

def initialize_database():
    """Creates the players table if it doesn't exist."""
    logger.info("Initializing database schema...")
    conn = get_db_connection()
    if not conn:
        logger.error("Cannot initialize database without a connection.")
        return

    create_players_sql = """
    CREATE TABLE IF NOT EXISTS players (
        user_id BIGINT PRIMARY KEY,
        display_name TEXT,
        franchise_name TEXT,
        cash NUMERIC(18, 4) DEFAULT 0.0,
        pizza_coins INTEGER DEFAULT 0,
        shops JSONB DEFAULT '{}'::jsonb,
        unlocked_achievements TEXT[] DEFAULT ARRAY[]::TEXT[],
        current_title TEXT,
        active_challenges JSONB DEFAULT '{}'::jsonb,
        challenge_progress JSONB DEFAULT '{}'::jsonb,
        stats JSONB DEFAULT '{}'::jsonb,
        total_income_earned NUMERIC(18, 4) DEFAULT 0.0,
        last_login_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        collection_count INTEGER DEFAULT 0,
        last_sabotage_attempt_time TIMESTAMP WITH TIME ZONE
    );
    """
    create_perf_sql = """
    CREATE TABLE IF NOT EXISTS location_performance (
        location_name TEXT PRIMARY KEY,
        current_multiplier NUMERIC(4, 2) DEFAULT 1.0,
        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """
    # Add index on lowercased display_name for case-insensitive search
    create_name_index_sql = """
    CREATE INDEX IF NOT EXISTS idx_players_display_name_lower
    ON players (LOWER(display_name));
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_players_sql)
            cur.execute(create_perf_sql)
            cur.execute(create_name_index_sql) # <<< Add index creation
        conn.commit()
        logger.info("Schema checked/created successfully (players, location_performance, indexes).") # Updated log
    except psycopg2.DatabaseError as e:
        logger.error(f"Error initializing database tables: {e}", exc_info=True)
        conn.rollback()

# --- Game Constants ---
INITIAL_CASH = 10
INITIAL_SHOP_NAME = "Brooklyn"
BASE_INCOME_PER_SECOND = 0.1
BASE_UPGRADE_COST = 75
UPGRADE_COST_MULTIPLIER = 1.75
UPGRADE_FAILURE_CHANCE = 0.15 # 15% chance for an upgrade to fail
BASE_EXPANSION_COST = 1000 # Base cost to expand
SABOTAGE_BASE_COST = 1000     # <<< Increased
SABOTAGE_PCT_COST = 0.05      # <<< Increased (5%)
SABOTAGE_SUCCESS_CHANCE = 0.40  # <<< Decreased (40%)
SABOTAGE_BACKFIRE_CHANCE = 0.25 # <<< Added (25% on failure)
SABOTAGE_DURATION_SECONDS = 3600
SABOTAGE_COOLDOWN_SECONDS = 900 # <<< Added (15 minutes)

# Expansion: Location: (Req Type, Req Val, GDP Factor, Cost Scale Factor)
# Req Types: 'level' (initial shop level), 'total_income', 'shops_count', 'has_shop' (requires specific shop)
# GDP Factor: Multiplies base income per level.
# Cost Scale Factor: Multiplies base upgrade cost.
EXPANSION_LOCATIONS = {
    # USA - NYC Boroughs
    "Manhattan":    ("level", 5, 1.5, 1.5),      # Req: Brooklyn Lvl 5
    "Queens":       ("level", 10, 1.2, 1.2),     # Req: Brooklyn Lvl 10 (Lower GDP/Cost than Manhattan)
    # USA - Regional
    "Philadelphia": ("level", 15, 1.8, 1.8),     # Req: Brooklyn Lvl 15
    "Albany":       ("total_income", 25000, 1.4, 1.5),     # Req: Total Earned $25k
    "Chicago":      ("shops_count", 5, 2.5, 2.5),     # Req: Own 5 Shops Total
    "Los Angeles":  ("shop_level", "Manhattan", 10, 3.0, 3.0), # <<< New: Req Manhattan Lvl 10
    "Miami":        ("shop_level", "Philadelphia", 10, 2.2, 2.8), # <<< New: Req Philly Lvl 10
    # Europe
    "London":       ("total_income", 100000, 3.5, 4.0),     # Req: Total Earned $100k
    "Paris":        ("has_shop", "London", 3.2, 3.8),     # Req: Own London shop
    "Rome":         ("has_shop", "Paris", 5, 3.0, 3.5),    # <<< New: Req Paris Lvl 5
    # Asia
    "Tokyo":        ("total_income", 500000, 5.0, 6.0),     # Req: Total Earned $500k
    "Beijing":      ("has_shop", "Tokyo", 4.5, 5.5),     # Req: Own Tokyo shop
    "Dubai":        ("has_shop", "Chicago", 4.8, 6.5),    # <<< New: Req Own Chicago
    # Americas (Other)
    "Mexico City":  ("shops_count", 7, 2.0, 2.0),     # Req: Own 7 Shops Total
    "Rio de Janeiro": ("has_shop", "Mexico City", 1.8, 2.2), # <<< New: Req Own Mexico City
    # Oceania
    "Sydney":       ("total_income", 250000, 2.8, 3.5),     # Req: Total Earned $250k
}

# --- Achievement Definitions ---
# ID: (Name, Description, Check Function Args, Requirement, Reward Type, Reward Value, Title Awarded)
# Check Function Args: Tuple defining what metric to check (e.g., ('total_income',), ('shops_count',))
ACHIEVEMENTS = {
    # Income Milestones
    "income_1k": ("Pizza Mogul", "Earn $1,000 total", ('total_income_earned',), 1000, 'cash', 100, "Mogul"),
    "income_10k": ("Pizza Tycoon", "Earn $10,000 total", ('total_income_earned',), 10000, 'pizza_coins', 50, "Tycoon"),
    "income_100k": ("Pizza Baron", "Earn $100,000 total", ('total_income_earned',), 100000, 'pizza_coins', 250, "Baron"),
    "income_1m": ("Pizza Magnate", "Earn $1,000,000 total", ('total_income_earned',), 1000000, 'pizza_coins', 1000, "Magnate"),
    # Shop Count Milestones
    "shops_3": ("City Spreader", "Own 3 shops", ('shops_count',), 3, 'cash', 500, "City Spreader"),
    "shops_5": ("Empire Builder", "Own 5 shops", ('shops_count',), 5, 'pizza_coins', 100, "Empire Builder"),
    # Specific Shop Level Milestones
    "brooklyn_10": ("Brooklyn Boss", "Upgrade Brooklyn to Level 10", ('shop_level', INITIAL_SHOP_NAME), 10, 'cash', 2000, "Brooklyn Boss"),
    "manhattan_5": ("Manhattan Maven", "Upgrade Manhattan to Level 5", ('shop_level', "Manhattan"), 5, 'pizza_coins', 25, "Manhattan Maven"),
    # Expansion Milestones
    "first_expansion": ("Branching Out", "Open your second shop", ('shops_count',), 2, 'cash', 250, None),
    "statewide": ("Empire State of Mind", "Expand to Albany", ('has_shop', "Albany"), 1, 'pizza_coins', 75, None),
    "london_calling": ("London Calling", "Expand to London", ('has_shop', "London"), 1, 'pizza_coins', 150, "Globetrotter"),
    "asia_expansion": ("Taste of the East", "Expand to Tokyo", ('has_shop', "Tokyo"), 1, 'pizza_coins', 200, None),
    "la_la_land": ("La La Land", "Expand to Los Angeles", ('has_shop', "Los Angeles"), 1, 'pizza_coins', 100, "West Coast Boss"), # <<< New
    "roman_holiday": ("Roman Holiday", "Expand to Rome", ('has_shop', "Rome"), 1, 'pizza_coins', 125, None), # <<< New
    # Add more achievements: rivals defeated (requires rival logic), specific shop levels, etc.
}

# --- Challenge Definitions ---
# Type: (Description Template, Metric, Timescale ('daily', 'weekly'), Base Goal, Goal Increase Per Level (approx), Reward Type, Base Reward, Reward Increase Per Level)
CHALLENGE_TYPES = {
    "earn_cash": ("Earn ${goal:,.2f} {timescale}", "session_income", None, 100, 1.5, 'cash', 50, 1.5),
    "upgrade_shops": ("Upgrade {goal} shops {timescale}", "session_upgrades", None, 1, 1.2, 'pizza_coins', 10, 1.3),
    "collect_times": ("Collect income {goal} times {timescale}", "session_collects", None, 3, 1.1, 'cash', 20, 1.2),
    "expand_shops": ("Expand to {goal} new location(s) {timescale}", "session_expansions", None, 1, 1.1, 'pizza_coins', 50, 1.4) # Requires tracking expansions
}

# --- Database Player Data Management ---

def update_display_name(user_id: int, user: "telegram.User | None") -> None:
    """Updates the player's display name in the database if available."""
    if not user or not user.full_name:
        return # Cannot update without user object or name

    logger.debug(f"Checking/Updating display name for user {user_id}")
    conn = get_db_connection()
    if not conn: return

    sql_update = "UPDATE players SET display_name = %s WHERE user_id = %s AND (display_name IS NULL OR display_name != %s);"
    try:
        with conn.cursor() as cur:
            cur.execute(sql_update, (user.full_name, user_id, user.full_name))
            if cur.rowcount > 0:
                logger.info(f"Updated display name for user {user_id} to '{user.full_name}'")
        conn.commit()
    except psycopg2.DatabaseError as e:
        logger.error(f"Database error updating display name for {user_id}: {e}", exc_info=True)
        conn.rollback()

def load_player_data(user_id: int) -> dict | None:
    """Loads player data from the database. Returns default state if not found."""
    logger.debug(f"Attempting to load data for user {user_id} from database.")
    conn = get_db_connection()
    if not conn: return get_default_player_state(user_id) # Return default if DB fails initially

    sql = """
    SELECT display_name, franchise_name, cash, pizza_coins, shops, unlocked_achievements, current_title,
           active_challenges, challenge_progress, stats, total_income_earned, last_login_time,
           collection_count, last_sabotage_attempt_time
    FROM players WHERE user_id = %s;
    """
    default_state = get_default_player_state(user_id)

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))
            result = cur.fetchone()

        if result:
            logger.debug(f"Found existing player data for {user_id}.")
            player_data = {
                "user_id": user_id,
                "display_name": result[0],
                "franchise_name": result[1],
                "cash": float(result[2]),
                "pizza_coins": result[3],
                "shops": result[4] if result[4] is not None else {},
                "unlocked_achievements": result[5] if result[5] is not None else [],
                "current_title": result[6],
                "active_challenges": result[7] if result[7] is not None else {'daily': None, 'weekly': None},
                "challenge_progress": result[8] if result[8] is not None else {'daily': {}, 'weekly': {}},
                "stats": result[9] if result[9] is not None else {},
                "total_income_earned": float(result[10]),
                "last_login_time": result[11].timestamp() if result[11] else time.time(),
                "collection_count": result[12] or 0,
                "last_sabotage_attempt_time": result[13].timestamp() if result[13] else 0.0
            }
            player_data.setdefault("active_challenges", {'daily': None, 'weekly': None})
            player_data.setdefault("challenge_progress", {'daily': {}, 'weekly': {}})
            player_data.setdefault("stats", {})
            player_data['stats'].setdefault('session_income', 0)
            player_data['stats'].setdefault('session_upgrades', 0)
            player_data['stats'].setdefault('session_collects', 0)
            player_data['stats'].setdefault('session_expansions', 0)
            # --- Migration / Defaulting for shop names --- #
            if player_data["shops"]:
                for loc, shop_data in player_data["shops"].items():
                    shop_data.setdefault("custom_name", loc) # Default name to location if missing
                    # Ensure level and time exist too for consistency
                    shop_data.setdefault("level", 1)
                    shop_data.setdefault("last_collected_time", time.time())
                    shop_data.setdefault("shutdown_until", None) # <<< Add default
            # --- End Migration --- #
            return player_data
        else:
            logger.info(f"No player data found for {user_id}. Inserting default state.")
            default_state["collection_count"] = 0 # Ensure default includes it
            save_player_data(user_id, default_state)
            return default_state

    except psycopg2.DatabaseError as e:
        logger.error(f"Database error loading data for {user_id}: {e}", exc_info=True)
        conn.rollback()
        # Fallback strategy: return default state without saving
        return default_state
    except Exception as e:
         logger.error(f"Unexpected error loading data for {user_id}: {e}", exc_info=True)
         return default_state # General fallback

def save_player_data(user_id: int, data: dict) -> None:
    """Saves player data to the database using INSERT ON CONFLICT (upsert)."""
    logger.debug(f"Attempting to save data for user {user_id} to database.")
    conn = get_db_connection()
    if not conn:
        logger.error(f"Cannot save data for {user_id}, no database connection.")
        return

    # Ensure necessary top-level keys exist with defaults before saving
    data.setdefault("cash", 0.0)
    data.setdefault("pizza_coins", 0)
    data.setdefault("shops", {})
    data.setdefault("unlocked_achievements", [])
    data.setdefault("current_title", None)
    data.setdefault("active_challenges", {'daily': None, 'weekly': None})
    data.setdefault("challenge_progress", {'daily': {}, 'weekly': {}})
    data.setdefault("stats", {})
    data.setdefault("total_income_earned", 0.0)
    data.setdefault("last_login_time", time.time()) # Use current time if missing
    data.setdefault("collection_count", 0) # Ensure collection_count key exists
    data.setdefault("franchise_name", None) # Ensure key exists
    data.setdefault("last_sabotage_attempt_time", 0.0) # <<< Added default

    # Ensure default sub-dicts/lists for JSONB compatibility
    data["shops"] = data.get("shops") or {}
    data["unlocked_achievements"] = data.get("unlocked_achievements") or []
    data["active_challenges"] = data.get("active_challenges") or {'daily': None, 'weekly': None}
    data["challenge_progress"] = data.get("challenge_progress") or {'daily': {}, 'weekly': {}}
    data["stats"] = data.get("stats") or {}
    data['stats'].setdefault('session_income', 0)
    data['stats'].setdefault('session_upgrades', 0)
    data['stats'].setdefault('session_collects', 0)
    data['stats'].setdefault('session_expansions', 0)

    # Ensure shop sub-dictionaries have default names
    if data["shops"]:
        for loc, shop_data in data["shops"].items():
            shop_data.setdefault("custom_name", loc)
            shop_data.setdefault("level", 1)
            shop_data.setdefault("last_collected_time", time.time())
            shop_data.setdefault("shutdown_until", None) # <<< Add default

    sql = """
    INSERT INTO players (
        user_id, display_name, franchise_name, cash, pizza_coins, shops, unlocked_achievements, current_title,
        active_challenges, challenge_progress, stats, total_income_earned, last_login_time,
        collection_count, last_sabotage_attempt_time
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), %s, to_timestamp(%s))
    ON CONFLICT (user_id) DO UPDATE SET
        display_name = EXCLUDED.display_name,
        franchise_name = EXCLUDED.franchise_name,
        cash = EXCLUDED.cash,
        pizza_coins = EXCLUDED.pizza_coins,
        shops = EXCLUDED.shops,
        unlocked_achievements = EXCLUDED.unlocked_achievements,
        current_title = EXCLUDED.current_title,
        active_challenges = EXCLUDED.active_challenges,
        challenge_progress = EXCLUDED.challenge_progress,
        stats = EXCLUDED.stats,
        total_income_earned = EXCLUDED.total_income_earned,
        last_login_time = EXCLUDED.last_login_time,
        collection_count = EXCLUDED.collection_count,
        last_sabotage_attempt_time = EXCLUDED.last_sabotage_attempt_time;
    """
    try:
        # Convert complex types to JSON strings for psycopg2 if needed,
        # though register_default_jsonb should handle dicts/lists directly.
        shops_json = json.dumps(data["shops"])
        achievements_list = data["unlocked_achievements"] # Keep as list for TEXT[]
        active_challenges_json = json.dumps(data["active_challenges"])
        challenge_progress_json = json.dumps(data["challenge_progress"])
        stats_json = json.dumps(data["stats"])

        with conn.cursor() as cur:
            cur.execute(sql, (
                user_id,
                data["display_name"],
                data["franchise_name"],
                data["cash"],
                data["pizza_coins"],
                shops_json,
                achievements_list,
                data["current_title"],
                active_challenges_json,
                challenge_progress_json,
                stats_json,
                data["total_income_earned"],
                data["last_login_time"],
                data["collection_count"],
                data["last_sabotage_attempt_time"]
            ))
        conn.commit()
        logger.debug(f"Successfully saved data for user {user_id}.")
    except psycopg2.DatabaseError as e:
        logger.error(f"Database error saving data for {user_id}: {e}", exc_info=True)
        conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error saving data for {user_id}: {e}", exc_info=True)
        # Attempt rollback just in case
        try:
            conn.rollback()
        except psycopg2.InterfaceError: # If connection already closed
             pass

def get_all_user_ids() -> list[int]:
    """Fetches all user IDs from the players table."""
    logger.debug("Fetching all user IDs from database.")
    conn = get_db_connection()
    if not conn: return []
    results = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM players;")
            results = [row[0] for row in cur.fetchall()]
        logger.debug(f"Fetched {len(results)} user IDs.")
    except psycopg2.DatabaseError as e:
        logger.error(f"Database error fetching all user IDs: {e}", exc_info=True)
        conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error fetching all user IDs: {e}", exc_info=True)
    return results

def get_default_player_state(user_id: int) -> dict:
    """Returns the initial state dictionary for a new player."""
    logger.info(f"Generating default state dictionary for user {user_id}")
    return {
        "user_id": user_id,
        "display_name": None,
        "franchise_name": None,
        "cash": float(INITIAL_CASH),
        "pizza_coins": 0,
        "shops": {
            INITIAL_SHOP_NAME: {
                "custom_name": INITIAL_SHOP_NAME,
                "level": 1,
                "last_collected_time": time.time(),
                "shutdown_until": None # <<< Add default
            }
        },
        "unlocked_achievements": [],
        "current_title": None,
        "active_challenges": {'daily': None, 'weekly': None},
        "challenge_progress": {'daily': {}, 'weekly': {}},
        "stats": {
            'session_income': 0, 'session_upgrades': 0,
            'session_collects': 0, 'session_expansions': 0
        },
        "total_income_earned": 0.0,
        "last_login_time": time.time(),
        "collection_count": 0,
        "last_sabotage_attempt_time": 0.0
    }

# --- Income Calculation (Uses GDP Factor) ---
def calculate_income_rate(shops: dict) -> float:
    total_rate = 0.0
    for name, shop_data in shops.items():
        level = shop_data.get("level", 1)
        # Use get_shop_income_rate which now includes GDP factor
        shop_rate = get_shop_income_rate(name, level)
        total_rate += shop_rate
    return total_rate

def get_shop_income_rate(shop_name: str, level: int) -> float:
    """Calculates the income rate, including base GDP and current performance."""
    base_gdp_factor = 1.0
    if shop_name != INITIAL_SHOP_NAME and shop_name in EXPANSION_LOCATIONS:
        if len(EXPANSION_LOCATIONS[shop_name]) > 2:
             base_gdp_factor = EXPANSION_LOCATIONS[shop_name][2]
        else:
             logger.warning(f"Missing GDP factor for expansion {shop_name}, using 1.0")

    current_performance = get_current_performance_multiplier(shop_name)
    # Combine base potential with current market fluctuation
    effective_rate = (BASE_INCOME_PER_SECOND * level * base_gdp_factor) * current_performance
    return effective_rate

def calculate_uncollected_income(player_data: dict) -> float:
    current_time = time.time()
    total_uncollected = 0.0
    shops = player_data.get("shops", {})
    for name, shop_data in shops.items():
        level = shop_data.get("level", 1)
        last_collected = shop_data.get("last_collected_time", current_time)
        shutdown_until = shop_data.get("shutdown_until") # Get shutdown time

        # Determine the effective start time for calculation for this period
        effective_start_time = last_collected
        if shutdown_until and shutdown_until > last_collected:
            # If shutdown ended after last collection, start earning from shutdown end
            effective_start_time = max(last_collected, shutdown_until)

        # Determine the effective end time for calculation for this period
        effective_end_time = current_time
        if shutdown_until and shutdown_until > effective_start_time:
            # If still shut down, or shut down started within this period,
            # cap earning time at the shutdown start time.
            effective_end_time = min(current_time, shutdown_until)

        # Calculate the duration the shop was actually active
        active_duration = max(0, effective_end_time - effective_start_time)

        if active_duration > 0:
            shop_rate = get_shop_income_rate(name, level)
            total_uncollected += shop_rate * active_duration
        # else: logger.debug(f"Shop {name} generated 0 income (active_duration: {active_duration})")

    return total_uncollected

def collect_income(user_id: int) -> tuple[float, list[str], bool, float | None]:
    """Collects income, increments count, checks for Mafia.
       Returns (collected_amount, completed_challenges, is_mafia_event, mafia_demand_or_None)."""
    player_data = load_player_data(user_id)
    if not player_data:
        logger.error(f"Failed to load player data for collect_income, user {user_id}")
        return 0.0, [], False, None

    uncollected = calculate_uncollected_income(player_data)
    completed_challenges = []
    is_mafia_event = False
    mafia_demand = None

    if uncollected > 0.01:
        # --- Update collection time and count FIRST --- #
        current_time = time.time()
        for shop_name in player_data["shops"]:
            # Ensure shop_data exists before accessing
            if shop_name in player_data.get("shops", {}):
                 player_data["shops"][shop_name]["last_collected_time"] = current_time
            else:
                 logger.warning(f"Shop {shop_name} found in keys but not in dict during collect for user {user_id}")

        player_data["collection_count"] = player_data.get("collection_count", 0) + 1
        collection_count = player_data["collection_count"]
        logger.info(f"User {user_id} collection attempt #{collection_count}. Amount: ${uncollected:.2f}")

        # Save the updated time and count *before* deciding the outcome
        # This prevents collecting the same time window again if Mafia event happens
        save_player_data(user_id, player_data)
        # --- End Time/Count Update --- #

        # --- Check for Mafia Event --- #
        if collection_count > 0 and collection_count % 5 == 0:
            is_mafia_event = True
            demand_percentage = random.uniform(0.10, 0.75)
            mafia_demand = round(uncollected * demand_percentage, 2)
            logger.info(f"Mafia event triggered for user {user_id}! Demand: ${mafia_demand:.2f} ({demand_percentage*100:.1f}%)")
            # Return amount calculated from OLD time, but timestamps/count are already saved
            return uncollected, [], is_mafia_event, mafia_demand
        else:
            # --- Normal Collection --- #
            # Timestamps and count already saved, now just add cash/stats
            player_data["cash"] = player_data.get("cash", 0) + uncollected
            player_data["total_income_earned"] = player_data.get("total_income_earned", 0) + uncollected
            player_data["stats"]["session_income"] = player_data["stats"].get("session_income", 0) + uncollected
            player_data["stats"]["session_collects"] = player_data["stats"].get("session_collects", 0) + 1

            completed_challenges = update_challenge_progress(player_data, ["session_income", "session_collects"])
            save_player_data(user_id, player_data) # Save cash/stats update
            return uncollected, completed_challenges, is_mafia_event, mafia_demand
    else:
        # Nothing to collect, still return structure
        return 0.0, [], False, None

# --- Upgrade & Expansion Logic (Modified for failure chance) ---

def get_expansion_cost(shop_name: str) -> float:
    """Calculates the cost to expand to a new location."""
    base_cost = BASE_EXPANSION_COST
    cost_scale = 1.0 # Default if not found (shouldn't happen)
    if shop_name in EXPANSION_LOCATIONS:
        # Cost Scale Factor is the 4th element (index 3)
        if len(EXPANSION_LOCATIONS[shop_name]) > 3:
             cost_scale = EXPANSION_LOCATIONS[shop_name][3]
        else:
             logger.warning(f"Missing cost scale factor for expansion {shop_name} in cost calculation, using 1.0")
    else:
         logger.warning(f"Shop name {shop_name} not found in EXPANSION_LOCATIONS for cost calculation.")

    return round(base_cost * cost_scale, 2)

def get_upgrade_cost(current_level: int, shop_name: str) -> float:
    """Calculates the cost to upgrade to the next level, considering location."""
    base_location_cost = BASE_UPGRADE_COST

    # Get location cost scale factor (default to 1.0 for Brooklyn/initial)
    location_cost_scale = 1.0
    if shop_name != INITIAL_SHOP_NAME and shop_name in EXPANSION_LOCATIONS:
        # Ensure the tuple has the 4th element (cost scale)
        if len(EXPANSION_LOCATIONS[shop_name]) > 3:
             location_cost_scale = EXPANSION_LOCATIONS[shop_name][3]
        else:
             logger.warning(f"Missing cost scale factor for expansion {shop_name}, using 1.0")

    # Apply location scaling and level multiplier
    level_cost = (base_location_cost * location_cost_scale) * (UPGRADE_COST_MULTIPLIER ** (current_level - 1))
    return round(level_cost, 2) # Round to 2 decimal places

def upgrade_shop(user_id: int, shop_name: str) -> tuple[bool, str, list[str]]:
    """Attempts to upgrade a shop with a chance of failure.
       Returns (success, message_or_data, completed_challenge_messages)."""
    player_data = load_player_data(user_id)
    if not player_data:
        return False, "Failed to load player data.", []

    shops = player_data.get("shops", {})
    completed_challenges = []

    if shop_name not in shops:
        return False, f"You don't own a shop in {shop_name}!", []

    current_level = shops[shop_name].get("level", 1)
    cost = get_upgrade_cost(current_level, shop_name)
    cash = player_data.get("cash", 0)

    if cash < cost:
        return False, f"Not enough cash! Need ${cost:,.2f} to upgrade {shop_name} to level {current_level + 1}. You have ${cash:,.2f}.", []

    # --- Upgrade Attempt: Deduct cost first --- #
    player_data["cash"] = cash - cost
    logger.info(f"User {user_id} attempting upgrade on {shop_name} Lvl {current_level}. Cost: ${cost:,.2f}. New cash (temp): ${player_data['cash']:.2f}")

    # --- Check for Failure --- #
    if random.random() < UPGRADE_FAILURE_CHANCE:
        logger.warning(f"Upgrade FAILED for user {user_id} on {shop_name} Lvl {current_level}!")
        # Save the data with deducted cash, but no level increase or stats update
        save_player_data(user_id, player_data)
        # Return False and the cost (so main.py can mention it in the failure message)
        return False, f"Oh no! The upgrade failed! You lost ${cost:,.2f} in the attempt!", [] # Specific message format
    else:
        # --- Success --- #
        logger.info(f"Upgrade SUCCEEDED for user {user_id} on {shop_name} Lvl {current_level}.")
        new_level = current_level + 1
        player_data["shops"][shop_name]["level"] = new_level
        # Only update stats on success
        player_data["stats"]["session_upgrades"] = player_data["stats"].get("session_upgrades", 0) + 1

        # Check challenges after successful upgrade
        completed_challenges = update_challenge_progress(player_data, ["session_upgrades"])

        save_player_data(user_id, player_data)

        # Return True and the new level as a string
        return True, str(new_level), completed_challenges

def get_available_expansions(player_data: dict) -> list[str]:
    available = []
    owned_shops = player_data.get("shops", {})
    initial_shop_level = owned_shops.get(INITIAL_SHOP_NAME, {}).get("level", 1)
    total_income = player_data.get("total_income_earned", 0)

    for name, req_data in EXPANSION_LOCATIONS.items():
        if name in owned_shops:
            continue

        # Unpack based on expected length for safety
        req_type = req_data[0]
        req_value = req_data[1]

        met_requirement = False
        if req_type == "level": # This was initially just for Brooklyn
            # Assume 'level' requirement still refers to INITIAL_SHOP_NAME level for now
            # unless we redesign requirements significantly
            if initial_shop_level >= req_value:
                met_requirement = True
        elif req_type == "shop_level": # New: Requirement on a specific OTHER shop's level
            required_shop_name = req_value # In this case, req_value is the shop name
            required_level = req_data[2]   # The actual level needed is now 3rd element
            current_shop_level = owned_shops.get(required_shop_name, {}).get("level", 0)
            if current_shop_level >= required_level:
                met_requirement = True
        elif req_type == "total_income":
            if total_income >= req_value:
                met_requirement = True
        elif req_type == "shops_count":
            if len(owned_shops) >= req_value:
                 met_requirement = True
        elif req_type == "has_shop":
             prereq_shop = req_value
             if prereq_shop in owned_shops:
                  met_requirement = True

        if met_requirement:
            available.append(name)
    return available

def expand_shop(user_id: int, expansion_name: str) -> tuple[bool, str, list[str]]:
    """Attempts to establish a new shop, checking and deducting cost."""
    player_data = load_player_data(user_id)
    if not player_data: return False, "Failed to load player data.", []

    available_expansions = get_available_expansions(player_data)
    completed_challenges = []

    if expansion_name not in EXPANSION_LOCATIONS:
         return False, f"{expansion_name} is not a valid expansion location.", []

    if expansion_name in player_data["shops"]:
        return False, f"You already have a shop in {expansion_name}!", []

    if expansion_name not in available_expansions:
        req_data = EXPANSION_LOCATIONS[expansion_name]
        req_type = req_data[0]
        req_value = req_data[1]

        # Generate requirement message
        req_msg = f"You don't meet the requirements to expand to {expansion_name} yet."
        if req_type == "level": req_msg = f"Requires {INITIAL_SHOP_NAME} Lvl {req_value}."
        elif req_type == "shop_level": req_msg = f"Requires {req_value} Lvl {req_data[2]}."
        elif req_type == "total_income": req_msg = f"Requires ${req_value:,.2f} total income earned."
        elif req_type == "shops_count": req_msg = f"Requires {req_value} total shops (you have {len(player_data.get('shops', {}))})."
        elif req_type == "has_shop": req_msg = f"Requires owning a shop in {req_value}."

        return False, f"Can't expand to {expansion_name} yet. {req_msg}", []

    # --- Expansion Cost Check --- #
    expansion_cost = get_expansion_cost(expansion_name)
    current_cash = player_data.get("cash", 0)

    if current_cash < expansion_cost:
        return False, f"Not enough cash to expand to {expansion_name}! Need ${expansion_cost:,.2f}, you have ${current_cash:,.2f}.", []
    # --- End Cost Check --- #

    # Deduct cost, add shop, update stats
    player_data["cash"] = current_cash - expansion_cost
    logger.info(f"User {user_id} expanding to {expansion_name}. Cost: ${expansion_cost:,.2f}. New cash: ${player_data['cash']:.2f}")

    player_data["shops"][expansion_name] = {
        "custom_name": expansion_name,
        "level": 1,
        "last_collected_time": time.time()
    }
    player_data["stats"]["session_expansions"] = player_data["stats"].get("session_expansions", 0) + 1

    completed_challenges = update_challenge_progress(player_data, ["session_expansions"])
    save_player_data(user_id, player_data)

    # Return success message (main.py handles cheeky message)
    msg = f"Expansion to {expansion_name} successful! Cost: ${expansion_cost:,.2f}"
    return True, msg, completed_challenges

# --- Achievement Logic ---

def get_achievement_value(player_data: dict, metric_args: tuple) -> float | int:
    """Gets the current value for an achievement metric."""
    metric = metric_args[0]
    shops = player_data.get("shops", {})

    if metric == 'total_income_earned':
        return player_data.get("total_income_earned", 0)
    elif metric == 'shops_count':
        return len(shops)
    elif metric == 'shop_level':
        shop_name = metric_args[1]
        return shops.get(shop_name, {}).get("level", 0)
    elif metric == 'has_shop':
        shop_name = metric_args[1]
        return 1 if shop_name in shops else 0
    # Add more metrics here
    else:
        return 0

def check_achievements(user_id: int) -> list[tuple[str, str, str | None]]:
    """Checks for unlocked achievements and returns (name, description, title) for newly unlocked ones."""
    player_data = load_player_data(user_id)
    unlocked_achievements = player_data.get("unlocked_achievements", [])
    newly_unlocked = []
    highest_new_title = None

    for achievement_id, (name, desc, metric_args, req, _, _, title) in ACHIEVEMENTS.items():
        if achievement_id not in unlocked_achievements:
            current_value = get_achievement_value(player_data, metric_args)
            if current_value >= req:
                logger.info(f"User {user_id} unlocked achievement: {achievement_id} ({name})")
                unlocked_achievements.append(achievement_id)
                newly_unlocked.append((name, desc, title))
                if title:
                    # Simple logic: last unlocked title is equipped? Or choose based on rank?
                    highest_new_title = title # For now, just take the latest one

    if newly_unlocked:
        player_data["unlocked_achievements"] = unlocked_achievements
        if highest_new_title:
             player_data["current_title"] = highest_new_title
             logger.info(f"User {user_id} equipped title: {highest_new_title}")
        save_player_data(user_id, player_data)

    return newly_unlocked

# --- Challenge Logic ---

def generate_new_challenges(user_id: int, timescale: str):
    """Generates new daily or weekly challenges for the player."""
    logger.info(f"Attempting to generate {timescale} challenge for user {user_id}.")
    try:
        player_data = load_player_data(user_id)
        player_level = len(player_data.get("unlocked_achievements", [])) # Use achievement count as proxy for level
        logger.debug(f"Player {user_id} level (based on achievements): {player_level}")

        # Choose a random challenge type
        challenge_type_id = random.choice(list(CHALLENGE_TYPES.keys()))
        logger.debug(f"Selected challenge type for {user_id} ({timescale}): {challenge_type_id}")
        desc_template, metric, _, base_goal, goal_mult, reward_type, base_reward, reward_mult = CHALLENGE_TYPES[challenge_type_id]

        # Scale goal and reward based on player level (simple example)
        goal = int(base_goal * (goal_mult ** player_level))
        reward_value = int(base_reward * (reward_mult ** player_level))
        logger.debug(f"Calculated goal: {goal}, reward: {reward_value} for {user_id} ({timescale})")

        # Prevent excessively easy goals
        if "cash" in metric and goal < 100: goal = 100
        if "upgrade" in metric and goal < 1: goal = 1
        if "collect" in metric and goal < 2: goal = 2

        description = desc_template.format(goal=goal, timescale=timescale)
        logger.debug(f"Formatted description: {description}")

        challenge_data = {
            "id": f"{timescale}_{challenge_type_id}_{int(time.time())}", # Unique ID
            "type": challenge_type_id,
            "description": description,
            "metric": metric,
            "goal": goal,
            "reward_type": reward_type,
            "reward_value": reward_value,
            "start_time": time.time(),
            "timescale": timescale
        }

        player_data["active_challenges"][timescale] = challenge_data
        player_data["challenge_progress"][timescale] = {} # Reset progress for this timescale
        player_data["stats"] = {k: 0 for k in player_data["stats"]} # Reset tracked stats
        logger.debug(f"Updated player_data challenge/stats for {user_id} ({timescale})")

        logger.info(f"Generated new {timescale} challenge for user {user_id}: {description} (Goal: {goal} {metric}, Reward: {reward_value} {reward_type})")
        save_player_data(user_id, player_data)
        logger.info(f"Successfully saved player data after {timescale} challenge generation for {user_id}.")
    except Exception as e:
        logger.error(f"ERROR during generate_new_challenges for user {user_id}, timescale {timescale}: {e}", exc_info=True)
        # Re-raise or handle appropriately? For now, just log.

def update_challenge_progress(player_data: dict, updated_metrics: list[str]) -> list[str]:
    """Updates progress for active challenges based on player stats and returns messages for completed challenges."""
    completed_messages = []
    user_id = player_data["user_id"]
    stats = player_data.get("stats", {})

    for timescale, challenge in player_data["active_challenges"].items():
        if challenge and challenge["metric"] in updated_metrics:
            metric = challenge["metric"]
            current_progress = stats.get(metric, 0)
            goal = challenge["goal"]
            challenge_id = challenge["id"]
            progress_key = f"{timescale}_{metric}" # Use a timescale-specific key if needed, maybe just metric is fine?

            logger.debug(f"Checking {timescale} challenge progress for {user_id}. Metric: {metric}, Progress: {current_progress}, Goal: {goal}")

            # Check if progress already met (avoid duplicate completions)
            if player_data["challenge_progress"].get(timescale, {}).get(challenge_id, False):
                continue # Already completed this specific challenge

            if current_progress >= goal:
                logger.info(f"User {user_id} completed {timescale} challenge: {challenge['description']}")
                reward_type = challenge["reward_type"]
                reward_value = challenge["reward_value"]
                # Format reward value with commas
                msg = f"🎉 Challenge Complete! 🎉\n\"{challenge['description']}\"\nReward: {reward_value:,} {reward_type.upper()}!"

                # Grant reward
                if reward_type == 'cash':
                    player_data["cash"] = player_data.get("cash", 0) + reward_value
                elif reward_type == 'pizza_coins':
                    player_data["pizza_coins"] = player_data.get("pizza_coins", 0) + reward_value

                completed_messages.append(msg)
                # Mark as completed for this period
                player_data["challenge_progress"].setdefault(timescale, {})[challenge_id] = True
                # We don't remove the active challenge here, just mark progress complete.
                # It will be replaced by generate_new_challenges on schedule.

    # No need to save here, the calling function (collect, upgrade) will save.
    return completed_messages

# --- Status Formatting (with sorting) ---
def format_status(player_data: dict, sort_by: str = 'name') -> str:
    """Formats the player's status, allowing sorting of the shop list."""
    user_id = player_data.get("user_id", "Unknown")
    cash = player_data.get("cash", 0)
    pizza_coins = player_data.get("pizza_coins", 0)
    shops = player_data.get("shops", {})
    total_income_earned = player_data.get("total_income_earned", 0)
    franchise_name = player_data.get("franchise_name")
    title = player_data.get("current_title", None)
    achievements_unlocked = len(player_data.get("unlocked_achievements", []))

    header = f"<b>--- {franchise_name or 'Your Pizza Empire'} ---</b>"
    if title:
        header += f"\n<i>Title: &lt;{title}&gt;</i>"

    status_lines = [
        header,
        f"(Player ID: {user_id})",
        f"<b>Cash:</b> ${cash:,.2f}",
        f"<b>Pizza Coins:</b> {pizza_coins:,} 🍕",
        f"<b>Total Income Earned:</b> ${total_income_earned:,.2f}",
        f"<b>Achievements Unlocked:</b> {achievements_unlocked}",
        "<b>Shops:</b>"
    ]

    if not shops:
        status_lines.append("  None yet! Use /start")
    else:
        # --- Sorting Logic --- #
        shop_list_to_sort = []
        for name, data in shops.items():
            level = data.get("level", 1)
            custom_name = data.get("custom_name", name)
            upgrade_cost = get_upgrade_cost(level, name)
            current_perf = get_current_performance_multiplier(name)
            shop_list_to_sort.append({
                'location': name,
                'level': level,
                'custom_name': custom_name,
                'upgrade_cost': upgrade_cost,
                'performance': current_perf
            })

        valid_sort_keys = ['name', 'level', 'cost']
        sort_key_internal = 'location' # Default db key
        reverse_sort = False

        sort_param = sort_by.lower()
        if sort_param == 'level':
            sort_key_internal = 'level'
            reverse_sort = True # Highest level first
        elif sort_param == 'upgrade_cost' or sort_param == 'cost':
            sort_key_internal = 'upgrade_cost'
            # Default: Lowest cost first (reverse=False)
        # else default sort by name/location (reverse=False)

        logger.debug(f"Sorting shops by '{sort_key_internal}', reverse={reverse_sort}")
        try:
            sorted_shops = sorted(shop_list_to_sort, key=lambda item: item[sort_key_internal], reverse=reverse_sort)
        except KeyError:
             logger.warning(f"Invalid sort key '{sort_key_internal}', defaulting to name sort.")
             sorted_shops = sorted(shop_list_to_sort, key=lambda item: item['location'])
        # --- End Sorting Logic --- #

        # Iterate through sorted list
        for shop_info in sorted_shops:
            name = shop_info['location']
            shop_data_dict = shops.get(name, {}) # Get the full data dict
            level = shop_info['level']
            custom_name = shop_info['custom_name']
            upgrade_cost = shop_info['upgrade_cost']
            current_perf = shop_info['performance']
            perf_emoji = "📈" if current_perf > 1.1 else "📉" if current_perf < 0.9 else "🤷‍♂️"
            display_shop_name = f"{custom_name} ({name})" if custom_name != name else name

            # Check for shutdown
            shutdown_str = ""
            shutdown_until = shop_data_dict.get("shutdown_until")
            if shutdown_until and shutdown_until > time.time():
                 time_left = timedelta(seconds=int(shutdown_until - time.time()))
                 shutdown_str = f" 🚫(Closed: {str(time_left).split('.')[0]})"

            status_lines.append(f"  - {perf_emoji} <b>{display_shop_name}:</b> Level {level} (Upgrade Cost: ${upgrade_cost:,.2f}){shutdown_str}")

    income_rate = calculate_income_rate(shops)
    status_lines.append(f"<b>Current Income Rate:</b> ${income_rate:.2f}/sec")
    uncollected_income = calculate_uncollected_income(player_data)
    status_lines.append(f"<b>Uncollected Income:</b> ${uncollected_income:.2f} (Use /collect)")
    available_expansions = get_available_expansions(player_data)
    status_lines.append("<b>Available Expansions:</b>")
    if available_expansions:
        exp_list_formatted = []
        for loc in available_expansions:
            req_data = EXPANSION_LOCATIONS[loc]
            req_type = req_data[0]
            req_value = req_data[1]
            gdp_factor = req_data[2]
            cost_scale = req_data[3]
            expansion_cost = get_expansion_cost(loc)
            current_perf = get_current_performance_multiplier(loc)
            perf_emoji = "📈" if current_perf > 1.1 else "📉" if current_perf < 0.9 else "🤷‍♂️"
            if req_type == "level": req_str = f"(Req: {INITIAL_SHOP_NAME} Lvl {req_value})"
            elif req_type == "shop_level": req_str = f"(Req: {req_value} Lvl {req_data[2]})"
            elif req_type == "total_income": req_str = f"(Req: Total Earned ${req_value:,.2f})"
            elif req_type == "shops_count": req_str = f"(Req: {req_value} Shops)"
            elif req_type == "has_shop": req_str = f"(Req: Own {req_value})"
            else: req_str = "(Unknown Req)"
            exp_list_formatted.append(f"  - {loc} {perf_emoji}x{current_perf:.1f} - Cost: ${expansion_cost:,.2f} {req_str} - Use /expand {loc.lower()}")
        status_lines.extend(sorted(exp_list_formatted)) # Sort expansions alphabetically
    else:
        status_lines.append("  None available right now. Keep upgrading!")
    status_lines.append("<b>Active Challenges:</b> (/challenges for details)")
    for timescale, challenge in player_data.get("active_challenges", {}).items():
        if challenge:
            status_lines.append(f"  - {timescale.capitalize()}: {challenge['description']}")
        else:
            status_lines.append(f"  - {timescale.capitalize()}: None active")
    status_lines.append("\n<i>Use Pizza Coins 🍕 to speed things up! (Coming Soon)</i>")
    return "\n".join(status_lines)

# --- Payment Logic (Pack Definitions) ---
PIZZA_COIN_PACKS = {
    "pack_small": ("Small Coin Pack", "A small boost for your empire!", 99, 100),
    "pack_medium": ("Medium Coin Pack", "A helpful amount of coins.", 499, 550),
    "pack_large": ("Large Coin Pack", "Rule the pizza world!", 999, 1200),
}

def get_pizza_coin_pack(pack_id: str) -> tuple | None:
    return PIZZA_COIN_PACKS.get(pack_id)

def credit_pizza_coins(user_id: int, amount: int):
    if amount <= 0:
        logger.warning(f"Attempted to credit non-positive coin amount ({amount}) for user {user_id}")
        return
    try:
        player_data = load_player_data(user_id)
        player_data["pizza_coins"] = player_data.get("pizza_coins", 0) + amount
        save_player_data(user_id, player_data)
        logger.info(f"Successfully credited {amount} Pizza Coins to user {user_id}. New balance: {player_data['pizza_coins']}")
    except Exception as e:
        logger.error(f"Failed to credit {amount} Pizza Coins to user {user_id}: {e}", exc_info=True)

# --- DEPRECATED placeholders ---
def use_pizza_coins_for_speedup(user_id: int, feature: str):
    logger.info(f"Placeholder: User {user_id} attempting to use Pizza Coins for {feature}.")
    player_data = load_player_data(user_id)
    if player_data.get("pizza_coins", 0) > 0:
        return "Spending Pizza Coins is coming soon!"
    else:
        return "You don't have any Pizza Coins! Purchases are coming soon."

def get_leaderboard_data(limit: int = 10) -> list[tuple[int, str | None, float]]:
    """Fetches top players based on total_income_earned."""
    logger.debug(f"Fetching leaderboard data (top {limit})")
    conn = get_db_connection()
    if not conn: return []

    sql = """
    SELECT user_id, display_name, total_income_earned
    FROM players
    ORDER BY total_income_earned DESC
    LIMIT %s;
    """
    results = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            fetched_results = cur.fetchall()
            # Convert numeric total_income_earned back to float
            results = [(row[0], row[1], float(row[2])) for row in fetched_results]
        logger.debug(f"Fetched {len(results)} rows for leaderboard.")
    except psycopg2.DatabaseError as e:
        logger.error(f"Database error fetching leaderboard: {e}", exc_info=True)
        conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error fetching leaderboard: {e}", exc_info=True)

    return results

def get_cash_leaderboard_data(limit: int = 10) -> list[tuple[int, str | None, float]]:
    """Fetches top players based on current cash on hand."""
    logger.debug(f"Fetching cash leaderboard data (top {limit})")
    conn = get_db_connection()
    if not conn: return []

    # Order by cash DESC
    sql = """
    SELECT user_id, display_name, cash
    FROM players
    ORDER BY cash DESC
    LIMIT %s;
    """
    results = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            fetched_results = cur.fetchall()
            # Convert numeric cash back to float
            results = [(row[0], row[1], float(row[2])) for row in fetched_results]
        logger.debug(f"Fetched {len(results)} rows for cash leaderboard.")
    except psycopg2.DatabaseError as e:
        logger.error(f"Database error fetching cash leaderboard: {e}", exc_info=True)
        conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error fetching cash leaderboard: {e}", exc_info=True)

    return results

# --- Helper to get display name by ID ---
def find_display_name_by_id(user_id: int) -> str | None:
     """Fetches just the display name for a given user ID."""
     conn = get_db_connection()
     if not conn: return None
     sql = "SELECT display_name FROM players WHERE user_id = %s;"
     name = None
     try:
         with conn.cursor() as cur:
             cur.execute(sql, (user_id,))
             result = cur.fetchone()
             if result:
                 name = result[0]
     except Exception as e:
          logger.error(f"Error fetching display name for {user_id}: {e}")
          # conn.rollback() # Read-only query, rollback might not be needed
     return name

# --- New Location Performance Functions ---
def get_current_performance_multiplier(location_name: str) -> float:
    """Gets the current performance multiplier for a location from the DB."""
    if location_name == INITIAL_SHOP_NAME: # Base location always has 1.0x performance
        return 1.0

    conn = get_db_connection()
    if not conn: return 1.0 # Default to 1.0 if DB error

    sql = "SELECT current_multiplier FROM location_performance WHERE location_name = %s;"
    multiplier = 1.0
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (location_name,))
            result = cur.fetchone()
            if result:
                multiplier = float(result[0])
            else:
                 # If location not in table yet, return 1.0 and log warning
                 logger.warning(f"No performance data found for {location_name}, returning 1.0.")
    except psycopg2.DatabaseError as e:
        logger.error(f"DB error fetching performance multiplier for {location_name}: {e}", exc_info=True)
        conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error fetching performance multiplier for {location_name}: {e}", exc_info=True)

    # Clamp multiplier just in case? Optional.
    # multiplier = max(0.5, min(2.0, multiplier))
    return multiplier

def update_location_performance():
    """Calculates and saves new random multipliers for all locations."""
    logger.info("Updating location performance multipliers...")
    conn = get_db_connection()
    if not conn: return

    sql = """
    INSERT INTO location_performance (location_name, current_multiplier, last_updated)
    VALUES (%s, %s, NOW())
    ON CONFLICT (location_name) DO UPDATE SET
        current_multiplier = EXCLUDED.current_multiplier,
        last_updated = EXCLUDED.last_updated;
    """
    updates = []
    for name, data in EXPANSION_LOCATIONS.items():
        # Fluctuate around 1.0, range e.g. 0.7 to 1.5
        fluctuation = random.uniform(0.7, 1.5)
        new_multiplier = round(fluctuation, 2)
        updates.append((name, new_multiplier))
        logger.debug(f"New performance for {name}: {new_multiplier:.2f}")

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, updates)
        conn.commit()
        logger.info(f"Successfully updated performance multipliers for {len(updates)} locations.")
    except psycopg2.DatabaseError as e:
        logger.error(f"DB error updating location performance: {e}", exc_info=True)
        conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error updating location performance: {e}", exc_info=True)

# --- New Sabotage Helper Functions ---
def get_top_earning_shop(shops: dict) -> str | None:
    """Finds the location name of the highest-earning shop."""
    top_shop = None
    max_rate = -1

    if not shops:
        return None

    for name, data in shops.items():
        level = data.get("level", 1)
        # Calculate potential rate (ignoring current performance/shutdown for targeting)
        gdp_factor = 1.0
        if name != INITIAL_SHOP_NAME and name in EXPANSION_LOCATIONS:
            if len(EXPANSION_LOCATIONS[name]) > 2: gdp_factor = EXPANSION_LOCATIONS[name][2]
        potential_rate = (BASE_INCOME_PER_SECOND * level * gdp_factor)

        if potential_rate > max_rate:
            max_rate = potential_rate
            top_shop = name

    # If only the initial shop exists, target that
    if top_shop is None and INITIAL_SHOP_NAME in shops:
        return INITIAL_SHOP_NAME

    return top_shop

def apply_shop_shutdown(target_user_id: int, shop_location: str, duration_seconds: int):
    """Applies a shutdown timer to a specific shop for a target user."""
    logger.info(f"Applying shutdown to {shop_location} for user {target_user_id} for {duration_seconds}s")
    player_data = load_player_data(target_user_id)
    if not player_data or shop_location not in player_data.get("shops", {}):
        logger.warning(f"Cannot apply shutdown: Player {target_user_id} or shop {shop_location} not found.")
        return False

    shutdown_end_time = time.time() + duration_seconds
    player_data["shops"][shop_location]["shutdown_until"] = shutdown_end_time
    save_player_data(target_user_id, player_data)
    logger.info(f"Shutdown applied successfully for {target_user_id}'s {shop_location} until {shutdown_end_time}")
    return True

# --- New function to find user by display name ---
def find_user_by_display_name(display_name: str) -> list[int]:
    """Finds user IDs by display name (case-insensitive)."""
    logger.debug(f"Searching for user ID by display name: {display_name}")
    conn = get_db_connection()
    if not conn: return []

    # Use LOWER() for case-insensitive comparison
    sql = "SELECT user_id FROM players WHERE LOWER(display_name) = LOWER(%s);"
    user_ids = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (display_name,))
            results = cur.fetchall()
            user_ids = [row[0] for row in results]
        logger.debug(f"Found {len(user_ids)} match(es) for display name '{display_name}'.")
    except psycopg2.DatabaseError as e:
        logger.error(f"DB error finding user by display name '{display_name}': {e}", exc_info=True)
        conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error finding user by display name '{display_name}': {e}", exc_info=True)

    return user_ids
