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

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS players (
        user_id BIGINT PRIMARY KEY,
        display_name TEXT,
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
        collection_count INTEGER DEFAULT 0
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
        conn.commit()
        logger.info("Players table checked/created successfully.")
    except psycopg2.DatabaseError as e:
        logger.error(f"Error initializing database table: {e}", exc_info=True)
        conn.rollback() # Rollback any partial changes

# --- Game Constants ---
INITIAL_CASH = 10
INITIAL_SHOP_NAME = "Brooklyn"
BASE_INCOME_PER_SECOND = 0.1
BASE_UPGRADE_COST = 75
UPGRADE_COST_MULTIPLIER = 1.75
UPGRADE_FAILURE_CHANCE = 0.15 # 15% chance for an upgrade to fail

EXPANSION_LOCATIONS = {
    "Manhattan":    ("level", 5, 1.5, 1.5),  # 1.5x cost
    "Queens":       ("level", 10, 2.0, 2.0),  # 2.0x cost
    "Philadelphia": ("level", 15, 3.0, 3.0),  # 3.0x cost
    "Albany":       ("total_income", 25000, 4.0, 2.5),  # 2.5x cost
    "Chicago":      ("shops_count", 5, 6.0, 4.0),  # 4.0x cost
    "Tokyo":        ("total_income", 1000000, 10.0, 7.5) # 7.5x cost
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
    "first_expansion": ("Branching Out", "Open your second shop", ('shops_count',), 2, 'cash', 250, None), # No title for this one
    "statewide": ("Empire State of Mind", "Expand to Albany", ('has_shop', "Albany"), 1, 'pizza_coins', 75, None),
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
    SELECT display_name, cash, pizza_coins, shops, unlocked_achievements, current_title,
           active_challenges, challenge_progress, stats, total_income_earned, last_login_time,
           collection_count
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
                "cash": float(result[1]),
                "pizza_coins": result[2],
                "shops": result[3] if result[3] is not None else {},
                "unlocked_achievements": result[4] if result[4] is not None else [],
                "current_title": result[5],
                "active_challenges": result[6] if result[6] is not None else {'daily': None, 'weekly': None},
                "challenge_progress": result[7] if result[7] is not None else {'daily': {}, 'weekly': {}},
                "stats": result[8] if result[8] is not None else {},
                "total_income_earned": float(result[9]),
                "last_login_time": result[10].timestamp() if result[10] else time.time(),
                "collection_count": result[11] or 0
            }
            player_data.setdefault("active_challenges", {'daily': None, 'weekly': None})
            player_data.setdefault("challenge_progress", {'daily': {}, 'weekly': {}})
            player_data.setdefault("stats", {})
            player_data['stats'].setdefault('session_income', 0)
            player_data['stats'].setdefault('session_upgrades', 0)
            player_data['stats'].setdefault('session_collects', 0)
            player_data['stats'].setdefault('session_expansions', 0)
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

    sql = """
    INSERT INTO players (
        user_id, display_name, cash, pizza_coins, shops, unlocked_achievements, current_title,
        active_challenges, challenge_progress, stats, total_income_earned, last_login_time,
        collection_count
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), %s)
    ON CONFLICT (user_id) DO UPDATE SET
        display_name = EXCLUDED.display_name,
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
        collection_count = EXCLUDED.collection_count;
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
                data["collection_count"]
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
        "cash": float(INITIAL_CASH),
        "pizza_coins": 0,
        "shops": {
            INITIAL_SHOP_NAME: {
                "level": 1,
                # Store last_collected_time within the shop dict (as before)
                # It will be saved as part of the shops JSONB
                "last_collected_time": time.time()
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
        "collection_count": 0
    }

# --- Income Calculation (Modified for stats) ---

def calculate_income_rate(shops: dict) -> float:
    total_rate = 0.0
    for name, shop_data in shops.items():
        level = shop_data.get("level", 1)
        base_multiplier = EXPANSION_LOCATIONS.get(name, (None, None, 1.0))[2] if name != INITIAL_SHOP_NAME else 1.0
        shop_rate = (BASE_INCOME_PER_SECOND * level) * base_multiplier
        total_rate += shop_rate
    return total_rate

def get_shop_income_rate(shop_name: str, level: int) -> float:
    """Calculates the income rate for a single shop at a specific level."""
    base_multiplier = EXPANSION_LOCATIONS.get(shop_name, (None, None, 1.0))[2] if shop_name != INITIAL_SHOP_NAME else 1.0
    shop_rate = (BASE_INCOME_PER_SECOND * level) * base_multiplier
    return shop_rate

def calculate_uncollected_income(player_data: dict) -> float:
    current_time = time.time()
    total_uncollected = 0.0
    shops = player_data.get("shops", {})
    for name, shop_data in shops.items():
        level = shop_data.get("level", 1)
        last_collected = shop_data.get("last_collected_time", current_time)
        time_diff = max(0, current_time - last_collected)
        base_multiplier = EXPANSION_LOCATIONS.get(name, (None, None, 1.0))[2] if name != INITIAL_SHOP_NAME else 1.0
        shop_rate = (BASE_INCOME_PER_SECOND * level) * base_multiplier
        total_uncollected += shop_rate * time_diff
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
        # Increment collection count ALWAYS before deciding outcome
        player_data["collection_count"] = player_data.get("collection_count", 0) + 1
        collection_count = player_data["collection_count"]
        logger.info(f"User {user_id} collection attempt #{collection_count}. Amount: ${uncollected:.2f}")

        # --- Check for Mafia Event --- #
        if collection_count > 0 and collection_count % 5 == 0:
            is_mafia_event = True
            # Calculate demand (e.g., 10-75% of uncollected)
            demand_percentage = random.uniform(0.10, 0.75)
            mafia_demand = round(uncollected * demand_percentage, 2)
            logger.info(f"Mafia event triggered for user {user_id}! Demand: ${mafia_demand:.2f} ({demand_percentage*100:.1f}%)")
            # DO NOT add cash yet. DO NOT check challenges/tips yet.
            # Save only the incremented collection count.
            save_player_data(user_id, player_data)
            return uncollected, [], is_mafia_event, mafia_demand
        else:
            # --- Normal Collection --- #
            player_data["cash"] = player_data.get("cash", 0) + uncollected
            player_data["total_income_earned"] = player_data.get("total_income_earned", 0) + uncollected
            player_data["stats"]["session_income"] = player_data["stats"].get("session_income", 0) + uncollected
            player_data["stats"]["session_collects"] = player_data["stats"].get("session_collects", 0) + 1

            current_time = time.time()
            for shop_name in player_data["shops"]:
                player_data["shops"][shop_name]["last_collected_time"] = current_time

            completed_challenges = update_challenge_progress(player_data, ["session_income", "session_collects"])
            save_player_data(user_id, player_data)
            return uncollected, completed_challenges, is_mafia_event, mafia_demand
    else:
        # Nothing to collect, still return structure
        return 0.0, [], False, None

# --- Upgrade & Expansion Logic (Modified for failure chance) ---

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

    for name, (req_type, req_value, _income_mult, _cost_scale) in EXPANSION_LOCATIONS.items():
        if name in owned_shops:
            continue
        met_requirement = False
        if req_type == "level":
            if initial_shop_level >= req_value:
                met_requirement = True
        elif req_type == "total_income":
            if total_income >= req_value:
                met_requirement = True
        if met_requirement:
            available.append(name)
    return available

def expand_shop(user_id: int, expansion_name: str) -> tuple[bool, str, list[str]]:
    """Attempts to establish a new shop. Returns (success, message, completed_challenge_messages)."""
    player_data = load_player_data(user_id)
    available_expansions = get_available_expansions(player_data)
    completed_challenges = []

    if expansion_name not in EXPANSION_LOCATIONS:
         return False, f"{expansion_name} is not a valid expansion location.", []

    if expansion_name in player_data["shops"]:
        return False, f"You already have a shop in {expansion_name}!", []

    if expansion_name not in available_expansions:
        req_type, req_value, _income_mult, _cost_scale = EXPANSION_LOCATIONS[expansion_name]
        if req_type == "level":
            return False, f"You can't expand to {expansion_name} yet. Requires {INITIAL_SHOP_NAME} to be Level {req_value}.", []
        elif req_type == "total_income":
             return False, f"You can't expand to {expansion_name} yet. Requires ${req_value:,.2f} total income earned.", []
        elif req_type == "shops_count":
             owned_count = len(player_data.get("shops", {}))
             return False, f"You can't expand to {expansion_name} yet. Requires {req_value} total shops (you have {owned_count}).", []
        else:
             return False, f"You don't meet the requirements to expand to {expansion_name} yet.", []

    player_data["shops"][expansion_name] = {
        "level": 1,
        "last_collected_time": time.time()
    }
    player_data["stats"]["session_expansions"] = player_data["stats"].get("session_expansions", 0) + 1

    # Check challenges after updating stats
    completed_challenges = update_challenge_progress(player_data, ["session_expansions"])

    save_player_data(user_id, player_data)
    income_rate = calculate_income_rate(player_data["shops"])
    msg = f"Congratulations! You've expanded your pizza empire to {expansion_name}! New total income rate: ${income_rate:.2f}/sec."
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

# --- Status Formatting ---

def format_status(player_data: dict) -> str:
    user_id = player_data.get("user_id", "Unknown")
    cash = player_data.get("cash", 0)
    pizza_coins = player_data.get("pizza_coins", 0)
    shops = player_data.get("shops", {})
    total_income_earned = player_data.get("total_income_earned", 0)
    title = player_data.get("current_title", None)
    achievements_unlocked = len(player_data.get("unlocked_achievements", []))

    title_str = f" Title: &lt;{title}&gt;" if title else ""

    status_lines = [
        f"<b>--- Player Status (ID: {user_id}{title_str}) ---</b>",
        f"<b>Cash:</b> ${cash:,.2f}",
        f"<b>Pizza Coins:</b> {pizza_coins:,} 🍕",
        f"<b>Total Income Earned:</b> ${total_income_earned:,.2f}",
        f"<b>Achievements Unlocked:</b> {achievements_unlocked}",
        "<b>Shops:</b>"
    ]
    if not shops:
        status_lines.append("  None yet! Use /start")
    else:
        for name, data in sorted(shops.items()):
            level = data.get("level", 1)
            upgrade_cost = get_upgrade_cost(level, name)
            status_lines.append(f"  - <b>{name}:</b> Level {level} (Upgrade Cost: ${upgrade_cost:,.2f})")

    income_rate = calculate_income_rate(shops)
    status_lines.append(f"<b>Current Income Rate:</b> ${income_rate:.2f}/sec")

    uncollected_income = calculate_uncollected_income(player_data)
    status_lines.append(f"<b>Uncollected Income:</b> ${uncollected_income:.2f} (Use /collect)")

    available_expansions = get_available_expansions(player_data)
    status_lines.append("<b>Available Expansions:</b>")
    if available_expansions:
        for loc in available_expansions:
             req_type, req_value, _income_mult, _cost_scale = EXPANSION_LOCATIONS[loc]
             req_str = f"(Req: {INITIAL_SHOP_NAME} Lvl {req_value})" if req_type == "level" else f"(Req: Total Earned ${req_value:,.2f})" if req_type == "total_income" else f"(Req: {req_value} Shops)"
             status_lines.append(f"  - {loc} {req_str} - Use /expand {loc.lower()}")
    else:
        status_lines.append("  None available right now. Keep upgrading!")

    # Display Challenges
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
