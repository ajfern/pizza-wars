import json
import os
import time
import random
from pathlib import Path
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DATA_DIR = Path("player_data")
DATA_DIR.mkdir(exist_ok=True)

# --- Game Constants ---
INITIAL_CASH = 10
INITIAL_SHOP_NAME = "Brooklyn"
BASE_INCOME_PER_SECOND = 0.1
BASE_UPGRADE_COST = 50
UPGRADE_COST_MULTIPLIER = 1.5

EXPANSION_LOCATIONS = {
    "Manhattan": ("level", 5, 2.0),
    "Queens": ("level", 10, 3.0),
    "Albany": ("total_income", 10000, 5.0),
}

# --- Achievement Definitions ---
# ID: (Name, Description, Check Function Args, Requirement, Reward Type, Reward Value, Title Awarded)
# Check Function Args: Tuple defining what metric to check (e.g., ('total_income',), ('shops_count',))
ACHIEVEMENTS = {
    "income_1k": ("Pizza Mogul", "Earn $1,000 total", ('total_income_earned',), 1000, 'cash', 100, "Mogul"),
    "income_10k": ("Pizza Tycoon", "Earn $10,000 total", ('total_income_earned',), 10000, 'pizza_coins', 50, "Tycoon"),
    "income_100k": ("Pizza Baron", "Earn $100,000 total", ('total_income_earned',), 100000, 'pizza_coins', 250, "Baron"),
    "shops_3": ("City Spreader", "Own 3 shops", ('shops_count',), 3, 'cash', 500, "City Spreader"),
    "shops_5": ("Empire Builder", "Own 5 shops", ('shops_count',), 5, 'pizza_coins', 100, "Empire Builder"),
    "brooklyn_10": ("Brooklyn Boss", "Upgrade Brooklyn to Level 10", ('shop_level', INITIAL_SHOP_NAME), 10, 'cash', 2000, "Brooklyn Boss"),
    # Add more achievements: rivals defeated (requires rival logic), specific shop levels, etc.
}

# --- Challenge Definitions ---
# Type: (Description Template, Metric, Timescale ('daily', 'weekly'), Base Goal, Goal Increase Per Level (approx), Reward Type, Base Reward, Reward Increase Per Level)
CHALLENGE_TYPES = {
    "earn_cash": ("Earn ${goal:,.2f} {timescale}", "session_income", None, 100, 1.5, 'cash', 50, 1.5),
    "upgrade_shops": ("Upgrade {goal} shops {timescale}", "session_upgrades", None, 1, 1.2, 'pizza_coins', 10, 1.3),
    "collect_times": ("Collect income {goal} times {timescale}", "session_collects", None, 3, 1.1, 'cash', 20, 1.2),
    # Add more types later (e.g., expand shops, reach level X)
}

# --- Player Data Management ---

def get_player_data_path(user_id: int) -> Path:
    """Returns the path to the player's data file."""
    return DATA_DIR / f"{user_id}.json"

def load_player_data(user_id: int) -> dict:
    """Loads player data from JSON file. Returns default if not found."""
    filepath = get_player_data_path(user_id)
    logger.debug(f"Attempting to load data for {user_id} from {filepath}")
    if filepath.exists():
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            logger.debug(f"Successfully loaded raw data for {user_id}.")
            # --- Migration for new fields --- #
            if "unlocked_achievements" not in data:
                data["unlocked_achievements"] = []
            if "current_title" not in data:
                data["current_title"] = None
            if "active_challenges" not in data:
                data["active_challenges"] = {"daily": None, "weekly": None}
            if "challenge_progress" not in data:
                data["challenge_progress"] = {"daily": {}, "weekly": {}}
            if "stats" not in data: # Add general stats tracking
                 data["stats"] = {
                     "session_income": 0,
                     "session_upgrades": 0,
                     "session_collects": 0,
                 }
            # --- End Migration --- #
            return data
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Error loading data for {user_id}: {e}. Returning default state.")
            return get_default_player_state(user_id)
    else:
        logger.info(f"No data file found for {user_id}. Returning default state.")
        return get_default_player_state(user_id)

def save_player_data(user_id: int, data: dict) -> None:
    """Saves player data to JSON file."""
    filepath = get_player_data_path(user_id)
    logger.debug(f"Attempting to save data for {user_id} to {filepath}")
    try:
        # Ensure consistent structure before saving
        data.setdefault("unlocked_achievements", [])
        data.setdefault("current_title", None)
        data.setdefault("active_challenges", {"daily": None, "weekly": None})
        data.setdefault("challenge_progress", {"daily": {}, "weekly": {}})
        data.setdefault("stats", {})
        data["stats"].setdefault("session_income", 0)
        data["stats"].setdefault("session_upgrades", 0)
        data["stats"].setdefault("session_collects", 0)

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        logger.debug(f"Successfully saved data for {user_id}.")
    except OSError as e:
        logger.error(f"Error saving data for {user_id} to {filepath}: {e}")

def get_default_player_state(user_id: int) -> dict:
    """Returns the initial state for a new player."""
    logger.info(f"Creating default state for user {user_id}")
    return {
        "user_id": user_id,
        "cash": INITIAL_CASH,
        "pizza_coins": 0,
        "shops": {
            INITIAL_SHOP_NAME: {
                "level": 1,
                "last_collected_time": time.time()
            }
        },
        "unlocked_expansions": list(EXPANSION_LOCATIONS.keys()),
        "total_income_earned": 0,
        "last_login_time": time.time(),
        # Achievements & Challenges
        "unlocked_achievements": [], # List of achievement IDs
        "current_title": None, # Highest unlocked title
        "active_challenges": { # Current challenges {timescale: challenge_data}
            "daily": None,
            "weekly": None
        },
        "challenge_progress": { # Current progress {timescale: {metric: value}}
            "daily": {},
            "weekly": {}
        },
        "stats": { # Stats tracked for challenges/achievements within a period (reset with challenges)
            "session_income": 0,
            "session_upgrades": 0,
            "session_collects": 0,
        }
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

def collect_income(user_id: int) -> tuple[float, list[str]]:
    """Collects income, updates player data, tracks stats, and returns (amount, completed_challenge_messages)."""
    player_data = load_player_data(user_id)
    uncollected = calculate_uncollected_income(player_data)
    completed_challenges = []

    if uncollected > 0.01:
        player_data["cash"] = player_data.get("cash", 0) + uncollected
        player_data["total_income_earned"] = player_data.get("total_income_earned", 0) + uncollected
        player_data["stats"]["session_income"] = player_data["stats"].get("session_income", 0) + uncollected
        player_data["stats"]["session_collects"] = player_data["stats"].get("session_collects", 0) + 1

        current_time = time.time()
        for shop_name in player_data["shops"]:
             player_data["shops"][shop_name]["last_collected_time"] = current_time

        # Check challenges after updating stats
        completed_challenges = update_challenge_progress(player_data, ["session_income", "session_collects"])

        save_player_data(user_id, player_data)
        return uncollected, completed_challenges
    else:
        return 0.0, []

# --- Upgrade & Expansion Logic (Modified for stats) ---

def get_upgrade_cost(current_level: int) -> float:
    return BASE_UPGRADE_COST * (UPGRADE_COST_MULTIPLIER ** (current_level - 1))

def upgrade_shop(user_id: int, shop_name: str) -> tuple[bool, str, list[str]]:
    """Attempts to upgrade a shop. Returns (success, message, completed_challenge_messages)."""
    player_data = load_player_data(user_id)
    shops = player_data.get("shops", {})
    completed_challenges = []

    if shop_name not in shops:
        return False, f"You don't own a shop in {shop_name}!", []

    current_level = shops[shop_name].get("level", 1)
    cost = get_upgrade_cost(current_level)
    cash = player_data.get("cash", 0)

    if cash < cost:
        return False, f"Not enough cash! Need ${cost:.2f} to upgrade {shop_name} to level {current_level + 1}. You have ${cash:.2f}.", []

    player_data["cash"] = cash - cost
    player_data["shops"][shop_name]["level"] = current_level + 1
    player_data["stats"]["session_upgrades"] = player_data["stats"].get("session_upgrades", 0) + 1

    # Check challenges after updating stats
    completed_challenges = update_challenge_progress(player_data, ["session_upgrades"])

    save_player_data(user_id, player_data)

    income_rate = calculate_income_rate(player_data["shops"])
    msg = f"Successfully upgraded {shop_name} to Level {current_level + 1}! Cost: ${cost:.2f}. New total income rate: ${income_rate:.2f}/sec."
    return True, msg, completed_challenges

def get_available_expansions(player_data: dict) -> list[str]:
    available = []
    owned_shops = player_data.get("shops", {})
    initial_shop_level = owned_shops.get(INITIAL_SHOP_NAME, {}).get("level", 1)
    total_income = player_data.get("total_income_earned", 0)

    for name, (req_type, req_value, _) in EXPANSION_LOCATIONS.items():
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

def expand_shop(user_id: int, expansion_name: str) -> tuple[bool, str]:
    """Attempts to establish a new shop in an expansion location. Returns (success, message)."""
    # Note: Expansion itself doesn't directly complete challenges in this design
    # but opening shops contributes to achievement checks later.
    player_data = load_player_data(user_id)
    available_expansions = get_available_expansions(player_data)

    if expansion_name not in EXPANSION_LOCATIONS:
         return False, f"{expansion_name} is not a valid expansion location."

    if expansion_name in player_data["shops"]:
        return False, f"You already have a shop in {expansion_name}!"

    if expansion_name not in available_expansions:
        req_type, req_value, _ = EXPANSION_LOCATIONS[expansion_name]
        if req_type == "level":
            return False, f"You can't expand to {expansion_name} yet. Requires {INITIAL_SHOP_NAME} to be Level {req_value}."
        elif req_type == "total_income":
             return False, f"You can't expand to {expansion_name} yet. Requires ${req_value:,.2f} total income earned."
        else:
             return False, f"You don't meet the requirements to expand to {expansion_name} yet."

    player_data["shops"][expansion_name] = {
        "level": 1,
        "last_collected_time": time.time()
    }
    save_player_data(user_id, player_data)
    income_rate = calculate_income_rate(player_data["shops"])
    return True, f"Congratulations! You've expanded your pizza empire to {expansion_name}! New total income rate: ${income_rate:.2f}/sec."

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
    player_data = load_player_data(user_id)
    player_level = len(player_data.get("unlocked_achievements", [])) # Use achievement count as proxy for level

    # Choose a random challenge type
    challenge_type_id = random.choice(list(CHALLENGE_TYPES.keys()))
    desc_template, metric, _, base_goal, goal_mult, reward_type, base_reward, reward_mult = CHALLENGE_TYPES[challenge_type_id]

    # Scale goal and reward based on player level (simple example)
    goal = int(base_goal * (goal_mult ** player_level))
    reward_value = int(base_reward * (reward_mult ** player_level))

    # Prevent excessively easy goals
    if "cash" in metric and goal < 100: goal = 100
    if "upgrade" in metric and goal < 1: goal = 1
    if "collect" in metric and goal < 2: goal = 2

    description = desc_template.format(goal=goal, timescale=timescale)

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

    logger.info(f"Generated new {timescale} challenge for user {user_id}: {description} (Goal: {goal} {metric}, Reward: {reward_value} {reward_type})")
    save_player_data(user_id, player_data)

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
                msg = f"🎉 Challenge Complete! 🎉\n\"{challenge['description']}\"\nReward: {reward_value} {reward_type.upper()}!"

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

    title_str = f" Title: <{title}>" if title else ""

    status_lines = [
        f"<b>--- Player Status (ID: {user_id}{title_str}) ---</b>",
        f"<b>Cash:</b> ${cash:,.2f}",
        f"<b>Pizza Coins:</b> {pizza_coins} 🍕",
        f"<b>Total Income Earned:</b> ${total_income_earned:,.2f}",
        f"<b>Achievements Unlocked:</b> {achievements_unlocked}",
        "<b>Shops:</b>"
    ]
    if not shops:
        status_lines.append("  None yet! Use /start")
    else:
        for name, data in sorted(shops.items()):
            level = data.get("level", 1)
            upgrade_cost = get_upgrade_cost(level)
            status_lines.append(f"  - <b>{name}:</b> Level {level} (Upgrade Cost: ${upgrade_cost:,.2f})")

    income_rate = calculate_income_rate(shops)
    status_lines.append(f"<b>Current Income Rate:</b> ${income_rate:.2f}/sec")

    uncollected_income = calculate_uncollected_income(player_data)
    status_lines.append(f"<b>Uncollected Income:</b> ${uncollected_income:.2f} (Use /collect)")

    available_expansions = get_available_expansions(player_data)
    status_lines.append("<b>Available Expansions:</b>")
    if available_expansions:
        for loc in available_expansions:
             req_type, req_value, mult = EXPANSION_LOCATIONS[loc]
             req_str = f"(Req: {INITIAL_SHOP_NAME} Lvl {req_value})" if req_type == "level" else f"(Req: Total Earned ${req_value:,.2f})"
             status_lines.append(f"  - {loc} {req_str} - Use /expand {loc}")
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
