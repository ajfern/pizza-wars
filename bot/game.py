import json
import os
import time
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

DATA_DIR = Path("player_data")
DATA_DIR.mkdir(exist_ok=True)  # Ensure directory exists

# --- Game Constants ---
INITIAL_CASH = 10
INITIAL_SHOP_NAME = "Brooklyn"
BASE_INCOME_PER_SECOND = 0.1  # Income per second for a level 1 shop
BASE_UPGRADE_COST = 50
UPGRADE_COST_MULTIPLIER = 1.5 # Cost increases by 50% each level

# Expansion definitions (Location Name: (Requirement Type, Requirement Value, Base Income Multiplier))
# Requirement Type: 'level' (shop level), 'total_income' (total income earned)
EXPANSION_LOCATIONS = {
    "Manhattan": ("level", 5, 2.0),      # Unlocks when Brooklyn reaches level 5
    "Queens": ("level", 10, 3.0),     # Unlocks when Brooklyn reaches level 10
    "Albany": ("total_income", 10000, 5.0), # Unlocks after earning 10k total
    # Add more state/regional/national/international later
}

# --- Player Data Management ---

def get_player_data_path(user_id: int) -> Path:
    """Returns the path to the player's data file."""
    return DATA_DIR / f"{user_id}.json"

def load_player_data(user_id: int) -> dict:
    """Loads player data from JSON file. Returns default if not found."""
    filepath = get_player_data_path(user_id)
    if filepath.exists():
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
                # Add migration logic here if data structure changes later
                return data
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Error loading data for {user_id}: {e}")
            # Fallback to default data in case of error
            return get_default_player_state(user_id)
    else:
        return get_default_player_state(user_id)

def save_player_data(user_id: int, data: dict) -> None:
    """Saves player data to JSON file."""
    filepath = get_player_data_path(user_id)
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
    except OSError as e:
        logger.error(f"Error saving data for {user_id}: {e}")

def get_default_player_state(user_id: int) -> dict:
    """Returns the initial state for a new player."""
    return {
        "user_id": user_id,
        "cash": INITIAL_CASH,
        "pizza_coins": 0, # Premium currency
        "shops": {
            INITIAL_SHOP_NAME: {
                "level": 1,
                "last_collected_time": time.time()
            }
        },
        "unlocked_expansions": list(EXPANSION_LOCATIONS.keys()), # For checking availability
        "total_income_earned": 0,
        "last_login_time": time.time(),
        # Add more fields as needed
    }

# --- Income Calculation ---

def calculate_income_rate(shops: dict) -> float:
    """Calculates the total income generated per second across all shops."""
    total_rate = 0.0
    for name, shop_data in shops.items():
        level = shop_data.get("level", 1)
        # Base income increases with level (e.g., linear or exponential)
        base_multiplier = EXPANSION_LOCATIONS.get(name, (None, None, 1.0))[2] if name != INITIAL_SHOP_NAME else 1.0
        shop_rate = (BASE_INCOME_PER_SECOND * level) * base_multiplier
        total_rate += shop_rate
    return total_rate

def calculate_uncollected_income(player_data: dict) -> float:
    """Calculates the income accumulated since the last collection for all shops."""
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

def collect_income(user_id: int) -> float:
    """Collects income, updates player data, and returns the amount collected."""
    player_data = load_player_data(user_id)
    uncollected = calculate_uncollected_income(player_data)

    if uncollected > 0.01: # Only collect if there's a meaningful amount
        player_data["cash"] = player_data.get("cash", 0) + uncollected
        player_data["total_income_earned"] = player_data.get("total_income_earned", 0) + uncollected
        current_time = time.time()
        # Update last_collected_time for all shops
        for shop_name in player_data["shops"]:
             player_data["shops"][shop_name]["last_collected_time"] = current_time
        save_player_data(user_id, player_data)
        return uncollected
    else:
        return 0.0

# --- Upgrade & Expansion Logic ---

def get_upgrade_cost(current_level: int) -> float:
    """Calculates the cost to upgrade to the next level."""
    # Example: Exponential cost increase
    return BASE_UPGRADE_COST * (UPGRADE_COST_MULTIPLIER ** (current_level - 1))

def upgrade_shop(user_id: int, shop_name: str) -> tuple[bool, str]:
    """Attempts to upgrade a shop. Returns (success, message)."""
    player_data = load_player_data(user_id)
    shops = player_data.get("shops", {})

    if shop_name not in shops:
        return False, f"You don't own a shop in {shop_name}!"

    current_level = shops[shop_name].get("level", 1)
    cost = get_upgrade_cost(current_level)
    cash = player_data.get("cash", 0)

    if cash < cost:
        return False, f"Not enough cash! Need ${cost:.2f} to upgrade {shop_name} to level {current_level + 1}. You have ${cash:.2f}."

    # Deduct cost, increase level, update collection time (optional)
    player_data["cash"] = cash - cost
    player_data["shops"][shop_name]["level"] = current_level + 1
    # Maybe reset collection time or give partial credit?
    # player_data["shops"][shop_name]["last_collected_time"] = time.time()
    save_player_data(user_id, player_data)

    income_rate = calculate_income_rate(player_data["shops"]) # Recalculate after upgrade
    return True, f"Successfully upgraded {shop_name} to Level {current_level + 1}! Cost: ${cost:.2f}. New total income rate: ${income_rate:.2f}/sec."

def get_available_expansions(player_data: dict) -> list[str]:
    """Checks which expansion locations the player meets the requirements for."""
    available = []
    owned_shops = player_data.get("shops", {})
    initial_shop_level = owned_shops.get(INITIAL_SHOP_NAME, {}).get("level", 1)
    total_income = player_data.get("total_income_earned", 0)

    for name, (req_type, req_value, _) in EXPANSION_LOCATIONS.items():
        if name in owned_shops: # Already own it
            continue

        met_requirement = False
        if req_type == "level":
            # For now, assume level requirements are based on the initial shop
            if initial_shop_level >= req_value:
                met_requirement = True
        elif req_type == "total_income":
            if total_income >= req_value:
                met_requirement = True

        if met_requirement:
            available.append(name)

    return available

def expand_shop(user_id: int, expansion_name: str) -> tuple[bool, str]:
    """Attempts to establish a new shop in an expansion location."""
    player_data = load_player_data(user_id)
    available_expansions = get_available_expansions(player_data)

    if expansion_name not in EXPANSION_LOCATIONS:
         return False, f"{expansion_name} is not a valid expansion location."

    if expansion_name in player_data["shops"]:
        return False, f"You already have a shop in {expansion_name}!"

    if expansion_name not in available_expansions:
        # Provide specific requirement info
        req_type, req_value, _ = EXPANSION_LOCATIONS[expansion_name]
        if req_type == "level":
            return False, f"You can't expand to {expansion_name} yet. Requires {INITIAL_SHOP_NAME} to be Level {req_value}."
        elif req_type == "total_income":
             return False, f"You can't expand to {expansion_name} yet. Requires ${req_value:,.2f} total income earned."
        else:
             return False, f"You don't meet the requirements to expand to {expansion_name} yet."

    # Add the new shop at level 1
    player_data["shops"][expansion_name] = {
        "level": 1,
        "last_collected_time": time.time() # Start earning immediately
    }
    save_player_data(user_id, player_data)
    income_rate = calculate_income_rate(player_data["shops"]) # Recalculate after expansion
    return True, f"Congratulations! You've expanded your pizza empire to {expansion_name}! New total income rate: ${income_rate:.2f}/sec."

# --- Status Formatting ---

def format_status(player_data: dict) -> str:
    """Formats the player's status into a readable string."""
    user_id = player_data.get("user_id", "Unknown")
    cash = player_data.get("cash", 0)
    pizza_coins = player_data.get("pizza_coins", 0)
    shops = player_data.get("shops", {})
    total_income_earned = player_data.get("total_income_earned", 0)

    status_lines = [
        f"<b>--- Player Status (ID: {user_id}) ---</b>",
        f"<b>Cash:</b> ${cash:,.2f}",
        f"<b>Pizza Coins:</b> {pizza_coins} 🍕",
        f"<b>Total Income Earned:</b> ${total_income_earned:,.2f}",
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

    # Placeholder for premium features
    status_lines.append("\n<i>Use Pizza Coins 🍕 to speed things up! (Coming Soon)</i>")

    return "\n".join(status_lines)

# --- Placeholder for Premium Currency/Payments ---
# This section would interact with Telegram Payments API

def buy_pizza_coins(user_id: int, amount: int):
    # 1. Initiate payment request via Telegram API
    # 2. On successful payment confirmation (webhook/callback):
    #    player_data = load_player_data(user_id)
    #    player_data["pizza_coins"] = player_data.get("pizza_coins", 0) + amount
    #    save_player_data(user_id, player_data)
    logger.info(f"Placeholder: User {user_id} attempting to buy {amount} Pizza Coins.")
    return "Pizza Coin purchases are coming soon!"

def use_pizza_coins_for_speedup(user_id: int, feature: str):
    # 1. Check if player has enough Pizza Coins
    # 2. Deduct coins
    # 3. Apply effect (e.g., instant collection, finish upgrade)
    # 4. Save data
    logger.info(f"Placeholder: User {user_id} attempting to use Pizza Coins for {feature}.")
    player_data = load_player_data(user_id)
    if player_data.get("pizza_coins", 0) > 0:
        return "Spending Pizza Coins is coming soon!"
    else:
        return "You don't have any Pizza Coins! Purchases are coming soon."
