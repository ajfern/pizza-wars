import logging
import os
import glob # For finding player data files
import random # For tips!
import time # <<< Added missing import
import re # For sanitization
import html # For escaping
from datetime import time as dt_time, timedelta, datetime, timezone

# Telegram Core Types
from telegram import Update, LabeledPrice, ShippingOption, Invoice, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
# Telegram Constants & Filters
from telegram.constants import ChatAction # Maybe useful later?
# Telegram Extensions
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    PreCheckoutQueryHandler,
    ShippingQueryHandler,
    CallbackQueryHandler
)

# Scheduling
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Import game logic functions
import game # Corrected import

# --- Change Log & Version --- #
# Manually update this list (newest first) and the version before deploying changes.
CHANGE_LOG_ENTRIES = [
    "**Sabotage Overhaul:** Sending agents is riskier! Costs more ($1k + 5% cash), lower success (40%), 25% backfire chance, 15-min cooldown.",
    "**Leaderboards Combined:** `/leaderboard` now shows both Total Income & Current Cash leaderboards.",
    "**Dynamic Performance:** Location income now fluctuates daily! Check `/status` or `/expand` for current multipliers (📈/📉/🤷‍♂️).",
    "**Expansion Costs:** Expanding now costs money, scaling with location potential.",
    "**Shop Naming:** Give shops custom names with `/renameshop [Location] [New Name]`!",
    "**Mafia Event:** Watch out on your 5th collection! You might get a visit...",
    "**Upgrade Failure:** Upgrades now have a 15% chance to fail (costing you dough!).",
    # Add new entries above this line
]
# Use a date format like YYYYMMDD or a simple version number
CURRENT_SUMMARY_VERSION = "20240406.1"

# Enable logging (ensure logger is configured as before)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING) # Keep scheduler logs quieter
logger = logging.getLogger(__name__)

# --- Bot Tokens & Config ---
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
PAYMENT_PROVIDER_TOKEN = os.getenv("PAYMENT_PROVIDER_TOKEN")

if not BOT_TOKEN:
    logger.critical("TELEGRAM_BOT_TOKEN environment variable not set! Exiting.")
    exit()

if not PAYMENT_PROVIDER_TOKEN:
    logger.warning("PAYMENT_PROVIDER_TOKEN environment variable not set! Payments will fail.")

# Initialize Database Schema & Seed Performance Data
try:
    logger.info("Initializing database...")
    game.initialize_database()
    logger.info("Seeding/Updating location performance data...")
    game.update_location_performance() # Ensure this runs on startup
    logger.info("Database init and performance seeding complete.")
except ConnectionError as e:
    logger.critical(f"Database connection failed on startup: {e}. Exiting.")
    exit()
except Exception as e:
    logger.critical(f"Unexpected error during database setup: {e}. Exiting.", exc_info=True)
    exit()

# Global Scheduler instance
scheduler = AsyncIOScheduler(timezone="UTC") # Use UTC for consistency

# --- Helper Functions ---
async def check_and_notify_achievements(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Checks for new achievements and sends notifications."""
    try:
        newly_unlocked = game.check_achievements(user_id)
        for name, desc, title in newly_unlocked:
            title_msg = f" You've earned the title: <{title}>!" if title else ""
            await context.bot.send_message(
                chat_id=user_id,
                text=f"🏆 Achievement Unlocked! 🏆\n<b>{name}</b>: {desc}{title_msg}\n<i>Share your success!</i>",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Error checking/notifying achievements for {user_id}: {e}", exc_info=True)

async def send_challenge_notifications(user_id: int, messages: list[str], context: ContextTypes.DEFAULT_TYPE):
    """Sends messages about completed challenges."""
    if not messages:
        return
    try:
        for msg in messages:
            await context.bot.send_message(chat_id=user_id, text=msg)
    except Exception as e:
        logger.error(f"Error sending challenge notification to {user_id}: {e}", exc_info=True)

# --- Helper Function to send summary (Modified) ---
async def send_change_summary(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Sending change summary version {CURRENT_SUMMARY_VERSION} to user {user_id}")
    try:
        if not CHANGE_LOG_ENTRIES:
            logger.warning("CHANGE_LOG_ENTRIES list is empty, cannot send summary.")
            return

        # Format the message
        summary_points = "\n".join([f"*   {entry}" for entry in CHANGE_LOG_ENTRIES])
        # Use an appropriate date or leave as version string
        today_date = datetime.now(timezone.utc).strftime("%B %d, %Y")
        full_message = (
            f"📢 <b>Updates to Pizza Wars for {today_date} ({CURRENT_SUMMARY_VERSION}):</b>\n\n"
            f"{summary_points}\n\n"
            f"<i>Keep building that empire!</i>"
        )

        await context.bot.send_message(chat_id=user_id, text=full_message, parse_mode="HTML")

        # Update player's seen version in DB
        player_data = game.load_player_data(user_id)
        if player_data:
            player_data["last_summary_seen_version"] = CURRENT_SUMMARY_VERSION
            game.save_player_data(user_id, player_data)
        else:
             logger.warning(f"Could not load player data for {user_id} after sending summary to update seen version.")
    except Exception as e:
        logger.error(f"Error sending change summary to {user_id}: {e}", exc_info=True)

# --- Utility function to update name ---
async def update_player_display_name(user_id: int, user: "telegram.User | None"):
    """Helper to call the game logic update function."""
    if user:
        game.update_display_name(user_id, user)

# --- NEW Helper to show upgrade options ---
async def _show_upgrade_options(update_or_query: Update | CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    """Fetches player shops and displays them as buttons for upgrading."""
    user = update_or_query.from_user
    chat_id = update_or_query.message.chat_id if hasattr(update_or_query, 'message') else None
    if not chat_id and hasattr(update_or_query, 'chat_instance'): # Fallback for query without message?
        chat_id = user.id # DM
    if not chat_id:
         logger.warning("_show_upgrade_options could not determine chat_id")
         return

    logger.debug(f"Showing upgrade options for user {user.id}")
    player_data = game.load_player_data(user.id)
    shops = player_data.get("shops", {})

    if not shops:
         await context.bot.send_message(chat_id=chat_id, text="You ain't got no shops to upgrade yet!")
         return

    keyboard = []
    shop_list = sorted(shops.items(), key=lambda item: game.get_upgrade_cost(item[1].get('level', 1), item[0])) # Sort by current upgrade cost asc

    for location, shop_data in shop_list:
        level = shop_data.get("level", 1)
        cost = game.get_upgrade_cost(level, location)
        custom_name = shop_data.get("custom_name", location)
        display_name = f"{custom_name} ({location})" if custom_name != location else location
        # Button shows Shop Name (Lvl X) - Cost $Y
        button_text = f"{display_name} (Lvl {level}) - ${cost:,.0f}"
        # Callback data format: upgrade_shop_{location_name}
        callback_data = f"upgrade_shop_{location}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        # One button per row

    if not keyboard:
        await context.bot.send_message(chat_id=chat_id, text="No shops available to upgrade right now.") # Should not happen if shops exist
        return

    reply_markup = InlineKeyboardMarkup(keyboard)
    # If called from a CallbackQuery, edit the message, otherwise send new
    if isinstance(update_or_query, CallbackQuery):
        try:
             await update_or_query.edit_message_text("🤌 Which shop needs some love (and cash)?", reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"Failed to edit message for upgrade options: {e}")
            # Fallback: Send new message if edit fails
            await context.bot.send_message(chat_id=chat_id, text="🤌 Which shop needs some love (and cash)?", reply_markup=reply_markup)
    else: # Called from /upgrade command
        await context.bot.send_message(chat_id=chat_id, text="🤌 Which shop needs some love (and cash)?", reply_markup=reply_markup)


# --- NEW Helper to process upgrade attempt ---
async def _process_upgrade(context: ContextTypes.DEFAULT_TYPE, user_id: int, shop_location: str, query: CallbackQuery | None = None):
    """Handles the core logic of attempting an upgrade."""
    logger.info(f"Processing upgrade attempt for user {user_id}, shop '{shop_location}'")
    try:
        # Need attacker data for display name fallback on failure message
        attacker_data = game.load_player_data(user_id)
        shops = attacker_data.get("shops", {})
        if shop_location not in shops:
             # This check might be redundant if called from button, but good safety
             raise ValueError(f"Shop {shop_location} not found for user {user_id}")

        current_level = shops[shop_location].get("level", 1)
        success, result_data, completed_challenges = game.upgrade_shop(user.id, shop_location)

        outcome_message = ""
        if success:
            new_level = result_data
            fun_messages = [
                f"🍾 Hot dang! Your {shop_location} spot just hit Level {new_level}. Lines around the block incoming!",
                f"🤌 Mama mia! {shop_location} is now Level {new_level}! More dough, less problems!",
                f"🎉 Level {new_level} for {shop_location}! You're cookin' with gas now!"
            ]
            outcome_message = random.choice(fun_messages)
        else:
            failure_message = result_data
            if "Not enough cash" in failure_message:
                 outcome_message = failure_message
            else:
                 cost_lost_str = "the cost"
                 try: cost_lost_str = failure_message.split("lost ")[-1].split(" in")[0]
                 except IndexError: logger.warning(f"Could not parse cost from failure message: {failure_message}")
                 failure_messages = [
                      f"💥 KABOOM! Contractors messed up! Upgrade failed, {cost_lost_str} went up in smoke!",
                      f"😱 Mamma Mia! Sinkhole swallowed the crew! Upgrade failed, dough gone ({cost_lost_str})!",
                      f"📉 Bad investment! {shop_location} upgrade flopped. Lost {cost_lost_str}!",
                      f"🔥 Grease fire! Upgrade went belly-up. Kiss {cost_lost_str} goodbye!"
                 ]
                 outcome_message = random.choice(failure_messages)

        # Send result: Edit message if from callback, else send new
        if query:
            await query.edit_message_text(text=outcome_message, parse_mode="HTML")
        else:
             await context.bot.send_message(chat_id=user_id, text=outcome_message, parse_mode="HTML")

        # Post-action checks
        if success:
            await send_challenge_notifications(user_id, completed_challenges, context)
            await check_and_notify_achievements(user_id, context)

    except Exception as e:
        logger.error(f"Error during _process_upgrade for {user_id}, shop {shop_location}: {e}", exc_info=True)
        error_msg = "Ay caramba! Somethin' went wrong with the upgrade."
        if query:
             try: await query.edit_message_text(text=error_msg)
             except Exception: await context.bot.send_message(chat_id=user_id, text=error_msg)
        else:
             await context.bot.send_message(chat_id=user_id, text=error_msg)


# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    logger.info("Entered start_command")
    if not user:
        logger.warning("start_command called without user info")
        return

    await update_player_display_name(user.id, user) # Update name first
    logger.info(f"User {user.id} ({user.username}) triggered /start.")

    try:
        logger.info(f"Loading player data for {user.id}...")
        player_data = game.load_player_data(user.id)
        if not player_data: # Handle potential load failure
             logger.error(f"Failed to load or initialize player data for {user.id} in start_command.")
             await update.message.reply_text("Sorry, couldn't retrieve your game data. Please try again.")
             return

        logger.info(f"Player data loaded for {user.id}.")

        # --- Check if summary needs to be shown --- #
        last_seen_version = player_data.get("last_summary_seen_version")
        if last_seen_version != CURRENT_SUMMARY_VERSION:
            await send_change_summary(user.id, context)
            # Update the local dict too, though save happens in send_change_summary
            player_data["last_summary_seen_version"] = CURRENT_SUMMARY_VERSION
        # --- End Summary Check --- #

        # --- Check if player seems new based on default data --- #
        # (e.g., exactly initial cash AND only the starting shop at level 1)
        is_likely_new = (
            player_data.get('total_income_earned', 0) < 0.01 and
            len(player_data.get('shops', {})) == 1 and
            game.INITIAL_SHOP_NAME in player_data.get('shops', {}) and
            player_data['shops'][game.INITIAL_SHOP_NAME].get('level') == 1
        )

        if is_likely_new:
             logger.info(f"Likely new player {user.id}, generating initial challenges.")
             # Ensure stats are reset correctly for new players before generating
             player_data['stats'] = {k: 0 for k in player_data.get('stats', {})} # Reset just in case
             game.save_player_data(user.id, player_data) # Save reset stats before generating
             # Generate challenges (will load/save again inside)
             game.generate_new_challenges(user.id, 'daily')
             game.generate_new_challenges(user.id, 'weekly')
             # Reload data to get generated challenges for the status message
             player_data = game.load_player_data(user.id)
             if not player_data: # Handle potential load failure after generation
                  logger.error(f"Failed to reload player data for {user.id} after challenge generation.")
                  await update.message.reply_text("Sorry, couldn't retrieve updated game data. Please try /status.")
                  return

        # --- Update login time and save --- #
        player_data["last_login_time"] = game.time.time()
        logger.info(f"Saving updated player data for {user.id}...")
        game.save_player_data(user.id, player_data) # Save login time etc.
        logger.info(f"Player data saved for {user.id}.")

        # --- Send Welcome & Initial Status --- #
        reply_message = (
            f"🍕 Ay-oh, Pizza Boss {user.mention_html()}! Welcome to Pizza Empire, where dough rules everything around me!\n\n"
            f"Here's how ya slice up the competition:\n"
            f"- Collect piles of cash automatically (because who has time for work?). Type /collect to scoop up your dough!\n"
            f"- Upgrade those pizza joints (/upgrade &lt;shop_location&gt;) to rake in more cheddar.\n"
            f"- Dominate from Brooklyn to the whole freakin' planet by hittin' big pizza milestones.\n\n"
            f"Now, get cookin', capisce? Check your /status!"
        )
        logger.info(f"Attempting to send welcome message to {user.id}...")
        await update.message.reply_html(reply_message)
        logger.info(f"Welcome message sent successfully to {user.id}.")

        # --- Show Status & Prompt for Name --- #
        logger.info(f"Sending initial status to player {user.id}")
        status_message = game.format_status(player_data)
        await update.message.reply_html(status_message) # Show initial status

        # Prompt for name if not set
        if not player_data.get("franchise_name"):
            await update.message.reply_text(
                """Looks like your empire doesn't have a name yet! Give it some pizzazz with:
/setname [Your Awesome Franchise Name]"""
            )
        # --- End Prompt --- #

        await check_and_notify_achievements(user.id, context)

    except Exception as e:
        logger.error(f"ERROR in start_command for user {user.id}: {e}", exc_info=True)
        await update.message.reply_text("Ay, somethin' went wrong gettin' ya started. Try /start again maybe?")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    await update_player_display_name(user.id, user)
    logger.info(f"User {user.id} requested status.")

    # --- Parse Sort Argument --- #
    sort_key = 'name' # Default sort
    if context.args:
        arg_lower = context.args[0].lower()
        if arg_lower.startswith('s:') or arg_lower.startswith('sort:'):
            potential_key = arg_lower.split(':', 1)[1]
            if potential_key in ['name', 'level', 'cost', 'upgrade_cost']:
                sort_key = potential_key
                logger.info(f"User {user.id} requested status sorted by: {sort_key}")
            else:
                 await update.message.reply_text(f"Unknown sort key '{potential_key}'. Use 'name', 'level', or 'cost'.")
                 # Defaulting to name sort
                 sort_key = 'name'
    # --- End Sort Argument --- #

    player_data = game.load_player_data(user.id)
    if not player_data:
         await update.message.reply_text("Couldn't load your data, boss. Try /start?")
         return
    status_message = game.format_status(player_data, sort_by=sort_key)

    # --- Create CORRECT Action Buttons --- #
    keyboard = [
        [
            InlineKeyboardButton("💰 Collect Income", callback_data="main_collect"),
            InlineKeyboardButton("⬆️ Upgrade Shop", callback_data="main_upgrade"),
        ],
        [
            InlineKeyboardButton("🗺️ Expand Empire", callback_data="main_expand"),
            InlineKeyboardButton("🎯 View Challenges", callback_data="main_challenges"),
        ],
        [
            InlineKeyboardButton("🏆 Leaderboard", callback_data="main_leaderboard"),
            InlineKeyboardButton("🍕 Buy Coins", callback_data="main_buycoins"),
        ],
        [
            InlineKeyboardButton("❓ Help Guide", callback_data="main_help"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    # --- End Action Buttons --- #

    await update.message.reply_html(status_message, reply_markup=reply_markup)

async def collect_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    await update_player_display_name(user.id, user)
    logger.info(f"User {user.id} requested collection.")

    try:
        # collect_income now returns: (collected_amount, completed_challenges, is_mafia_event, mafia_demand)
        collected_amount, completed_challenges, is_mafia_event, mafia_demand = game.collect_income(user.id)

        if is_mafia_event:
            # --- MAFIA EVENT --- # 
            if mafia_demand is None or mafia_demand <= 0:
                 logger.error(f"Mafia event triggered for {user.id} but demand is invalid: {mafia_demand}")
                 await update.message.reply_text("The usual collectors showed up, but seemed confused... they left empty-handed. Lucky break?")
                 return

            # Store necessary info for the callback handler
            context.user_data['mafia_collect_amount'] = collected_amount
            context.user_data['mafia_demand'] = mafia_demand
            logger.info(f"Storing user_data for Mafia event: collect={collected_amount}, demand={mafia_demand}")

            keyboard = [
                [
                    InlineKeyboardButton(f"🤌 Pay ${mafia_demand:,.2f}", callback_data="mafia_pay"),
                    InlineKeyboardButton("🙅‍♂️ Tell 'em Fuggedaboutit!", callback_data="mafia_refuse"),
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                f"🚨 Uh oh, boss! The Famiglia stopped by for their 'protection' fee.\n"
                f"They saw you collected ${collected_amount:,.2f} and demand **${mafia_demand:,.2f}**.\n\n"
                f"Whaddya wanna do?",
                reply_markup=reply_markup,
            )

        elif collected_amount > 0.01:
            # --- NORMAL COLLECTION (with tip/pineapple) --- #
            tip_message = ""
            pineapple_message = ""

            # "Just the Tip" Mechanic
            tip_chance = 0.15
            if random.random() < tip_chance:
                player_data = game.load_player_data(user.id)
                tip_amount = round(random.uniform(collected_amount * 0.05, collected_amount * 0.2) + random.uniform(5, 50), 2)
                tip_amount = max(5.0, tip_amount)
                player_data["cash"] = player_data.get("cash", 0) + tip_amount
                game.save_player_data(user.id, player_data)
                tip_message = f"\n🍕 Woah, some wiseguy just tipped you an extra ${tip_amount:.2f} for the 'best slice in town.' You're killin' it!"
                logger.info(f"User {user.id} received a tip of ${tip_amount:.2f}")

            # Pineapple Easter Egg
            pineapple_chance = 0.05
            if random.random() < pineapple_chance:
                pineapple_message = "\n🍍 Psst... Remember, putting pineapple on your pizza may get you sent to the gulag."
                logger.info(f"User {user.id} triggered the pineapple easter egg.")

            # Send confirmation with comma formatting
            await update.message.reply_html(f"🤑 Pizza payday, baby! You just grabbed ${collected_amount:,.2f} fresh outta the oven!{tip_message}{pineapple_message}")

            # Notifications AFTER confirmation
            await send_challenge_notifications(user.id, completed_challenges, context)
            await check_and_notify_achievements(user.id, context)
        else:
            await update.message.reply_html("Nothin' to collect, boss. Ovens are cold!")

    except Exception as e:
        logger.error(f"Error during collect_command for {user.id}: {e}", exc_info=True)
        await update.message.reply_text("Bada bing! Somethin' went wrong collectin' the dough.")

async def upgrade_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles /upgrade. Shows options if no args, processes if args."""
    user = update.effective_user
    if not user:
         await update.message.reply_text("Can't upgrade if I don't know who you are!")
         return
    await update_player_display_name(user.id, user)

    if not context.args:
        # No args - show options with buttons
        await _show_upgrade_options(update, context)
        return
    else:
        # Args provided - attempt direct upgrade
        shop_name_arg = " ".join(context.args).strip()
        player_data = game.load_player_data(user.id)
        shops = player_data.get("shops", {})
        target_shop_name = None
        for name in shops.keys():
            if name.lower() == shop_name_arg.lower():
                target_shop_name = name
                break
        if not target_shop_name:
            await update.message.reply_text(f"Whaddya talkin' about? You don't own '{shop_name_arg}'. Check /status or use /upgrade first.")
            return
        # Call the processing helper (pass update=None as it's not from a callback)
        await _process_upgrade(context, user.id, target_shop_name, query=None)

async def expand_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
         await update.message.reply_text("Who dis? Can't expand if I dunno who you are.")
         return
    await update_player_display_name(user.id, user)

    # If arguments provided, handle direct expansion attempt (existing logic)
    if context.args:
        expansion_name_arg = " ".join(context.args).strip()
        target_expansion_name = None
        for name in game.EXPANSION_LOCATIONS.keys():
            if name.lower() == expansion_name_arg.lower():
                target_expansion_name = name
                break
        if not target_expansion_name:
            await update.message.reply_text(f"'{expansion_name_arg}'? Never heard of it. Check available spots via /expand (no args) or /status.")
            return
        logger.info(f"User {user.id} attempting direct expand to '{target_expansion_name}'.")
        await _process_expansion(update, context, user.id, target_expansion_name)
        return

    # --- No arguments: Show available expansions with buttons & costs/perf --- #
    logger.info(f"User {user.id} requested expansion list.")
    player_data = game.load_player_data(user.id)
    if not player_data:
        await update.message.reply_text("Could not load your data.")
        return

    available = game.get_available_expansions(player_data)

    if not available:
        await update.message.reply_text("No new turf available right now, boss. Keep growin' the current spots!")
        return

    keyboard = []
    row = []
    for i, loc in enumerate(available):
        cost = game.get_expansion_cost(loc)
        current_perf = game.get_current_performance_multiplier(loc)
        perf_emoji = "📈" if current_perf > 1.1 else "📉" if current_perf < 0.9 else "🤷‍♂️"
        # Show performance and cost on button
        button_text = f"{loc} {perf_emoji}x{current_perf:.1f} (${cost:,.0f})"
        row.append(InlineKeyboardButton(button_text, callback_data=f"expand_{loc}"))
        if (i + 1) % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Ready to expand the empire? Choose your next conquest (Perf/Cost shown):", reply_markup=reply_markup)

# --- Helper for processing expansion --- #
async def _process_expansion(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, target_expansion_name: str):
    """Internal function to handle the actual expansion logic and feedback."""
    logger.info(f"Entered _process_expansion for user {user_id}, target {target_expansion_name}") # Added log
    try:
        success, message, completed_challenges = game.expand_shop(user_id, target_expansion_name)
        # Correctly check if the update object itself is the CallbackQuery
        from telegram import CallbackQuery # Local import for type check
        is_callback = isinstance(update, CallbackQuery)
        logger.debug(f"_process_expansion: is_callback = {is_callback}") # Added log

        if success:
            fun_messages = [
                 f"🗽 Fuggedaboutit! Your pizza empire just hit {target_expansion_name}!",
                 f"🗺️ You've outgrown the neighborhood? Time to take this pizza circus to {target_expansion_name}!",
                 f"🍕 Plantin' the flag in {target_expansion_name}! More ovens, more money!"
            ]
            response_message = random.choice(fun_messages)
            if is_callback:
                 logger.debug("Attempting to edit message for callback (success).")
                 await update.edit_message_text(text=response_message, parse_mode="HTML") # Use update directly
            else:
                 logger.debug("Attempting to send new message for command (success).")
                 await context.bot.send_message(chat_id=user_id, text=response_message, parse_mode="HTML")

            await send_challenge_notifications(user_id, completed_challenges, context)
            await check_and_notify_achievements(user_id, context)
        else:
            # Send the error message from game.expand_shop
            if is_callback:
                 logger.debug("Attempting to edit message for callback (failure).")
                 await update.edit_message_text(text=message) # Use update directly
            else:
                 logger.debug("Attempting to send new message for command (failure).")
                 await context.bot.send_message(chat_id=user_id, text=message)

    except Exception as e:
        logger.error(f"Error during _process_expansion for {user_id}, location {target_expansion_name}: {e}", exc_info=True)
        error_message = "Whoa there! Somethin' went sideways tryin' to expand."
        # Correctly check if the original trigger was a callback
        from telegram import CallbackQuery # Local import for type check
        is_callback = isinstance(update, CallbackQuery)
        logger.debug(f"_process_expansion exception: is_callback = {is_callback}")
        if is_callback:
             # Use update.message.chat_id for sending fallback if edit fails
             chat_id_to_reply = update.message.chat_id if update.message else user_id
             try:
                  await update.edit_message_text(text=error_message)
             except Exception as edit_err:
                  logger.error(f"Failed to edit message on expansion error: {edit_err}")
                  await context.bot.send_message(chat_id=chat_id_to_reply, text=error_message)
        else:
             await context.bot.send_message(chat_id=user_id, text=error_message)

# --- New Callback Handler for Expansion Buttons --- #
async def expansion_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles button presses for selecting an expansion location."""
    query = update.callback_query
    user = query.from_user
    logger.info(f"--- expansion_choice_callback ENTERED by user {user.id} ---") # <<< Log Entry

    try:
        await query.answer() # Answer callback query quickly
        logger.info(f"Callback query answered for user {user.id}.") # <<< Log Answer
    except Exception as e:
        logger.error(f"ERROR answering callback query for expansion: {e}", exc_info=True)
        # If answering fails, we likely can't edit the message either, just log.
        return

    # Extract location from callback data (e.g., "expand_London")
    try:
        target_location = query.data.split("expand_", 1)[1]
        logger.info(f"Parsed target_location: {target_location} for user {user.id}") # <<< Log Parse
    except IndexError:
        logger.warning(f"Invalid expansion callback data received: {query.data}")
        try:
            await query.edit_message_text("Invalid choice.")
        except Exception as edit_err:
             logger.error(f"Failed to edit message on invalid callback data: {edit_err}")
        return

    logger.info(f"User {user.id} chose to expand to {target_location} via button.")
    # Pass the query object to the helper
    await _process_expansion(query, context, user.id, target_location)

async def challenges_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    await update_player_display_name(user.id, user) # <-- Update name on challenges
    logger.info(f"User {user.id} requested challenges.")
    player_data = game.load_player_data(user.id)
    needs_save = False # Flag to check if we modified data

    # --- Generate challenges on demand if missing --- #
    if player_data.get("active_challenges", {}).get("daily") is None:
        logger.info(f"Daily challenge missing for {user.id}, generating on demand.")
        game.generate_new_challenges(user.id, 'daily')
        needs_save = True # generate_new_challenges saves, but we need to reload

    if player_data.get("active_challenges", {}).get("weekly") is None:
        logger.info(f"Weekly challenge missing for {user.id}, generating on demand.")
        game.generate_new_challenges(user.id, 'weekly')
        needs_save = True

    # Reload data if we generated any challenges
    if needs_save:
        logger.info(f"Reloading player data for {user.id} after on-demand generation.")
        player_data = game.load_player_data(user.id)
    # --- End generation on demand --- #

    stats = player_data.get("stats", {})
    active_challenges = player_data.get("active_challenges", {})
    challenge_progress = player_data.get("challenge_progress", {})

    lines = ["<b>--- Your Active Challenges ---</b>"]

    for timescale in ["daily", "weekly"]:
        challenge = active_challenges.get(timescale)
        lines.append(f"\n<b>{timescale.capitalize()} Challenge:</b>")
        if challenge:
            challenge_id = challenge["id"]
            metric = challenge["metric"]
            goal = challenge["goal"]
            current_prog = stats.get(metric, 0)
            is_complete = challenge_progress.get(timescale, {}).get(challenge_id, False)

            progress_str = f" ({current_prog:,.0f} / {goal:,.0f})" if isinstance(goal, (int, float)) else ""
            status_str = " ✅ Completed!" if is_complete else progress_str

            lines.append(f"  - {challenge['description']}{status_str}")
            lines.append(f"    Reward: {challenge['reward_value']:,} {challenge['reward_type'].upper()}")
        else:
            # This case should ideally not happen now, but keep as fallback
            lines.append("  Error generating challenge. Check logs or try again later.")

    await update.message.reply_html("\n".join(lines))

# --- Consolidated Leaderboard Command --- #
async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays both global leaderboards (Total Income & Current Cash)."""
    user = update.effective_user
    if not user:
        return
    logger.info(f"User {user.id} requested combined leaderboard.")
    await update_player_display_name(user.id, user)

    try:
        # --- Fetch Data --- #
        top_income_players = game.get_leaderboard_data(limit=10)
        top_cash_players = game.get_cash_leaderboard_data(limit=10)

        # --- Format Income Leaderboard --- #
        lines = ["<b>🏆 Global Pizza Empire Leaderboard 🏆</b>\n(Based on Total Income Earned)\n"]
        if not top_income_players:
            lines.append("<i>No income earned yet!</i>")
        else:
            for i, (player_id, display_name, total_income) in enumerate(top_income_players):
                rank = i + 1
                name = display_name or f"Player {player_id}"
                if len(name) > 25: name = name[:22] + "..."
                lines.append(f"{rank}. {name} - ${total_income:,.2f}")

        # --- Format Cash Leaderboard --- #
        lines.append("\n<b>🤑 William's Wallet Leaderboard 🤑</b>\n(Based on Current Cash)\n")
        if not top_cash_players:
            lines.append("<i>Everyone's broke!</i>")
        else:
            for i, (player_id, display_name, cash_amount) in enumerate(top_cash_players):
                rank = i + 1
                name = display_name or f"Player {player_id}"
                if len(name) > 25: name = name[:22] + "..."
                lines.append(f"{rank}. {name} - ${cash_amount:,.2f}")

        await update.message.reply_html("\n".join(lines))

    except Exception as e:
        logger.error(f"Error generating combined leaderboard: {e}", exc_info=True)
        await update.message.reply_text("Couldn't fetch the leaderboards right now, try again later.")

# --- Help Command --- #
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a helpful message listing available commands."""
    user = update.effective_user
    logger.info(f"User {user.id if user else 'Unknown'} requested help.")

    help_text = (
        "🍕 <b>Pizza Empire Command Guide</b> 🍕\n\n"
        "<b>Core Gameplay:</b>\n"
        "/start - Initialize your pizza empire (or view this message again).\n"
        "/setname [name] - Set your franchise name (e.g., `/setname Luigi's Finest`).\n"
        "/renameshop [loc] [name] - Rename a specific shop (e.g., `/renameshop Brooklyn Luigi's`).\n"
        "/status [s:key] - Check status. Optionally sort shops by `s:name`, `s:level`, or `s:cost` (e.g., `/status s:cost`).\n"
        "/collect - Scoop up the cash your shops have earned!\n"
        "/upgrade - Show available shop upgrades (or use `/upgrade [shop]` directly).\n"
        "/expand [location] - List expansion options (with costs!) or expand to a new location.\n\n"
        "<b>Progression & Fun:</b>\n"
        "/challenges - View your current daily and weekly challenges.\n"
        "/leaderboard - See top players by total income earned.\n"
        "/buycoins - View options to purchase Pizza Coins 🍕 (premium currency).\n"
        # Add /boost here if/when implemented
        "/help - Show this command guide.\n\n"
        "<b>PvP Actions:</b>\n"
        "/sabotage - Choose a player and shop to sabotage (Costs cash, high risk/reward!).\n\n"
        "<i>Now get back to building that empire!</i>"
    )
    await update.message.reply_html(help_text)

# --- Payment Handlers ---
async def buy_coins_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user # Get user object
    if not user:
         await update.message.reply_text("Cannot buy coins without user info.")
         return
    await update_player_display_name(user.id, user) # <-- Update name on buycoins
    if not PAYMENT_PROVIDER_TOKEN:
        await update.message.reply_text(
            "My owner still hasn't signed up for a Stripe account. If you send him funds, he will send you a bajillion pizza coins."
        )
        return
    for pack_id, (name, description, price_cents, coin_amount) in game.PIZZA_COIN_PACKS.items():
        title = f"{name} ({coin_amount} Coins)"
        payload = f"BUY_{pack_id.upper()}_{user.id}"
        currency = "USD"
        prices = [LabeledPrice(label=name, amount=price_cents)]
        logger.info(f"Sending invoice for {pack_id} to chat {user.id}")
        try:
            await context.bot.send_invoice(
                chat_id=user.id, title=title, description=description, payload=payload,
                provider_token=PAYMENT_PROVIDER_TOKEN, currency=currency, prices=prices,
            )
        except Exception as e:
            logger.error(f"Failed to send invoice for {pack_id} to {user.id}: {e}", exc_info=True)
            await update.message.reply_text(f"Sorry, couldn't start the purchase for {name}. Please try again.")

async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.pre_checkout_query
    payload_parts = query.invoice_payload.split('_')
    if len(payload_parts) != 3 or payload_parts[0] != 'BUY':
        logger.warning(f"Invalid payload received in precheckout: {query.invoice_payload}")
        await query.answer(ok=False, error_message="Something went wrong with your order details.")
        return
    pack_id = payload_parts[1].lower()
    user_id_from_payload = int(payload_parts[2])
    if query.from_user.id != user_id_from_payload:
         logger.warning(f"User ID mismatch in precheckout! Query from {query.from_user.id}, payload for {user_id_from_payload}")
         await query.answer(ok=False, error_message="User mismatch, cannot proceed.")
         return
    pack_details = game.get_pizza_coin_pack(pack_id)
    if not pack_details or query.total_amount != pack_details[2]:
        logger.warning(f"Pack details mismatch or amount changed for {pack_id}. Query: {query.total_amount}, Expected: {pack_details[2] if pack_details else 'N/A'}")
        await query.answer(ok=False, error_message="Sorry, the price or item details have changed. Please try initiating the purchase again.")
    else:
        logger.info(f"PreCheckout OK for user {query.from_user.id}, pack {pack_id}")
        await query.answer(ok=True)

async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # User object is available here
    user = update.message.from_user
    if user:
        await update_player_display_name(user.id, user)
    payment_info = update.message.successful_payment
    payload = payment_info.invoice_payload
    user_id = update.message.from_user.id
    amount_paid = payment_info.total_amount
    currency = payment_info.currency
    logger.info(
        f"Successful payment received! User: {user_id}, Amount: {amount_paid} {currency}, Payload: {payload}"
    )
    payload_parts = payload.split('_')
    pack_id = None
    if len(payload_parts) == 3 and payload_parts[0] == 'BUY':
        pack_id = payload_parts[1].lower()
    if pack_id:
        pack_details = game.get_pizza_coin_pack(pack_id)
        if pack_details:
            _, _, _, coin_amount = pack_details
            logger.info(f"Crediting {coin_amount} coins for pack {pack_id} to user {user_id}")
            game.credit_pizza_coins(user_id, coin_amount)
            await update.message.reply_text(
                f"Thank you for your purchase! {coin_amount} Pizza Coins 🍕 have been added to your account."
            )
            # Check achievements after successful purchase too?
            # await check_and_notify_achievements(user_id, context)
        else:
            logger.error(f"Could not find pack details for pack_id '{pack_id}' from successful payment payload: {payload}")
            await update.message.reply_text("Thank you for your purchase! There was an issue crediting the coins automatically, please contact support.")
    else:
        logger.error(f"Could not parse pack_id from successful payment payload: {payload}")
        await update.message.reply_text("Thank you for your purchase! There was an issue crediting the coins automatically, please contact support.")

# --- DEPRECATED Placeholder Premium Commands ---
async def boost_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    message = game.use_pizza_coins_for_speedup(user.id, "instant_collect")
    await update.message.reply_text(message)

async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Sorry, I didn't understand that command. Try /start or /status.")

# --- Mafia Callback Handler --- #
async def mafia_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the response to the Mafia shakedown buttons."""
    query = update.callback_query
    user = query.from_user
    await query.answer() # Answer callback query quickly

    choice = query.data # "mafia_pay" or "mafia_refuse"

    # Retrieve stored data
    collected_amount = context.user_data.get('mafia_collect_amount')
    mafia_demand = context.user_data.get('mafia_demand')

    if collected_amount is None or mafia_demand is None:
        logger.warning(f"Mafia callback triggered for user {user.id} but user_data missing.")
        await query.edit_message_text(text="Something went wrong processing your choice. Try collecting again.")
        return

    # Clear temporary data immediately
    context.user_data.pop('mafia_collect_amount', None)
    context.user_data.pop('mafia_demand', None)

    logger.info(f"User {user.id} chose '{choice}' for Mafia event (Collect: ${collected_amount:.2f}, Demand: ${mafia_demand:.2f})")

    cash_to_add = 0.0
    outcome_message = ""
    challenge_metrics_to_update = []

    if choice == "mafia_pay":
        cash_to_add = max(0, collected_amount - mafia_demand)
        outcome_message = f"💸 You paid the ${mafia_demand:,.2f}. Smart move... maybe. You keep ${cash_to_add:,.2f}."
        challenge_metrics_to_update = ["session_income", "session_collects"]
    elif choice == "mafia_refuse":
        # 50/50 chance
        if random.random() < 0.5:
            # Win!
            cash_to_add = collected_amount
            outcome_message = f"💪 You told 'em to fuggedaboutit, and they backed down! You keep the whole ${cash_to_add:,.2f}!"
            challenge_metrics_to_update = ["session_income", "session_collects"]
            logger.info(f"User {user.id} WON the Mafia gamble.")
        else:
            # Lose!
            cash_to_add = 0.0
            outcome_message = f"🤕 Ouch! They weren't bluffing. They took the whole ${collected_amount:,.2f}. Maybe pay up next time?"
            # Don't track session_income if they lost it all
            challenge_metrics_to_update = ["session_collects"] # Still counts as a collection attempt for challenges
            logger.info(f"User {user.id} LOST the Mafia gamble.")

    # --- Update Player Data --- #
    try:
        player_data = game.load_player_data(user.id)
        if not player_data:
             raise ValueError("Failed to load player data after Mafia interaction.")

        if cash_to_add > 0:
            player_data["cash"] = player_data.get("cash", 0) + cash_to_add
            player_data["total_income_earned"] = player_data.get("total_income_earned", 0) + cash_to_add
            # Only track income stat if they actually received cash
            if "session_income" in challenge_metrics_to_update:
                 player_data["stats"]["session_income"] = player_data["stats"].get("session_income", 0) + cash_to_add

        # Always track collection attempt stat
        player_data["stats"]["session_collects"] = player_data["stats"].get("session_collects", 0) + 1

        # Check challenges based on what actually happened
        completed_challenges = game.update_challenge_progress(player_data, challenge_metrics_to_update)

        game.save_player_data(user.id, player_data)

        # --- Notify User --- #
        await query.edit_message_text(text=outcome_message) # Update the original message
        await send_challenge_notifications(user.id, completed_challenges, context)
        # Check achievements based on final state
        await check_and_notify_achievements(user.id, context)

    except Exception as e:
        logger.error(f"Error processing Mafia callback outcome for {user.id}: {e}", exc_info=True)
        # Try to send a fallback message if editing failed
        try:
            await context.bot.send_message(chat_id=user.id, text="An error occurred processing your decision. Please check /status.")
        except Exception: # Ignore errors sending the fallback
            pass

# --- Set Franchise Name Command --- #
async def setname_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Allows the player to set their franchise name."""
    user = update.effective_user
    if not user:
        await update.message.reply_text("Cannot set name without user info.")
        return
    await update_player_display_name(user.id, user)

    if not context.args:
        await update.message.reply_text("Usage: /setname [Your Franchise Name]")
        return

    new_name = " ".join(context.args).strip()
    max_len = 50
    if not new_name:
        await update.message.reply_text("You gotta give your empire a name!")
        return
    if len(new_name) > max_len:
        await update.message.reply_text(f"Whoa, that's a long name! Keep it under {max_len} characters, boss.")
        return

    # Basic sanitization: Remove potential HTML tags just in case
    # A more robust solution might involve allowing specific safe tags or using a library
    sanitized_name = re.sub('<[^<]+?>', '', new_name) # Strip HTML tags
    if not sanitized_name:
         await update.message.reply_text("C'mon, give it a real name!")
         return

    logger.info(f"User {user.id} attempting to set franchise name to: {sanitized_name}")
    try:
        player_data = game.load_player_data(user.id)
        if not player_data:
             await update.message.reply_text("Could not load your data to set the name.")
             return

        player_data["franchise_name"] = sanitized_name
        game.save_player_data(user.id, player_data)
        # Use html.escape for displaying user-provided name safely in HTML context
        await update.message.reply_html(f"Alright, your pizza empire shall henceforth be known as: <b>{html.escape(sanitized_name)}</b>! Good luck!")

    except Exception as e:
        logger.error(f"Error setting franchise name for {user.id}: {e}", exc_info=True)
        await update.message.reply_text("Couldn't save the new name right now. Try again.")

# --- Rename Shop Command --- #
async def renameshop_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Allows the player to rename a specific shop location."""
    user = update.effective_user
    if not user:
        await update.message.reply_text("Cannot rename shop without user info.")
        return
    await update_player_display_name(user.id, user)

    # Expecting: /renameshop [location] [new name]
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: /renameshop [Location] [New Custom Name]")
        return

    location_arg = context.args[0].strip()
    new_custom_name = " ".join(context.args[1:]).strip()
    max_len = 30 # Shorter max len for shop names

    if not new_custom_name:
        await update.message.reply_text("You gotta give the shop a new name!")
        return
    if len(new_custom_name) > max_len:
        await update.message.reply_text(f"Keep the shop name under {max_len} characters, boss.")
        return

    # Basic sanitization
    sanitized_new_name = re.sub('<[^<]+?>', '', new_custom_name)
    if not sanitized_new_name:
         await update.message.reply_text("C'mon, give it a real name (no funny HTML stuff)!")
         return

    logger.info(f"User {user.id} attempting to rename shop at '{location_arg}' to: {sanitized_new_name}")

    try:
        player_data = game.load_player_data(user.id)
        if not player_data:
             await update.message.reply_text("Could not load your data to rename the shop.")
             return

        shops = player_data.get("shops", {})
        target_location_key = None
        # Find the location key case-insensitively
        for loc_key in shops.keys():
            if loc_key.lower() == location_arg.lower():
                target_location_key = loc_key
                break

        if not target_location_key:
            await update.message.reply_text(f"You don't own a shop at '{location_arg}'. Check /status.")
            return

        # Update the custom name
        shops[target_location_key]["custom_name"] = sanitized_new_name
        player_data["shops"] = shops # Ensure the shops dict is updated in player_data
        game.save_player_data(user.id, player_data)

        await update.message.reply_html(f"Alright, your shop at {target_location_key} is now proudly called: <b>{html.escape(sanitized_new_name)}</b>!")

    except Exception as e:
        logger.error(f"Error renaming shop for {user.id}: {e}", exc_info=True)
        await update.message.reply_text("Couldn't save the new shop name right now. Try again.")

# --- Sabotage Command --- #
SABOTAGE_BASE_COST = 1000
SABOTAGE_PCT_COST = 0.05
SABOTAGE_SUCCESS_CHANCE = 0.40
SABOTAGE_BACKFIRE_CHANCE = 0.25 # Chance of backfire *if* the initial attempt fails
SABOTAGE_DURATION_SECONDS = 3600
SABOTAGE_COOLDOWN_SECONDS = 900

async def sabotage_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Starts the sabotage process by showing potential targets and explaining risks."""
    user = update.effective_user
    if not user:
        await update.message.reply_text("Need user info to start sabotage.")
        return
    await update_player_display_name(user.id, user)
    logger.info(f"User {user.id} initiated sabotage command.")

    attacker_user_id = user.id
    attacker_data = game.load_player_data(attacker_user_id)
    if not attacker_data:
        await update.message.reply_text("Couldn't load your data.")
        return

    # --- Check Cooldown First --- #
    now = time.time()
    last_attempt_time = attacker_data.get("last_sabotage_attempt_time", 0.0)
    time_since_last = now - last_attempt_time
    if time_since_last < SABOTAGE_COOLDOWN_SECONDS:
         remaining_cooldown = timedelta(seconds=int(SABOTAGE_COOLDOWN_SECONDS - time_since_last))
         await update.message.reply_text(f"Your agents need to lay low! Sabotage available again in {str(remaining_cooldown).split('.')[0]}.")
         return
    # --- End Cooldown Check --- #

    # Calculate potential cost for explanation
    attacker_cash = attacker_data.get("cash", 0)
    potential_cost = round(SABOTAGE_BASE_COST + (attacker_cash * SABOTAGE_PCT_COST), 2)

    # Show Target List
    try:
        potential_targets = game.get_cash_leaderboard_data(limit=20)
        valid_targets = [(pid, name, cash) for pid, name, cash in potential_targets if pid != user.id]
        if not valid_targets:
            await update.message.reply_text("No valid targets found on the cash leaderboard right now!")
            return

        keyboard = []
        for i, (target_id, display_name, cash_amount) in enumerate(valid_targets):
            rank = i + 1
            name = display_name or f"Player {target_id}"
            if len(name) > 20: name = name[:17] + "..."
            button_text = f"{rank}. {name} (${cash_amount:,.0f})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"sabotage_{target_id}")])

        if not keyboard: # Should be caught by valid_targets check, but safety
             await update.message.reply_text("No valid targets found.")
             return

        reply_markup = InlineKeyboardMarkup(keyboard)
        # --- Explanation Message --- #
        explanation = (
            f"⚠️ **Sabotage Warning!** ⚠️\n"
            f"Attempting sabotage currently costs: **${potential_cost:,.2f}** (Cost only applies *if you fail*).\n\n"
            f"<b>Success ({int(SABOTAGE_SUCCESS_CHANCE * 100)}%):</b> Target's top shop shut down for {int(SABOTAGE_DURATION_SECONDS / 60)} mins.\n"
            f"<b>Failure ({int((1 - SABOTAGE_SUCCESS_CHANCE) * 100)}%):</b> You lose the ${potential_cost:,.2f} cost.\n"
            f"  - <b>Backfire Chance ({int(SABOTAGE_BACKFIRE_CHANCE * 100)}% of failures):</b> Your *own* top shop gets shut down too!\n\n"
            f"Cooldown after attempt: {int(SABOTAGE_COOLDOWN_SECONDS / 60)} minutes.\n\n"
            f"Choose your target wisely:"
        )
        await update.message.reply_html(explanation, reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Error preparing sabotage target list: {e}", exc_info=True)
        await update.message.reply_text("Couldn't fetch potential targets right now.")

# --- Sabotage Choice Callback Handler (Select Target Player) --- #
async def sabotage_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = query.from_user
    await query.answer()

    try:
        target_user_id = int(query.data.split("sabotage_", 1)[1])
    except (IndexError, ValueError):
        logger.warning(f"Invalid sabotage callback data: {query.data}")
        await query.edit_message_text("Invalid target selection.")
        return

    attacker_user_id = user.id
    if target_user_id == attacker_user_id:
        await query.edit_message_text("Can't sabotage yourself!")
        return

    # Load target data to get their shops
    target_data = game.load_player_data(target_user_id)
    target_name = target_data.get("display_name") or f"Player {target_user_id}"
    target_shops = target_data.get("shops", {})

    if not target_data or not target_shops:
        await query.edit_message_text(f"{target_name} has no shops to sabotage!")
        return

    # --- Show Shop Selection Buttons --- #
    keyboard = []
    shop_list = sorted(target_shops.items(), key=lambda item: item[1].get('level', 1), reverse=True) # Sort by level desc

    for location, shop_data in shop_list:
        custom_name = shop_data.get("custom_name", location)
        level = shop_data.get("level", 1)
        display_name = f"{custom_name} ({location})" if custom_name != location else location
        # Callback data format: sabotage_shop_{target_user_id}_{location_name}
        callback_data = f"sabo_shop_{target_user_id}_{location}"
        keyboard.append([InlineKeyboardButton(f"{display_name} (Lvl {level})", callback_data=callback_data)])

    if not keyboard:
        await query.edit_message_text(f"{target_name} seems to have no valid shops.")
        return

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"Targeting {target_name}. Which of their shops should the agent hit?",
        reply_markup=reply_markup
    )

# --- NEW Callback Handler for Upgrade Shop Buttons --- #
async def upgrade_shop_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the button press selecting the specific shop to upgrade."""
    query = update.callback_query
    user = query.from_user
    logger.info(f"--- upgrade_shop_choice_callback ENTERED by user {user.id} ---")
    await query.answer()

    try:
        # Format: upgrade_shop_{location_name}
        shop_location = query.data.split("upgrade_shop_", 1)[1]
        logger.info(f"Parsed shop_location: {shop_location} for user {user.id}")
    except IndexError:
        logger.warning(f"Invalid upgrade shop choice callback data: {query.data}")
        await query.edit_message_text("Invalid shop choice.")
        return

    # Call the processing helper, passing the query object
    await _process_upgrade(context, user.id, shop_location, query=query)

# --- Main Menu Callback Handler (Updated for Upgrade) --- #
async def main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles button presses from the main action keyboard, performs the action."""
    query = update.callback_query
    user = query.from_user
    action = query.data
    chat_id = query.message.chat_id # Use chat_id for sending new messages
    logger.info(f"--- main_menu_callback ENTERED by user {user.id}, action: {action} ---")

    # Answer callback query quickly
    try:
        await query.answer()
        logger.info(f"Callback query answered for main_menu action {action}, user {user.id}.")
    except Exception as e: logger.error(f"ERROR answering callback query for main_menu: {e}", exc_info=True); return

    # Remove keyboard first
    try: await query.edit_message_reply_markup(reply_markup=None); logger.debug(f"Original status keyboard removed.")
    except Exception as e: logger.warning(f"Failed to remove original status keyboard: {e}")

    try:
        await update_player_display_name(user.id, user)
        if action == "main_collect":
            logger.debug(f"Handling main_collect action for {user.id}")
            # Replicate collect_command logic
            collected_amount, completed_challenges, is_mafia_event, mafia_demand = game.collect_income(user.id)
            if is_mafia_event:
                if mafia_demand is None or mafia_demand <= 0:
                    await context.bot.send_message(chat_id=chat_id, text="Collectors seemed confused... lucky break?")
                else:
                    context.user_data['mafia_collect_amount'] = collected_amount
                    context.user_data['mafia_demand'] = mafia_demand
                    keyboard = [[InlineKeyboardButton(f"🤌 Pay ${mafia_demand:,.2f}", callback_data="mafia_pay"), InlineKeyboardButton("🙅‍♂️ Tell 'em Fuggedaboutit!", callback_data="mafia_refuse"),]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await context.bot.send_message(chat_id=chat_id, text=f"🚨 Uh oh! The Famiglia wants ${mafia_demand:,.2f} of your ${collected_amount:,.2f} haul! Pay up or refuse?", reply_markup=reply_markup)
            elif collected_amount > 0.01:
                tip_message, pineapple_message = "", ""
                if random.random() < 0.15: # Tip chance
                    player_data_tip = game.load_player_data(user.id)
                    tip_amount = round(random.uniform(collected_amount * 0.05, collected_amount * 0.2) + random.uniform(5, 50), 2)
                    tip_amount = max(5.0, tip_amount)
                    player_data_tip["cash"] = player_data_tip.get("cash", 0) + tip_amount
                    game.save_player_data(user.id, player_data_tip)
                    tip_message = f"\n🍕 Wiseguy tipped ya ${tip_amount:.2f}!"
                if random.random() < 0.05: # Pineapple chance
                    pineapple_message = "\n🍍 Psst... Remember the pineapple rule..."
                await context.bot.send_message(chat_id=chat_id, text=f"🤑 Pizza payday! +${collected_amount:,.2f}!{tip_message}{pineapple_message}", parse_mode="HTML")
                await send_challenge_notifications(user.id, completed_challenges, context)
                await check_and_notify_achievements(user.id, context)
            else:
                await context.bot.send_message(chat_id=chat_id, text="Nothin' to collect, boss. Ovens are cold!")

        elif action == "main_upgrade":
            logger.debug(f"Handling main_upgrade action for {user.id} - showing options")
            await _show_upgrade_options(query, context) # <<< Call helper to show buttons

        elif action == "main_expand":
            logger.debug(f"Handling main_expand action for {user.id}")
            # Replicate expand_command logic (no args case)
            player_data = game.load_player_data(user.id)
            if not player_data:
                await context.bot.send_message(chat_id=chat_id, text="Could not load your data.")
            else:
                available = game.get_available_expansions(player_data)
                if not available:
                     await context.bot.send_message(chat_id=chat_id, text="No new turf available right now, boss!")
                else:
                     keyboard = []
                     row = []
                     for i, loc in enumerate(available):
                         cost = game.get_expansion_cost(loc)
                         current_perf = game.get_current_performance_multiplier(loc)
                         perf_emoji = "📈" if current_perf > 1.1 else "📉" if current_perf < 0.9 else "🤷‍♂️"
                         button_text = f"{loc} {perf_emoji}x{current_perf:.1f} (${cost:,.0f})"
                         row.append(InlineKeyboardButton(button_text, callback_data=f"expand_{loc}"))
                         if (i + 1) % 2 == 0: keyboard.append(row); row = []
                     if row: keyboard.append(row)
                     reply_markup = InlineKeyboardMarkup(keyboard)
                     await context.bot.send_message(chat_id=chat_id, text="Choose your next conquest (Perf/Cost shown):", reply_markup=reply_markup)

        elif action == "main_challenges":
             logger.debug(f"Handling main_challenges action for {user.id}")
             await challenges_command(query.message, context)

        elif action == "main_leaderboard":
             logger.debug(f"Handling main_leaderboard action for {user.id}")
             await leaderboard_command(query.message, context)

        elif action == "main_buycoins":
             logger.debug(f"Handling main_buycoins action for {user.id}")
             await buy_coins_command(query.message, context)

        elif action == "main_help":
             logger.debug(f"Handling main_help action for {user.id}")
             await help_command(query.message, context)

        else:
            logger.warning(f"Received unknown main_menu callback query data: {action}")
            await context.bot.send_message(chat_id=chat_id, text="Sorry, that button seems outdated.")

    except Exception as e:
         logger.error(f"Error handling main_menu_callback action {action} for {user.id}: {e}", exc_info=True)
         await context.bot.send_message(chat_id=chat_id, text="Ay! Somethin' went wrong with that button.")

def main() -> None:
    """Start the bot and scheduler."""
    logger.info("Building Telegram Application...")
    # Revert Application builder to simpler form
    application = Application.builder().token(BOT_TOKEN).build()
    logger.info("Telegram Application built successfully.")

    logger.info("Adding command handlers...")
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("collect", collect_command))
    application.add_handler(CommandHandler("upgrade", upgrade_command))
    application.add_handler(CommandHandler("expand", expand_command))
    application.add_handler(CommandHandler("challenges", challenges_command))
    application.add_handler(CommandHandler("buycoins", buy_coins_command))
    application.add_handler(CommandHandler("leaderboard", leaderboard_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("setname", setname_command))
    application.add_handler(CommandHandler("renameshop", renameshop_command))
    application.add_handler(CommandHandler("sabotage", sabotage_command))
    # application.add_handler(CommandHandler("boost", boost_command)) # Placeholder boost command

    logger.info("Adding payment handlers...")
    application.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))

    logger.info("Adding unknown command handler...")
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    # --- Add Callback Handlers --- #
    logger.info("Adding callback handlers...")
    application.add_handler(CallbackQueryHandler(mafia_button_callback, pattern="^mafia_(pay|refuse)$"))
    application.add_handler(CallbackQueryHandler(expansion_choice_callback, pattern="^expand_"))
    application.add_handler(CallbackQueryHandler(sabotage_choice_callback, pattern="^sabotage_"))
    application.add_handler(CallbackQueryHandler(sabotage_shop_choice_callback, pattern="^sabo_shop_"))
    application.add_handler(CallbackQueryHandler(upgrade_shop_choice_callback, pattern="^upgrade_shop_"))
    application.add_handler(CallbackQueryHandler(main_menu_callback, pattern="^main_.*"))

    # Schedule challenge generation jobs
    logger.info("Setting up scheduled jobs...")
    try:
        # Run daily at 00:01 UTC
        scheduler.add_job(generate_daily_challenges_job, CronTrigger(hour=0, minute=1, timezone="UTC"), args=[application])
        # Run weekly on Monday at 00:05 UTC
        scheduler.add_job(generate_weekly_challenges_job, CronTrigger(day_of_week='mon', hour=0, minute=5, timezone="UTC"), args=[application])
        # Add new job for performance update (e.g., daily at 00:03 UTC)
        scheduler.add_job(update_location_performance_job, CronTrigger(hour=0, minute=3, timezone="UTC"), args=[application])
        scheduler.start()
        logger.info("Scheduler started successfully.")
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}", exc_info=True)
        # Depending on severity, might want to exit or just log

    logger.info("Starting Pizza Wars bot polling...")
    application.run_polling()

    # Shut down scheduler gracefully if bot stops (though run_polling blocks)
    # Consider using run_webhook for production which doesn't block like this
    # scheduler.shutdown()

if __name__ == "__main__":
    main()

