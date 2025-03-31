import logging
import os
import glob # For finding player data files
import random # For tips!
from datetime import time as dt_time, timedelta

# Telegram Core Types
from telegram import Update, LabeledPrice, ShippingOption, Invoice, InlineKeyboardButton, InlineKeyboardMarkup
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
    CallbackQueryHandler,
)

# Scheduling
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Import game logic functions
import game # Corrected import

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

# Initialize Database Schema
try:
    game.initialize_database()
except ConnectionError as e:
    logger.critical(f"Database connection failed on startup: {e}. Exiting.")
    exit() # Exit if DB can't be initialized
except Exception as e:
    logger.critical(f"Unexpected error during database initialization: {e}. Exiting.", exc_info=True)
    exit() # Exit on other DB init errors

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

# --- Utility function to update name ---
async def update_player_display_name(user_id: int, user: "telegram.User | None"):
    """Helper to call the game logic update function."""
    if user:
        game.update_display_name(user_id, user)

# --- Scheduled Job Functions ---
async def generate_daily_challenges_job(context: ContextTypes.DEFAULT_TYPE):
    """Scheduled job to generate daily challenges for all players in DB."""
    logger.info("Running daily challenge generation job...")
    try:
        user_ids = game.get_all_user_ids() # <<< Use DB function
        if not user_ids:
            logger.info("No players found in database for daily challenge generation.")
            return

        generated_count = 0
        for user_id in user_ids:
            try:
                # Optional: Add activity check here if desired
                game.generate_new_challenges(user_id, 'daily')
                generated_count += 1
                # Optional: Notify user (consider rate limiting if many users)
            except Exception as e:
                logger.error(f"Error generating daily challenge for user {user_id}: {e}", exc_info=True)
        logger.info(f"Daily challenge generation complete. Processed for {generated_count}/{len(user_ids)} users.")
    except Exception as e:
        logger.error(f"Failed to fetch user IDs for daily challenge job: {e}", exc_info=True)

async def generate_weekly_challenges_job(context: ContextTypes.DEFAULT_TYPE):
    """Scheduled job to generate weekly challenges for all players in DB."""
    logger.info("Running weekly challenge generation job...")
    try:
        user_ids = game.get_all_user_ids() # <<< Use DB function
        if not user_ids:
            logger.info("No players found in database for weekly challenge generation.")
            return

        generated_count = 0
        for user_id in user_ids:
            try:
                game.generate_new_challenges(user_id, 'weekly')
                generated_count += 1
                # Optional: Notify user
            except Exception as e:
                logger.error(f"Error generating weekly challenge for user {user_id}: {e}", exc_info=True)
        logger.info(f"Weekly challenge generation complete. Processed for {generated_count}/{len(user_ids)} users.")
    except Exception as e:
        logger.error(f"Failed to fetch user IDs for weekly challenge job: {e}", exc_info=True)

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

        # FTUE: Show status immediately
        logger.info(f"Sending initial status to player {user.id}")
        status_message = game.format_status(player_data)
        await update.message.reply_html(status_message)

        # Check initial achievements (safe even if not new, won't re-award)
        await check_and_notify_achievements(user.id, context)

    except Exception as e:
        logger.error(f"ERROR in start_command for user {user.id}: {e}", exc_info=True)
        await update.message.reply_text("Ay, somethin' went wrong gettin' ya started. Try /start again maybe?")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    await update_player_display_name(user.id, user) # <-- Update name on status
    logger.info(f"User {user.id} requested status.")
    player_data = game.load_player_data(user.id)
    status_message = game.format_status(player_data)
    await update.message.reply_html(status_message)

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

            # Send confirmation
            await update.message.reply_html(f"🤑 Pizza payday, baby! You just grabbed ${collected_amount:.2f} fresh outta the oven!{tip_message}{pineapple_message}")

            # Notifications AFTER confirmation
            await send_challenge_notifications(user.id, completed_challenges, context)
            await check_and_notify_achievements(user.id, context)
        else:
            await update.message.reply_html("Nothin' to collect, boss. Ovens are cold!")

    except Exception as e:
        logger.error(f"Error during collect_command for {user.id}: {e}", exc_info=True)
        await update.message.reply_text("Bada bing! Somethin' went wrong collectin' the dough.")

async def upgrade_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
         await update.message.reply_text("Can't upgrade if I don't know who you are!")
         return
    await update_player_display_name(user.id, user) # <-- Update name on upgrade
    player_data = game.load_player_data(user.id)
    shops = player_data.get("shops", {})

    # --- "Talkin' Dough" Clarity --- #
    if not context.args:
        if not shops:
             await update.message.reply_text("You ain't got no shops to upgrade yet, boss! Get started!")
             return

        lines = ["🤌 Hey Pizza Maestro, thinkin' 'bout upgrades? Here's what's cookin':\n"]
        for shop_name, shop_data in shops.items():
            current_level = shop_data.get("level", 1)
            next_level = current_level + 1
            upgrade_cost = game.get_upgrade_cost(current_level, shop_name)
            current_rate_hr = game.get_shop_income_rate(shop_name, current_level) * 3600
            next_rate_hr = game.get_shop_income_rate(shop_name, next_level) * 3600
            lines.append(f"- <b>{shop_name}</b> (Level {current_level} → {next_level}): Costs ya ${upgrade_cost:,.2f}, bumps your earnings from ${current_rate_hr:,.2f}/hr to ${next_rate_hr:,.2f}/hr.")
            lines.append(f"  Type /upgrade {shop_name.lower()} to toss your dough at it!")

        await update.message.reply_html("\n".join(lines))
        return

    # --- Process Specific Upgrade Request --- #
    shop_name_arg = " ".join(context.args).strip()
    # Find the shop matching the argument (case-insensitive)
    target_shop_name = None
    for name in shops.keys():
        if name.lower() == shop_name_arg.lower():
            target_shop_name = name
            break

    if not target_shop_name:
        await update.message.reply_text(f"Whaddya talkin' about? You don't own a shop called '{shop_name_arg}'. Check ya /status.")
        return

    logger.info(f"User {user.id} attempting to upgrade '{target_shop_name}'.")

    try:
        # upgrade_shop now returns (success, message_or_new_level_str, completed_challenges)
        success, result_data, completed_challenges = game.upgrade_shop(user.id, target_shop_name)

        if success:
            # --- Cheeky SUCCESS Feedback --- #
            new_level = result_data # On success, result_data is the new level string
            fun_messages = [
                f"🍾 Hot dang! Your {target_shop_name} spot just hit Level {new_level}. Lines around the block incoming!",
                f"🤌 Mama mia! {target_shop_name} is now Level {new_level}! More dough, less problems!",
                f"🎉 Level {new_level} for {target_shop_name}! You're cookin' with gas now!"
            ]
            await update.message.reply_html(random.choice(fun_messages))
            await send_challenge_notifications(user.id, completed_challenges, context)
            await check_and_notify_achievements(user.id, context)
        else:
            # --- Handle FAILURE --- #
            failure_message = result_data # On failure, result_data is the message from game.py
            if "Not enough cash" in failure_message:
                 await update.message.reply_html(failure_message) # Send the standard insufficient funds message
            else:
                 # It was a random failure, use dramatic messages
                 # Extract cost from the specific failure message format
                 cost_lost_str = "the cost" # Default fallback
                 try:
                      cost_lost_str = failure_message.split("lost ")[-1].split(" in")[0]
                 except IndexError:
                      logger.warning(f"Could not parse cost from failure message: {failure_message}")

                 failure_messages = [
                      f"💥 KABOOM! The contractors messed up! The upgrade failed and {cost_lost_str} went up in smoke!",
                      f"😱 Mamma Mia! A sinkhole swallowed the construction crew! Upgrade failed, dough is gone ({cost_lost_str})!",
                      f"📉 Bad investment, boss! The upgrade for {target_shop_name} flopped harder than a soggy pizza base. Lost {cost_lost_str}!",
                      f"🔥 Grease fire! The whole upgrade went belly-up. Kiss {cost_lost_str} goodbye!"
                 ]
                 await update.message.reply_html(random.choice(failure_messages))

    except Exception as e:
        logger.error(f"Error during upgrade_command for {user.id}, shop {target_shop_name}: {e}", exc_info=True)
        await update.message.reply_text("Ay caramba! Somethin' went wrong with the upgrade.")

async def expand_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
         await update.message.reply_text("Who dis? Can't expand if I dunno who you are.")
         return
    await update_player_display_name(user.id, user) # <-- Update name on expand
    if not context.args:
        # Maybe list available expansions here too?
        player_data = game.load_player_data(user.id)
        available = game.get_available_expansions(player_data)
        if not available:
            await update.message.reply_text("No new turf available right now, boss. Keep growin'!")
        else:
            lines = ["Ready to expand the empire? Here's where you can plant your flag next:"]
            for loc in available:
                lines.append(f"- {loc} (Use /expand {loc.lower()})")
            await update.message.reply_text("\n".join(lines))
        return

    expansion_name_arg = " ".join(context.args).strip()
    # Check if it's a valid *possible* expansion (case-insensitive)
    target_expansion_name = None
    for name in game.EXPANSION_LOCATIONS.keys():
         if name.lower() == expansion_name_arg.lower():
              target_expansion_name = name
              break

    if not target_expansion_name:
         await update.message.reply_text(f"'{expansion_name_arg}'? Never heard of it. Where's dat? Try checkin' ya /status for available spots.")
         return

    logger.info(f"User {user.id} attempting to expand to '{target_expansion_name}'.")

    try:
        # Expand shop now returns completed challenges
        success, message, completed_challenges = game.expand_shop(user.id, target_expansion_name)

        # --- Cheeky Feedback --- #
        if success:
            fun_messages = [
                 f"🗽 Fuggedaboutit! Your pizza empire just hit {target_expansion_name}!",
                 f"🗺️ You've outgrown the neighborhood? Time to take this pizza circus to {target_expansion_name}!",
                 f"🍕 Plantin' the flag in {target_expansion_name}! More ovens, more money!"
            ]
            await update.message.reply_html(random.choice(fun_messages))
            # Notify about completed expansion challenges
            await send_challenge_notifications(user.id, completed_challenges, context)
            # Check achievements AFTER replying about expansion
            await check_and_notify_achievements(user.id, context)
        else:
            # Send the error message from game.expand_shop
            await update.message.reply_html(message)

    except Exception as e:
        logger.error(f"Error during expand_command for {user.id}, location {target_expansion_name}: {e}", exc_info=True)
        await update.message.reply_text("Whoa there! Somethin' went sideways tryin' to expand.")

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
            lines.append(f"    Reward: {challenge['reward_value']} {challenge['reward_type'].upper()}")
        else:
            # This case should ideally not happen now, but keep as fallback
            lines.append("  Error generating challenge. Check logs or try again later.")

    await update.message.reply_html("\n".join(lines))

# --- New Leaderboard Command --- #
async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the global leaderboard."""
    user = update.effective_user
    if not user:
        return
    logger.info(f"User {user.id} requested leaderboard.")
    await update_player_display_name(user.id, user) # Ensure name is updated

    try:
        top_players = game.get_leaderboard_data(limit=10) # Get Top 10

        if not top_players:
            await update.message.reply_text("The leaderboard is empty! Be the first!")
            return

        lines = ["<b>🏆 Global Pizza Empire Leaderboard 🏆</b>\n(Based on Total Income Earned)\n"] # Add emoji
        for i, (player_id, display_name, total_income) in enumerate(top_players):
            rank = i + 1
            name = display_name or f"Player {player_id}" # Fallback if name is missing
            # Truncate long names if needed
            if len(name) > 25:
                 name = name[:22] + "..."
            lines.append(f"{rank}. {name} - ${total_income:,.2f}")

        await update.message.reply_html("\n".join(lines))

    except Exception as e:
        logger.error(f"Error generating leaderboard: {e}", exc_info=True)
        await update.message.reply_text("Couldn't fetch the leaderboard right now, try again later.")

# --- Help Command --- #
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a helpful message listing available commands."""
    user = update.effective_user
    logger.info(f"User {user.id if user else 'Unknown'} requested help.")

    help_text = (
        "🍕 <b>Pizza Empire Command Guide</b> 🍕\n\n"
        "<b>Core Gameplay:</b>\n"
        "/start - Initialize your pizza empire (or view this message again).\n"
        "/status - Check your cash, shops, title, and achievements.\n"
        "/collect - Scoop up the cash your shops have earned!\n"
        "/upgrade [shop] - List upgrade options or upgrade a specific shop (e.g., `/upgrade Brooklyn`).\n"
        "/expand [location] - List expansion options or expand to a new location (e.g., `/expand Manhattan`).\n\n"
        "<b>Progression & Fun:</b>\n"
        "/challenges - View your current daily and weekly challenges.\n"
        "/leaderboard - See who's top dog on the global leaderboard.\n"
        "/buycoins - View options to purchase Pizza Coins 🍕 (premium currency).\n"
        # Add /boost here if/when implemented
        "/help - Show this command guide.\n\n"
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

def main() -> None:
    """Start the bot and scheduler."""
    logger.info("Building Telegram Application...")
    # Pass context=application for scheduler jobs to access bot
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
    # application.add_handler(CommandHandler("boost", boost_command)) # Placeholder boost command

    logger.info("Adding payment handlers...")
    application.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))

    logger.info("Adding unknown command handler...")
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    # --- Add Mafia Callback Handler --- #
    application.add_handler(CallbackQueryHandler(mafia_button_callback, pattern="^mafia_(pay|refuse)$"))

    # Schedule challenge generation jobs
    logger.info("Setting up scheduled jobs...")
    try:
        # Run daily at 00:01 UTC
        scheduler.add_job(generate_daily_challenges_job, CronTrigger(hour=0, minute=1), args=[application])
        # Run weekly on Monday at 00:05 UTC
        scheduler.add_job(generate_weekly_challenges_job, CronTrigger(day_of_week='mon', hour=0, minute=5), args=[application])
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

