import logging
import os
import glob # For finding player data files
import random # For tips!
from datetime import time as dt_time, timedelta

# Telegram Core Types
from telegram import Update, LabeledPrice, ShippingOption, Invoice
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

# --- Scheduled Job Functions ---
async def generate_daily_challenges_job(context: ContextTypes.DEFAULT_TYPE):
    """Scheduled job to generate daily challenges for all known players."""
    logger.info("Running daily challenge generation job...")
    player_files = glob.glob(os.path.join(game.DATA_DIR, "*.json"))
    active_users_today = 0
    for filepath in player_files:
        try:
            user_id = int(os.path.basename(filepath).split('.')[0])
            # Optional: Add logic here to only generate for recently active players
            # player_data = game.load_player_data(user_id)
            # if time.time() - player_data.get('last_login_time', 0) < timedelta(days=7).total_seconds():
            game.generate_new_challenges(user_id, 'daily')
            active_users_today += 1
            # Notify user of new challenge?
            # try:
            #     player_data = game.load_player_data(user_id) # Reload to get new challenge
            #     challenge_desc = player_data.get("active_challenges", {}).get("daily", {}).get("description")
            #     if challenge_desc:
            #          await context.bot.send_message(user_id, f"☀️ Your new daily challenge is: \n"{challenge_desc}"")
            # except Exception as notify_err:
            #      logger.warning(f"Failed to notify user {user_id} of new daily challenge: {notify_err}")
        except (ValueError, Exception) as e:
            logger.error(f"Error processing daily challenge for file {filepath}: {e}")
    logger.info(f"Daily challenge generation complete. Generated for {active_users_today} users.")

async def generate_weekly_challenges_job(context: ContextTypes.DEFAULT_TYPE):
    """Scheduled job to generate weekly challenges for all known players."""
    logger.info("Running weekly challenge generation job...")
    player_files = glob.glob(os.path.join(game.DATA_DIR, "*.json"))
    active_users_week = 0
    for filepath in player_files:
        try:
            user_id = int(os.path.basename(filepath).split('.')[0])
            game.generate_new_challenges(user_id, 'weekly')
            active_users_week += 1
             # Notify user?
        except (ValueError, Exception) as e:
            logger.error(f"Error processing weekly challenge for file {filepath}: {e}")
    logger.info(f"Weekly challenge generation complete. Generated for {active_users_week} users.")

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    logger.info("Entered start_command")
    if not user:
        logger.warning("start_command called without user info")
        return

    logger.info(f"User {user.id} ({user.username}) triggered /start.")
    is_new_player = not game.get_player_data_path(user.id).exists()

    try:
        logger.info(f"Loading player data for {user.id}...")
        player_data = game.load_player_data(user.id)
        logger.info(f"Player data loaded for {user.id}.")

        # Generate initial challenges if new player
        if is_new_player:
             logger.info(f"New player {user.id}, generating initial challenges.")
             game.generate_new_challenges(user.id, 'daily')
             game.generate_new_challenges(user.id, 'weekly')
             player_data = game.load_player_data(user.id) # <-- Corrected to user.id

        player_data["last_login_time"] = game.time.time()
        logger.info(f"Saving updated player data for {user.id}...")
        game.save_player_data(user.id, player_data)
        logger.info(f"Player data saved for {user.id}.")

        # --- Saucy Onboarding Instructions --- #
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

        # --- FTUE: Show status immediately --- #
        logger.info(f"Sending initial status to new/returning player {user.id}")
        status_message = game.format_status(player_data) # Use reloaded data if new player
        await update.message.reply_html(status_message)
        # --- End FTUE --- #

        await check_and_notify_achievements(user.id, context)

    except Exception as e:
        logger.error(f"ERROR in start_command for user {user.id}: {e}", exc_info=True)
        await update.message.reply_text("Ay, somethin' went wrong gettin' ya started. Try /start again maybe?")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    logger.info(f"User {user.id} requested status.")
    player_data = game.load_player_data(user.id)
    status_message = game.format_status(player_data)
    await update.message.reply_html(status_message)

async def collect_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    logger.info(f"User {user.id} requested collection.")
    try:
        collected_amount, completed_challenges = game.collect_income(user.id)
        tip_message = ""
        pineapple_message = "" # <-- Initialize pineapple message

        if collected_amount > 0.01:
            # --- "Just the Tip" Mechanic --- #
            tip_chance = 0.15 # 15% chance of getting a tip
            if random.random() < tip_chance:
                player_data = game.load_player_data(user.id) # Need to reload to add tip
                tip_amount = round(random.uniform(collected_amount * 0.05, collected_amount * 0.2) + random.uniform(5, 50), 2)
                tip_amount = max(5.0, tip_amount) # Minimum tip
                player_data["cash"] = player_data.get("cash", 0) + tip_amount
                game.save_player_data(user.id, player_data) # Save tip addition
                tip_message = f"\n🍕 Woah, some wiseguy just tipped you an extra ${tip_amount:.2f} for the 'best slice in town.' You're killin' it!"
                logger.info(f"User {user.id} received a tip of ${tip_amount:.2f}")

            # --- Pineapple Easter Egg --- #
            pineapple_chance = 0.05 # 5% chance
            if random.random() < pineapple_chance:
                pineapple_message = "\n🍍 Psst... Remember, putting pineapple on your pizza may get you sent to the gulag."
                logger.info(f"User {user.id} triggered the pineapple easter egg.")

            # --- Cheeky Feedback --- #
            # Append both messages if they triggered
            await update.message.reply_html(f"🤑 Pizza payday, baby! You just grabbed ${collected_amount:.2f} fresh outta the oven!{tip_message}{pineapple_message}")

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
            upgrade_cost = game.get_upgrade_cost(current_level)
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
        current_level = shops[target_shop_name].get("level", 1) # Get level before potential success message
        success, message, completed_challenges = game.upgrade_shop(user.id, target_shop_name)

        # --- Cheeky Feedback --- #
        if success:
            fun_messages = [
                f"🍾 Hot dang! Your {target_shop_name} spot just hit Level {current_level + 1}. Lines around the block incoming!",
                f"🤌 Mama mia! {target_shop_name} is now Level {current_level + 1}! More dough, less problems!",
                f"🎉 Level {current_level + 1} for {target_shop_name}! You're cookin' with gas now!"
            ]
            await update.message.reply_html(random.choice(fun_messages))
            await send_challenge_notifications(user.id, completed_challenges, context)
            await check_and_notify_achievements(user.id, context)
        else:
            # Send the error message from game.upgrade_shop
            await update.message.reply_html(message)

    except Exception as e:
        logger.error(f"Error during upgrade_command for {user.id}, shop {target_shop_name}: {e}", exc_info=True)
        await update.message.reply_text("Ay caramba! Somethin' went wrong with the upgrade.")

async def expand_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
         await update.message.reply_text("Who dis? Can't expand if I dunno who you are.")
         return
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
    """Displays the player's active challenges, generating if missing."""
    user = update.effective_user
    if not user:
        return
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

# --- Payment Handlers ---
async def buy_coins_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.message.chat_id
    if not PAYMENT_PROVIDER_TOKEN:
        await update.message.reply_text(
            "My owner still hasn't signed up for a Stripe account. If you send him funds, he will send you a bajillion pizza coins."
        )
        return
    for pack_id, (name, description, price_cents, coin_amount) in game.PIZZA_COIN_PACKS.items():
        title = f"{name} ({coin_amount} Coins)"
        payload = f"BUY_{pack_id.upper()}_{chat_id}"
        currency = "USD"
        prices = [LabeledPrice(label=name, amount=price_cents)]
        logger.info(f"Sending invoice for {pack_id} to chat {chat_id}")
        try:
            await context.bot.send_invoice(
                chat_id=chat_id, title=title, description=description, payload=payload,
                provider_token=PAYMENT_PROVIDER_TOKEN, currency=currency, prices=prices,
            )
        except Exception as e:
            logger.error(f"Failed to send invoice for {pack_id} to {chat_id}: {e}", exc_info=True)
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
    application.add_handler(CommandHandler("challenges", challenges_command)) # Add challenges command
    application.add_handler(CommandHandler("buycoins", buy_coins_command))
    # application.add_handler(CommandHandler("boost", boost_command)) # Placeholder boost command

    logger.info("Adding payment handlers...")
    application.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))

    logger.info("Adding unknown command handler...")
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))

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

