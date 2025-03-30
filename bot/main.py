import logging
import os
import glob # For finding player data files
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
             player_data = game.load_player_data(user_id) # Reload data

        player_data["last_login_time"] = dt_time.time()
        logger.info(f"Saving updated player data for {user.id}...")
        game.save_player_data(user.id, player_data)
        logger.info(f"Player data saved for {user.id}.")

        reply_message = (
            rf"Hi {user.mention_html()}! Welcome to 🍕 Pizza Wars! 🍕" \
            "\n\nReady to build your pizza empire? Here are the basic commands:" \
            "\n/status - Check your shops and cash" \
            "\n/collect - Collect earned income" \
            "\n/upgrade [location] - Upgrade a shop" \
            "\n/expand [location] - Open a shop in a new area" \
            "\n/challenges - View your active challenges" \
            "\n/buycoins - Purchase Pizza Coins" \
            "\n\nUse /status to see where you're at!"
        )

        logger.info(f"Attempting to send reply to {user.id}...")
        await update.message.reply_html(reply_message)
        logger.info(f"Reply sent successfully to {user.id}.")

        # Check initial achievements
        await check_and_notify_achievements(user.id, context)

    except Exception as e:
        logger.error(f"ERROR in start_command for user {user.id}: {e}", exc_info=True)

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

        if collected_amount > 0:
            await update.message.reply_html(f"Collected ${collected_amount:.2f}! 💰")
            # Check achievements and notify about completed challenges AFTER replying about collection
            await send_challenge_notifications(user.id, completed_challenges, context)
            await check_and_notify_achievements(user.id, context)
        else:
            await update.message.reply_html("No income to collect right now. Keep those ovens hot! 🔥")

    except Exception as e:
        logger.error(f"Error during collect_command for {user.id}: {e}", exc_info=True)
        await update.message.reply_text("An error occurred while collecting income.")

async def upgrade_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
         await update.message.reply_text("Cannot identify user.")
         return
    if not context.args:
        await update.message.reply_text("Please specify which shop to upgrade. Usage: /upgrade [location_name] (e.g., /upgrade Brooklyn)")
        return

    shop_name = " ".join(context.args).strip().title()
    logger.info(f"User {user.id} attempting to upgrade '{shop_name}'.")
    if not shop_name:
         await update.message.reply_text("Please specify which shop to upgrade. Usage: /upgrade [location_name]")
         return

    try:
        success, message, completed_challenges = game.upgrade_shop(user.id, shop_name)
        await update.message.reply_html(message)

        if success:
            # Check achievements and notify about completed challenges AFTER replying about upgrade
            await send_challenge_notifications(user.id, completed_challenges, context)
            await check_and_notify_achievements(user.id, context)

    except Exception as e:
        logger.error(f"Error during upgrade_command for {user.id}, shop {shop_name}: {e}", exc_info=True)
        await update.message.reply_text("An error occurred while upgrading the shop.")

async def expand_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
         await update.message.reply_text("Cannot identify user.")
         return
    if not context.args:
        await update.message.reply_text("Please specify which location to expand to. Usage: /expand [location_name] (e.g., /expand Manhattan)")
        return

    expansion_name = " ".join(context.args).strip().title()
    logger.info(f"User {user.id} attempting to expand to '{expansion_name}'.")
    if not expansion_name:
         await update.message.reply_text("Please specify which location to expand to. Usage: /expand [location_name]")
         return

    try:
        success, message = game.expand_shop(user.id, expansion_name)
        await update.message.reply_html(message)

        if success:
            # Check achievements AFTER replying about expansion
            await check_and_notify_achievements(user.id, context)

    except Exception as e:
        logger.error(f"Error during expand_command for {user.id}, location {expansion_name}: {e}", exc_info=True)
        await update.message.reply_text("An error occurred during expansion.")

async def challenges_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the player's active daily and weekly challenges and progress."""
    user = update.effective_user
    if not user:
        return
    logger.info(f"User {user.id} requested challenges.")
    player_data = game.load_player_data(user.id)
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
            lines.append("  None active. Check back later!")

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

