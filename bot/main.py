import logging
import os

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Import game logic functions
from . import game # Use relative import

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Get the bot token from environment variable
# IMPORTANT: You'll need to set this environment variable.
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
    # You might want to exit here or handle it differently
    exit()

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /start command. Initializes player data if it doesn't exist."""
    user = update.effective_user
    logger.info("Entered start_command")
    if not user:
        logger.warning("start_command called without user info")
        return

    logger.info(f"User {user.id} ({user.username}) triggered /start.")

    try:
        # Load data - this will create default if not exists
        logger.info(f"Loading player data for {user.id}...")
        player_data = game.load_player_data(user.id)
        logger.info(f"Player data loaded for {user.id}.")

        # Update last login time
        player_data["last_login_time"] = game.time.time()
        logger.info(f"Saving updated player data for {user.id}...")
        game.save_player_data(user.id, player_data)
        logger.info(f"Player data saved for {user.id}.")

        reply_message = (
            rf"Hi {user.mention_html()}! Welcome to 🍕 Pizza Wars! 🍕" \
            "\n\nReady to build your pizza empire? Here are the basic commands:" \
            "\n/status - Check your shops and cash" \
            "\n/collect - Collect earned income" \
            "\n/upgrade [location] - Upgrade a shop (e.g., /upgrade Brooklyn)" \
            "\n/expand [location] - Open a shop in a new area (e.g., /expand Manhattan)" \
            "\n\nUse /status to see where you're at!"
        )

        logger.info(f"Attempting to send reply to {user.id}...")
        await update.message.reply_html(reply_message)
        logger.info(f"Reply sent successfully to {user.id}.")

    except Exception as e:
        logger.error(f"ERROR in start_command for user {user.id}: {e}", exc_info=True)
        # Optionally notify user of error
        # await update.message.reply_text("Oops! Something went wrong processing your request.")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /status command. Displays player status."""
    user = update.effective_user
    if not user:
        return

    logger.info(f"User {user.id} requested status.")
    player_data = game.load_player_data(user.id)
    status_message = game.format_status(player_data)
    await update.message.reply_html(status_message)

async def collect_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /collect command. Collects passive income."""
    user = update.effective_user
    if not user:
        return

    logger.info(f"User {user.id} requested collection.")
    collected_amount = game.collect_income(user.id)

    if collected_amount > 0:
        await update.message.reply_html(f"Collected ${collected_amount:.2f}! 💰")
    else:
        await update.message.reply_html("No income to collect right now. Keep those ovens hot! 🔥")
    # Optionally, show status after collection
    # player_data = game.load_player_data(user.id)
    # status_message = game.format_status(player_data)
    # await update.message.reply_html(status_message)


async def upgrade_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /upgrade [location] command."""
    user = update.effective_user
    if not user or not context.args:
        await update.message.reply_text("Please specify which shop to upgrade. Usage: /upgrade [location_name] (e.g., /upgrade Brooklyn)")
        return

    shop_name = " ".join(context.args).strip().title() # Join args, strip whitespace, title case
    logger.info(f"User {user.id} attempting to upgrade '{shop_name}'.")

    if not shop_name:
         await update.message.reply_text("Please specify which shop to upgrade. Usage: /upgrade [location_name]")
         return

    success, message = game.upgrade_shop(user.id, shop_name)
    await update.message.reply_html(message)


async def expand_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /expand [location] command."""
    user = update.effective_user
    if not user or not context.args:
        await update.message.reply_text("Please specify which location to expand to. Usage: /expand [location_name] (e.g., /expand Manhattan)")
        return

    expansion_name = " ".join(context.args).strip().title()
    logger.info(f"User {user.id} attempting to expand to '{expansion_name}'.")

    if not expansion_name:
         await update.message.reply_text("Please specify which location to expand to. Usage: /expand [location_name]")
         return

    success, message = game.expand_shop(user.id, expansion_name)
    await update.message.reply_html(message)

# --- Placeholder Premium Commands ---
async def buy_coins_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Placeholder for buying premium currency."""
    user = update.effective_user
    if not user:
        return
    # In a real scenario, you might parse args for amount or show options
    message = game.buy_pizza_coins(user.id, 100) # Example amount
    await update.message.reply_text(message)

async def boost_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Placeholder for using premium currency."""
    user = update.effective_user
    if not user:
        return
    # In a real scenario, you might parse args for boost type
    message = game.use_pizza_coins_for_speedup(user.id, "instant_collect") # Example feature
    await update.message.reply_text(message)

async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles unknown commands."""
    await update.message.reply_text("Sorry, I didn't understand that command. Try /start or /status.")

def main() -> None:
    """Start the bot."""
    logger.info("Building Telegram Application...")
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(BOT_TOKEN).build()
    logger.info("Telegram Application built successfully.")

    logger.info("Adding command handlers...")
    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("collect", collect_command))
    application.add_handler(CommandHandler("upgrade", upgrade_command))
    application.add_handler(CommandHandler("expand", expand_command))

    # Add placeholder premium command handlers
    application.add_handler(CommandHandler("buycoins", buy_coins_command))
    application.add_handler(CommandHandler("boost", boost_command))

    # Add a handler for unknown commands (must be last)
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    # Run the bot until the user presses Ctrl-C
    logger.info("Starting Pizza Wars bot...")
    application.run_polling()


if __name__ == "__main__":
    main()

