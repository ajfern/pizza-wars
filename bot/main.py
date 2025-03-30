import logging
import os

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
    ShippingQueryHandler, # Not strictly needed now, but good to import
)

# Import game logic functions
import game # Corrected import

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
PAYMENT_PROVIDER_TOKEN = os.getenv("PAYMENT_PROVIDER_TOKEN") # Get payment token

if not BOT_TOKEN:
    logger.critical("TELEGRAM_BOT_TOKEN environment variable not set! Exiting.") # Use critical
    exit()

if not PAYMENT_PROVIDER_TOKEN:
    logger.warning("PAYMENT_PROVIDER_TOKEN environment variable not set! Payments will fail.")
    # We don't exit here, but real payments won't work

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
            "\n/buycoins - Purchase Pizza Coins (premium currency)" \
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

# --- Payment Handlers ---

async def buy_coins_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lists available coin packs and sends invoices upon selection (via callback query later)."""
    chat_id = update.message.chat_id
    message_text = "Choose a Pizza Coin pack to purchase:\n\n"

    if not PAYMENT_PROVIDER_TOKEN:
        await update.message.reply_text(
            "Sorry, the payment system is currently unavailable. Please try again later."
        )
        return

    # --- Send Invoices for each pack --- #
    # In a real bot, you might use Inline Keyboards + Callback Queries here
    # For simplicity now, we just send multiple invoices.

    for pack_id, (name, description, price_cents, coin_amount) in game.PIZZA_COIN_PACKS.items():
        title = f"{name} ({coin_amount} Coins)"
        payload = f"BUY_{pack_id.upper()}_{chat_id}" # Unique payload for this specific purchase intent
        currency = "USD"
        prices = [LabeledPrice(label=name, amount=price_cents)]

        logger.info(f"Sending invoice for {pack_id} to chat {chat_id}")
        try:
            await context.bot.send_invoice(
                chat_id=chat_id,
                title=title,
                description=description,
                payload=payload,
                provider_token=PAYMENT_PROVIDER_TOKEN,
                currency=currency,
                prices=prices,
                # Optional: Add need_name, need_phone_number, need_email etc. if required by provider
            )
        except Exception as e:
            logger.error(f"Failed to send invoice for {pack_id} to {chat_id}: {e}", exc_info=True)
            await update.message.reply_text(f"Sorry, couldn't start the purchase for {name}. Please try again.")

async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Answers the PreCheckoutQuery. Must be answered within 10 seconds."""
    query = update.pre_checkout_query
    # check the payload and see if we want to continue
    # The payload is BUY_{PACK_ID}_{USER_ID}
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
        # Price or pack definition might have changed, reject payment
        await query.answer(ok=False, error_message="Sorry, the price or item details have changed. Please try initiating the purchase again.")
    else:
        logger.info(f"PreCheckout OK for user {query.from_user.id}, pack {pack_id}")
        await query.answer(ok=True)

async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirms the successful payment and credits coins."""
    payment_info = update.message.successful_payment
    payload = payment_info.invoice_payload
    user_id = update.message.from_user.id
    amount_paid = payment_info.total_amount
    currency = payment_info.currency

    logger.info(
        f"Successful payment received! User: {user_id}, Amount: {amount_paid} {currency}, Payload: {payload}"
    )

    # Extract pack_id and coin amount from payload or look up via amount/payload
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
        else:
            logger.error(f"Could not find pack details for pack_id '{pack_id}' from successful payment payload: {payload}")
            await update.message.reply_text("Thank you for your purchase! There was an issue crediting the coins automatically, please contact support.")
    else:
        logger.error(f"Could not parse pack_id from successful payment payload: {payload}")
        await update.message.reply_text("Thank you for your purchase! There was an issue crediting the coins automatically, please contact support.")

# --- DEPRECATED Placeholder Premium Commands ---
# async def buy_coins_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None: ...
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
    application.add_handler(CommandHandler("buycoins", buy_coins_command))

    # Add payment handlers
    logger.info("Adding payment handlers...")
    application.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))

    # Add a handler for unknown commands (must be last)
    logger.info("Adding unknown command handler...")
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    # Run the bot until the user presses Ctrl-C
    logger.info("Starting Pizza Wars bot...")
    application.run_polling()


if __name__ == "__main__":
    main()

