import os
import asyncio
import threading
from typing import Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Request
from .bot import bot # Import the bot instance
from discord import Member, Guild
import re

# Create a FastAPI router
router = APIRouter()

# Get the Discord channel ID from an environment variable for security
discord_channel_id = int(os.getenv("DISCORD_CHANNEL_ID", "YOUR_CHANNEL_ID_HERE"))

def format_price(price: Optional[float]) -> str:
    """
    Formats a price to a string with comma separators and up to 2 decimal places.
    Matches the frontend's formatting.
    """
    if price is None or not isinstance(price, (int, float)):
        return "N/A ICA"
    
    if price == int(price):
        return f"{int(price):,} ICA"
    else:
        return f"{price:,.2f} ICA"

def format_amount(amount: Optional[int]) -> str:
    """
    Formats an amount to a string with comma separators and no decimal places.
    Matches the frontend's formatting.
    """
    if amount is None or not isinstance(amount, int):
        return "N/A"
    
    return f"{amount:,}"

# Helper function to send messages to Discord
async def send_discord_message(message_content: str):
    """Asynchronously sends a message to the configured Discord channel."""
    try:
        channel = bot.get_channel(discord_channel_id)
        if channel:
            await channel.send(message_content)
            print(f"✅ Successfully sent message to Discord.")
            return True
        else:
            print(f"❌ Error: Discord channel with ID {discord_channel_id} not found.")
            return False
    except Exception as e:
        print(f"❌ Error sending message to Discord: {e}")
        return False

# Function to search for a user in a guild by their in-game name
async def get_user_by_ingamename(guild: Guild, ingamename: str) -> Optional[Member]:
    """
    Asynchronously fetches a user from a guild by matching their display name or username.
    This is a resource-intensive operation on large guilds.
    """
    async for member in guild.fetch_members(limit=None):
        if member.display_name == ingamename or member.name == ingamename:
            return member
    return None

@router.post("/buy_order")
async def discord_buy_order_webhook(payload: dict):
    """
    Handles a buy order request, finds and pings users by their names, and sends a formatted message to Discord.
    """
    customer_ingamename = payload.get("customer", "Unknown Customer")
    vendors_data = payload.get("vendors", [])

    if not isinstance(vendors_data, list):
        raise HTTPException(status_code=422, detail="Invalid payload format. 'vendors' must be a list.")

    message_content = "🛒 **New Buy Order**\n\n"

    guild = bot.guilds[0]
    if guild:
        # Get the Discord user for the customer
        future_customer = asyncio.run_coroutine_threadsafe(
            get_user_by_ingamename(guild, customer_ingamename),
            bot.loop
        )
        customer_member = future_customer.result(timeout=5)
    else:
        customer_member = None

    for vendor_data in vendors_data:
        vendor_id = vendor_data.get("vendorid", "N/A")
        vendor_ingamename = vendor_data.get("gamename", "Unknown Vendor")
        orders = vendor_data.get("orders", [])
        
        if guild:
            # Get the Discord user for the vendor
            future_vendor = asyncio.run_coroutine_threadsafe(
                get_user_by_ingamename(guild, vendor_ingamename),
                bot.loop
            )
            vendor_member = future_vendor.result(timeout=5)
        else:
            print(f"Guild with ID {vendor_id} not found.")
            vendor_member = None
            customer_member = None

        vendor_mention = f"<@!{vendor_member.id}>" if vendor_member else vendor_ingamename
        customer_mention = f"<@!{customer_member.id}>" if customer_member else customer_ingamename
        
        message_content += f"Customer: {customer_mention}\n\n"
        message_content += f"**Vendor:** {vendor_mention}\n"
        
        if not orders:
            message_content += "  - No orders for this vendor.\n"
        else:
            for order in orders:
                ticker = order.get("ticker", "N/A")
                amount = order.get("amount", "N/A")
                price = order.get("price", "N/A")
                message_content += f"  -> {ticker} - {format_amount(amount)} - {format_price(price)}\n"
        
        message_content += "\n" # Add a newline between vendors

    # Safely run the coroutine to send the message on the bot's event loop
    future_send = asyncio.run_coroutine_threadsafe(
        send_discord_message(message_content),
        bot.loop
    )
    
    try:
        success = future_send.result(timeout=5)
        if success:
            return {"status": "success", "message": "Buy order message sent to Discord."}
        else:
            return {"status": "error", "message": "Discord channel not found."}, 404
    except Exception as e:
        print(f"Error sending message to Discord: {e}")
        return {"status": "error", "message": str(e)}, 500

@router.post("/test")
async def discord_test_webhook():
    """
    Handles a test request from the API and sends a confirmation message to Discord.
    """
    try:
        channel = bot.get_channel(discord_channel_id)
        if channel:
            message_content = "Test accepted from **HTTP API**."
            await channel.send(message_content)
            return {"status": "success", "message": "Test message sent to Discord."}
        else:
            print(f"Error: Discord channel with ID {discord_channel_id} not found.")
            return {"status": "error", "message": "Discord channel not found."}, 404

    except Exception as e:
        print(f"Error processing test webhook: {e}")
        return {"status": "error", "message": str(e)}, 400

# The original generic webhook endpoint (unmodified)
@router.post("/webhook")
async def discord_generic_webhook(request: Request):
    """
    The original generic webhook endpoint for other data.
    """
    try:
        data = await request.json()
        message_content = f"**New message from API:**\n```json\n{data}\n```"
        
        future = asyncio.run_coroutine_threadsafe(
            send_discord_message(message_content),
            bot.loop
        )
        
        success = future.result(timeout=5)
        if success:
            return {"status": "success", "message": "Message sent to Discord."}
        else:
            return {"status": "error", "message": "Discord channel not found."}, 404
    except Exception as e:
        print(f"Error processing generic webhook: {e}")
        return {"status": "error", "message": str(e)}, 500