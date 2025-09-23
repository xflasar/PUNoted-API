# bot.py

import os
import discord
from discord.ext import commands
import asyncio
import threading
import logging

# Initialize the bot
intents = discord.Intents.default()
intents.message_content = True
intents.members = True 
bot = commands.Bot(command_prefix='!', intents=intents)

# Global variable to store the running bot instance
global_bot_instance = None
logger = logging.getLogger(__name__)

# A simple on_ready event to confirm the bot is running
@bot.event
async def on_ready():
    logger.info(f'Discord bot logged in as {bot.user.name} ({bot.user.id})')
    logger.info('------')
    global global_bot_instance
    global_bot_instance = bot # Store the running instance

@bot.command()
async def hello(ctx):
    await ctx.send("Hello! I'm ready to receive messages from the API.")

def run_bot():
    """
    A synchronous function to start the bot.
    This is called from a separate thread.
    """
    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        logger.error("❌ Error: 'DISCORD_BOT_TOKEN' environment variable is not set.")
        return
    bot.run(bot_token)
    
def get_bot() -> commands.Bot:
    """Returns the globally running bot instance."""
    return global_bot_instance