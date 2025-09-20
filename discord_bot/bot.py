import discord
from discord.ext import commands

# Initialize the bot
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='!', intents=intents)

# A simple on_ready event to confirm the bot is running
@bot.event
async def on_ready():
    print(f'Discord bot logged in as {bot.user.name} ({bot.user.id})')
    print('------')

# You can add other bot commands here if needed
@bot.command()
async def hello(ctx):
    await ctx.send("Hello! I'm ready to receive messages from the API.")