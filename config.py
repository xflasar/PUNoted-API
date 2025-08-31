# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# --- Xata Configuration ---
XATA_API_KEY = os.environ.get("XATA_API_KEY")
XATA_DATABASE_URL = os.environ.get("XATA_DATABASE_URL")

# --- New JWT Configuration ---
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
TOKEN_LIFESPAN_SECONDS = int(os.getenv("TOKEN_LIFESPAN_SECONDS", 31449600)) # 1 year
EMAIL_VERIFICATION_CODE_LIFESPAN_SECONDS = 60 * 60 * 24

# --- Xata Table Schema Definitions ---
# not used yet
USERS_TABLE_NAME = "users"
VERIFICATION_CODES_TABLE_NAME = "user_verification_codes"
