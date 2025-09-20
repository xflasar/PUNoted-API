# auth.py
import logging
from typing import Optional, Dict
from datetime import datetime, timedelta
import random
import string
import jwt
from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer
from werkzeug.security import generate_password_hash, check_password_hash

try:
    from jwt.exceptions import ExpiredSignatureError, InvalidTokenError
except ImportError:
    from jwt import ExpiredSignatureError, InvalidTokenError

import config
import mailtrap as mt

auth_router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

logger = logging.getLogger(__name__)


# --- Email Sending Function ---
def send_verification_email(email: str, code: str):
    """
    Sends a verification email using Mailtrap.

    Args:
        email (str): The recipient's email address.
        code (str): The verification code to send.
    """
    try:
        mail = mt.Mail(
            sender=mt.Address(email="punoted@noreply.com", name="PUNoted"),
            to=[mt.Address(email=email)],
            subject="Email Verification for Your Account",
            html=f"""
            <!doctype html>
            <html>
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
                <title>Email Verification</title>
                <style>
                    body {{ font-family: sans-serif; line-height: 1.6; color: #333; }}
                    .container {{ max-width: 600px; margin: auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #f9f9f9; }}
                    h1 {{ font-size: 24px; color: #0056b3; margin-bottom: 20px; }}
                    p {{ margin-bottom: 10px; }}
                    .code-box {{ background-color: #eee; padding: 15px; border-radius: 5px; font-size: 20px; font-weight: bold; text-align: center; margin: 20px 0; }}
                    .footer {{ font-size: 12px; color: #777; text-align: center; margin-top: 30px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Verify Your Email Address</h1>
                    <p>Thank you for registering with our service!</p>
                    <p>To complete your registration, please use the following verification code:</p>
                    <div class="code-box">
                        {code}
                    </div>
                    <p>This code will expire in {config.EMAIL_VERIFICATION_CODE_LIFESPAN_SECONDS / 60:.0f} minutes.</p>
                    <p>If you did not register for an account, please ignore this email.</p>
                    <div class="footer">
                        <p>&copy; {datetime.now().year} PUNoted. All rights reserved.</p>
                    </div>
                </div>
            </body>
            </html>
            """,
            category="Email Verification",
            headers={"X-MT-Header": "Email Verification"}, # Custom header for Mailtrap
            custom_variables={"year": datetime.now().year} # Dynamic custom variable
        )

        if not hasattr(config, 'MAILTRAP_API_TOKEN') or not config.MAILTRAP_API_TOKEN:
            print("Error: MAILTRAP_API_TOKEN is not configured in config.py. Email not sent.")
            return

        client = mt.MailtrapClient(token=config.MAILTRAP_API_TOKEN)
        client.send(mail)
        print(f"Verification email sent to {email}.")
    except Exception as e:
        print(f"Failed to send verification email to {email}: {e}")


# ==============================================================================
# JWT Token Generation and Validation Functions
# ==============================================================================

async def generate_token(conn, user_id: str) -> tuple[str, int]:
    """
    Generates a new JWT access token for a given user ID with a long expiration time.
    The token is stored in the 'user_tokens' table using asyncpg.

    Args:
        conn (asyncpg.Connection): An active database connection from the pool.
        user_id (str): The ID of the user for whom the token is generated.

    Returns:
        tuple[str, int]: A tuple containing:
            - The generated access token string.
            - Its expiration timestamp (int).
    """
    # Define expiration time (long-lived)
    expires_at_dt = datetime.now() + timedelta(seconds=config.TOKEN_LIFESPAN_SECONDS)
    expires_at_ts = int(expires_at_dt.timestamp())

    # Create JWT payload
    payload = {
        "user_id": user_id,
        "type": "access",
        "exp": expires_at_dt,
        "iat": datetime.now().timestamp()
    }

    try:
        # Encode the JWT
        token = jwt.encode(payload, config.JWT_SECRET_KEY, algorithm="HS256")
        
        await conn.execute(
            "INSERT INTO user_tokens (userid, token, expiresat) VALUES ($1, $2, $3);",
            user_id,
            token,
            expires_at_dt
        )
        print(f"Generated new JWT for user ID '{user_id}'. Expires at: {datetime.fromtimestamp(expires_at_ts)}.")
        return token, expires_at_ts
    except Exception as e:
        print(f"Error generating or storing token for user ID '{user_id}': {e}")
        return "", 0

async def validate_token(conn, token: str) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Asynchronously validates a JWT access token and checks its expiration against the database.

    Args:
        conn (asyncpg.Connection): The database connection object from an asyncpg pool.
        token (str): The JWT access token string to validate.

    Returns:
        tuple[Optional[str], Optional[int], Optional[str]]: A tuple containing:
            - The user ID (str) if valid.
            - The access token's expiration timestamp (int) from the DB record.
            - An error message (str) if not valid.
    """
    try:
        # Decode the JWT with a leeway to account for clock skew
        decoded_payload = jwt.decode(token, config.JWT_SECRET_KEY, algorithms=["HS256"], leeway=5)
        user_id_from_jwt = decoded_payload.get("user_id")
        token_type = decoded_payload.get("type")

        if not user_id_from_jwt or token_type != "access":
            return None, None, "Invalid token: User ID or token type missing/incorrect from payload."

        # Check if the token exists in the database
        db_token_record = await conn.fetch_rows(
            """
            SELECT id, userid, expiresat
            FROM user_tokens
            WHERE userid = $1;
            """,
            user_id_from_jwt
        )

        if not db_token_record:
            return None, None, "Access token not found in database (possibly revoked or never issued)."

        if db_token_record[0]['userid'] != user_id_from_jwt:
            return None, None, "Token mismatch: User ID in token does not match database record."
            
        db_expires_at_dt = db_token_record[0]['expiresat']
        
        # Check if the token has expired
        if datetime.now() > db_expires_at_dt:
            # Token expired, delete it from DB
            await conn.execute("DELETE FROM user_tokens WHERE id = $1;", db_token_record[0]['id'])
            return None, None, "Access token expired (database check)."
            
        return user_id_from_jwt, int(db_expires_at_dt.timestamp()), None

    except ExpiredSignatureError:
        logger.warning("Access token expired")
        return None, None, "Access token expired"
    except InvalidTokenError as e:
        logger.warning(f"Invalid access token: {e}")
        return None, None, f"Invalid access token: {e}"
    except Exception as e:
        logger.error(f"An unexpected error occurred during access token validation: {e}", exc_info=True)
        return None, None, f"Server error during access token validation: {e}"


# ==============================================================================
# Email Verification Code Functions
# ==============================================================================

def generate_verification_code_str(length: int = 6) -> str:
    """Generates a random alphanumeric verification code."""
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(characters) for i in range(length))

async def store_verification_code(conn, email: str, code: str) -> Optional[str]:
    """
    Stores a verification code in the database with an expiration.

    Args:
        email (str): The email address the code is for.
        code (str): The generated verification code.

    Returns:
        Optional[str]: The Xata record ID if successful, None otherwise.
    """
    expires_at_dt = datetime.now() + timedelta(seconds=config.EMAIL_VERIFICATION_CODE_LIFESPAN_SECONDS)

    try:
        # First, delete any existing codes for this email to ensure only one is active.
        delete_query = "DELETE FROM user_verification_codes WHERE email = $1;"
        await conn.execute(delete_query, email)
        print(f"Deleted old verification codes for {email}.")

        # Now, insert the new verification code.
        insert_query = """
        INSERT INTO user_verification_codes (email, code, expiresat)
        VALUES ($1, $2, $3)
        RETURNING id;
        """
        record_id = await conn.fetch_rows(insert_query, email, code, expires_at_dt)
        
        if record_id:
            print(f"Stored verification code for {email}. Expires at: {expires_at_dt.isoformat()}")
            return record_id[0]['id']
        else:
            raise Exception("Insert operation returned no ID.")
    except Exception as e:
        print(f"Error storing verification code for {email}: {e}")
        return None

async def get_verification_code_from_db(conn, email: str, code: str) -> tuple[Optional[str], Optional[str]]:
    """
    Asynchronously retrieves and validates a verification code from the database.

    Args:
        pool (asyncpg.Pool): The asyncpg connection pool.
        email (str): The email address.
        code (str): The code to validate.

    Returns:
        tuple[Optional[str], Optional[str]]: A tuple containing the record ID of the code
                                             if valid, and an error message if not.
    """
    try:
        query = """
        SELECT id, expiresat FROM user_verification_codes
        WHERE email = $1 AND code = $2;
        """
        record = await conn.fetch_rows(query, email, code)

        if not record:
            return None, "Invalid email or verification code."

        db_expires_at_dt = record[0]['expiresat']

        if datetime.now(db_expires_at_dt.tzinfo) > db_expires_at_dt:
            # Code expired, delete it
            await conn.execute("DELETE FROM user_verification_codes WHERE id = $1;", record[0]["id"])
            return None, "Verification code expired."
        
        return str(record[0]["id"]), None # Return the record ID if valid
    except Exception as e:
        print(f"Error retrieving/validating verification code for {email}: {e}")
        return None, f"Server error during code validation: {e}"

async def delete_verification_code(conn, record_id: str) -> bool:
    """
    Asynchronously deletes a verification code record from the database.

    Args:
        pool (asyncpg.Pool): The asyncpg connection pool.
        record_id (str): The ID of the verification code record.

    Returns:
        bool: True if deletion was successful, False otherwise.
    """
    try:
        status_string = await conn.execute("DELETE FROM user_verification_codes WHERE id = $1;", int(record_id))
        
        # Check if any rows were actually deleted. The status will be "DELETE 1" for one row.
        if status_string.split()[-1] == '0':
            print(f"Warning: Verification code record '{record_id}' not found for deletion.")
            return False
            
        print(f"Deleted verification code record '{record_id}'.")
        return True
    except Exception as e:
        print(f"Error deleting verification code record '{record_id}': {e}")
        return False

# dependency function for other routes (Currently running 500 instead of not auth will be fixed later)
async def get_current_user_id(request: Request,
    token: str = Depends(oauth2_scheme)) -> str:
    conn = request.state.db
    user_id, expires_at, error = await validate_token(conn ,token)
    if error:
        print("Error", error)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error,
            headers={"WWW-Authenticate": "Bearer"},
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=error,
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user_id

# ==============================================================================
# Auth Routes
# ==============================================================================

@auth_router.post('/register')
async def register(user_data: Dict[str, str], request: Request):
    """
    Handles user registration.
    Expects JSON with 'username', 'email', and 'password'.
    Generates a verification code and sends it to the user's email.
    Stores user with 'isverified' as False.
    """
    conn = request.state.db

    username = user_data.get('username', '').lower()
    email = user_data.get('email')
    password = user_data.get('password')

    if not all([username, email, password]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username, email, and password are required."
        )

    # Check if user with this username or email already exists
    existing_users = await conn.fetch_rows(
        "SELECT username, email FROM users WHERE username = $1 OR email = $2;",
        username,
        email
    )

    if existing_users:
        for record in existing_users:
            if record['username'] == username:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Username already exists."
                )
            if record['email'] == email:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Email already registered."
                )

    # Hash password before storing
    password_hash = generate_password_hash(password)

    try:
        # Add user to the database with isverified=False
        user_id = await conn.fetch_rows(
            "INSERT INTO users (username, email, password_hash, isverified) VALUES ($1, $2, $3, $4) RETURNING xata_id;",
            username, email, password_hash, False
        )
        if not user_id:
            raise Exception("Failed to get user ID after registration.")

        # Generate and store verification code
        verification_code = generate_verification_code_str()
        code_stored_id = await store_verification_code(conn, email, verification_code)

        if not code_stored_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Registration successful, but failed to send verification email. Please contact support."
            )

        # After getting email server uncomment this
        # Send verification email (placeholder)
        # send_verification_email(email, verification_code)

        print(f"User '{username}' registered with ID: {user_id[0]['xata_id']}. Verification email sent to {email}.")
        return {"success": True, "message": "Registration successful! Please check your email for a verification code."}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error during registration: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {e}"
        )

@auth_router.post('/verify_email')
async def verify_email(verification_data: Dict[str, str], request: Request):
    """
    Handles email verification.
    Expects JSON with 'email' and 'code'.
    Validates the code and updates user's 'is_verified' status.
    """
    conn = request.state.db
    email = verification_data.get('email')
    code = verification_data.get('code')

    if not all([email, code]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email and verification code are required."
        )

    try:
        # Validate the code against the database
        code_record_id, validation_error = await get_verification_code_from_db(conn, email, code)

        if validation_error:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=validation_error
            )

        # Find the user by email to update their verification status
        user_record = await conn.fetch_rows(
            "SELECT xata_id, isverified FROM users WHERE email = $1;",
            email
        )

        if not user_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found."
            )
        
        if user_record[0].get("isverified"):
            # User is already verified, delete the code and inform
            await delete_verification_code(conn, code_record_id)
            return {"success": True, "message": "Email already verified."}

        # Update user's is_verified status to True
        await conn.execute(
            "UPDATE users SET isverified = TRUE WHERE xata_id = $1;",
            user_record[0]["xata_id"]
        )

        # Delete the used verification code
        await delete_verification_code(conn, code_record_id)

        print(f"Email '{email}' successfully verified.")
        return {"success": True, "message": "Email successfully verified!"}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error during email verification: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Email verification failed: {e}"
        )


@auth_router.post('/login')
async def login(login_data: Dict[str, str], request: Request):
    """
    Handles user login requests.
    """
    conn = request.state.db
    username = login_data.get('username', '').lower()
    password = login_data.get('password')
    isWebsite = login_data.get('isWebsite')
    
    if not username or not password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username and password are required."
        )

    try:
        query = ""
        if isWebsite:
            query = f"""SELECT us.xata_id, us.username, us.password_hash, us.isverified, ud.displayname, cd.companyname, cd.companycode
                    FROM users as us
                    INNER JOIN users_data as ud ON ud.userid = us.userdataid
                    INNER JOIN company_data as cd ON cd.userdataid = ud.userid
                    WHERE username = $1;"""
        else:
            query = f"SELECT xata_id, username, password_hash, isverified FROM users WHERE username = $1;"
        users = await conn.fetch_rows(query, username)
        
        # Check if a user was found and if the password is correct
        if users and check_password_hash(users[0]['password_hash'], password):
            user = users[0]
            if not user['isverified']:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Please verify your email address first."
                )

            token, expires_at = await generate_token(conn ,str(user['xata_id']))
            if not token:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to generate token"
                )
            if isWebsite == "true":
                return {
                    "success": True,
                    "message": "Login successful",
                    "token": token,
                    "expires_at": expires_at,
                    "username": user['username'],
                    "displayName": user['displayname'],
                    "companyName": user['companyname'],
                    "companyCode": user['companycode']
                }
            else:
                return {
                    "success": True,
                    "message": "Login successful",
                    "token": token,
                    "expires_at": expires_at,
                    "username": user['username']
                }
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error during login: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during login."
        )