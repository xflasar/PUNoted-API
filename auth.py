import json
import logging
import random
import re
import secrets
import smtplib
import string
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

import jwt
from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    Cookie,
    WebSocket,
    status,
)
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, OAuth2PasswordBearer
from pydantic import BaseModel
from starlette.websockets import WebSocketState
from werkzeug.security import check_password_hash, generate_password_hash

try:
    from jwt.exceptions import ExpiredSignatureError, InvalidTokenError
except ImportError:
    from jwt import ExpiredSignatureError, InvalidTokenError
import config

auth_router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

logger = logging.getLogger(__name__)


# --- Email Sending Function (For Account Verification/Registration) ---
def send_verification_email(email: str, code: str):
    """
    Sends a general verification email using GMAIL's SMTP.
    """
    try:
        # 1. Email Construction
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Email Verification for Your Account"
        msg["From"] = f"PUNoted <{config.SMTP_USERNAME}>"
        msg["To"] = email

        html_content = f"""
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
            """
        msg.attach(MIMEText(html_content, "html"))

        # 2. SMTP Connection and Sending
        with smtplib.SMTP_SSL(config.SMTP_SERVER, 465, timeout=120) as server:
            server.login(config.SMTP_USERNAME, config.SMTP_PASSWORD)
            server.sendmail(config.SMTP_USERNAME, email, msg.as_string())

        print(f"Verification email sent to {email} via Gmail SMTP.")
        return True
    except Exception as e:
        print(f"Failed to send verification email to {email}: {e}")
        return False


# -----------------------------------------------------------------


# --- Email Sending Function for PASSWORD RESET (Using Gmail SMTP) ---
def send_password_reset_email(email: str, code: str):
    """
    Sends a password reset code email using GMAIL's SMTP.

    Args:
        email (str): The recipient's email address.
        code (str): The password reset code to send.
    """
    try:
        # 1. Email Construction
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Password Reset Request - ACTION REQUIRED"
        msg["From"] = f"PUNoted <{config.SMTP_USERNAME}>"
        msg["To"] = email

        html_content = f"""
            <!doctype html>
            <html>
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
                <title>Password Reset</title>
                <style>
                    body {{ font-family: sans-serif; line-height: 1.6; color: #333; }}
                    .container {{ max-width: 600px; margin: auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #f9f9f9; }}
                    h1 {{ font-size: 24px; color: #b30000; margin-bottom: 20px; }}
                    p {{ margin-bottom: 10px; }}
                    /* Warning box style for reset code */
                    .code-box {{ background-color: #fce4e4; border: 1px solid #b30000; color: #b30000; padding: 15px; border-radius: 5px; font-size: 20px; font-weight: bold; text-align: center; margin: 20px 0; }}
                    .warning {{ color: #b30000; font-weight: bold; }}
                    .footer {{ font-size: 12px; color: #777; text-align: center; margin-top: 30px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Action Required: Password Reset</h1>
                    <p>We received a request to reset the password for your account associated with this email address.</p>
                    <p>To continue, please use the following **one-time code** on the password reset page:</p>
                    <div class="code-box">
                        {code}
                    </div>
                    <p class="warning">⚠️ IMPORTANT: If you did not request a password reset, please ignore this email immediately. Your password will remain unchanged.</p>
                    <p>This code will expire in {config.EMAIL_VERIFICATION_CODE_LIFESPAN_SECONDS / 60:.0f} minutes.</p>
                    <div class="footer">
                        <p>&copy; {datetime.now().year} PUNoted. All rights reserved.</p>
                    </div>
                </div>
            </body>
            </html>
            """
        msg.attach(MIMEText(html_content, "html"))

        # 2. SMTP Connection and Sending
        with smtplib.SMTP_SSL(config.SMTP_SERVER, 465, timeout=120) as server:
            server.login(config.SMTP_USERNAME, config.SMTP_PASSWORD)
            server.sendmail(config.SMTP_USERNAME, email, msg.as_string())

        print(f"Password reset email sent to {email} via Gmail SMTP.")
        return True
    except Exception as e:
        print(f"Failed to send password reset email to {email}: {e}")
        return False


# ==============================================================================
# JWT Token Generation and Validation Functions
# ==============================================================================


async def generate_auth_tokens(conn, user_id: str, request: Request, is_website: bool = False) -> tuple[str, str, int]:
    """
    Generates both a short-lived Access Token and a long-lived Refresh Token.
    Returns: (access_token, refresh_token, access_expires_at_timestamp)
    """
    user_agent = request.headers.get("user-agent", "Unknown Browser")
    x_forwarded_for = request.headers.get("X-Forwarded-For")
    ip_address = x_forwarded_for.split(",")[0].strip() if x_forwarded_for else (request.client.host or "N/A")
    
    now_aware = datetime.now(timezone.utc)
    
    # Lifespans
    access_lifespan = timedelta(days=7) if is_website else timedelta(days=365)
    refresh_lifespan = timedelta(days=30)
    
    access_expires_dt = now_aware + access_lifespan
    refresh_expires_dt = now_aware + refresh_lifespan

    # Generate Access Token
    access_payload = {
        "user_id": user_id,
        "type": "access" if is_website else "extension_access",
        "iat_id": secrets.token_hex(8),
        "exp": access_expires_dt,
        "iat": now_aware.timestamp()
    }
    access_token = jwt.encode(access_payload, config.JWT_SECRET_KEY, algorithm="HS256")

    # Generate Refresh Token (Only for website)
    refresh_token = ""
    if is_website:
        refresh_payload = {
            "user_id": user_id,
            "type": "refresh",
            "iat_id": secrets.token_hex(8),
            "exp": refresh_expires_dt,
            "iat": now_aware.timestamp()
        }
        refresh_token = jwt.encode(refresh_payload, config.JWT_SECRET_KEY, algorithm="HS256")

    try:
        # Store the REFRESH token in the DB (or the access token for extensions)
        db_token = refresh_token if is_website else access_token
        db_token_type = "refresh" if is_website else "extension_access"
        db_expires_naive = (refresh_expires_dt if is_website else access_expires_dt).replace(tzinfo=None)
        
        await conn.execute(
            """
            WITH inserted AS (
                INSERT INTO user_tokens (userid, type, token, expiresat, user_agent, last_ip, iat_id, xata_updatedat)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                RETURNING userid, type
            )
            DELETE FROM user_tokens
            WHERE id IN (
                SELECT id FROM user_tokens
                WHERE userid = (SELECT userid FROM inserted)
                  AND type = (SELECT type FROM inserted)
                ORDER BY expiresat DESC
                OFFSET 10
            ) OR (userid = $1 AND expiresat < NOW());
            """,
            user_id, db_token_type, db_token, db_expires_naive, user_agent, ip_address, access_payload["iat_id"]
        )
        return access_token, refresh_token, int(access_expires_dt.timestamp())
    except Exception as e:
        print(f"Database error during token generation: {e}")
        return "", "", 0

async def validate_token(conn, token: str) -> tuple[str, int, None] | tuple[None, None, str]:
    """
    Validates a JWT. 
    - Web Access Tokens are validated statelessly (cryptographically).
    - Refresh and Extension Tokens are validated statefully (checked against the DB).
    """
    try:
        # 1. Cryptographic Validation (Checks signature and 'exp' date)
        decoded_payload = jwt.decode(token, config.JWT_SECRET_KEY, algorithms=["HS256"], leeway=5)
    except ExpiredSignatureError:
        logger.warning("Token expired")
        return None, None, "Token expired"
    except InvalidTokenError as e:
        logger.warning(f"Invalid token: {e}")
        return None, None, f"Invalid token: {e}"

    user_id_from_jwt = decoded_payload.get("user_id")
    token_type = decoded_payload.get("type")
    
    if not user_id_from_jwt or token_type not in ["access", "extension_access", "refresh"]:
        return None, None, "Invalid token payload."

    if token_type == "access":
        return user_id_from_jwt, int(decoded_payload["exp"]), None

    db_token_record = await conn.fetchrow(
        """
        SELECT id, userid, expiresat
        FROM user_tokens
        WHERE token = $1 AND type = $2;
        """,
        token, token_type
    )

    if not db_token_record:
        return None, None, "Token revoked or not found in database."

    if str(db_token_record["userid"]) != str(user_id_from_jwt):
        return None, None, "Token mismatch: User ID does not match database record."

    db_expires_at_dt = db_token_record["expiresat"]

    if datetime.now(timezone.utc).replace(tzinfo=None) > db_expires_at_dt.replace(tzinfo=None):
        await conn.execute("DELETE FROM user_tokens WHERE id = $1;", db_token_record["id"])
        return None, None, "Token expired (database check)."

    return user_id_from_jwt, int(db_expires_at_dt.timestamp()), None

# ==============================================================================
# Email Verification Code Functions
# ==============================================================================


def generate_verification_code_str(length: int = 6) -> str:
    """Generates a random alphanumeric verification code."""
    characters = string.ascii_uppercase + string.digits
    return "".join(random.choice(characters) for i in range(length))


async def store_verification_code(conn, email: str, code: str, server_code: str) -> Optional[str]:
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
        INSERT INTO user_verification_codes (email, code, servercode, expiresat)
        VALUES ($1, $2, $3, $4)
        RETURNING id;
        """
        record_id = await conn.fetch_rows(insert_query, email, code, server_code, expires_at_dt)

        if record_id:
            print(f"Stored verification code for {email}. Expires at: {expires_at_dt.isoformat()}")
            return record_id[0]["id"]
        else:
            raise Exception("Insert operation returned no ID.")
    except Exception as e:
        print(f"Error storing verification code for {email}: {e}")
        return None


async def get_verification_code_from_db(conn, email: str, code: str) -> tuple[str, None] | tuple[None, str]:
    """
    Asynchronously retrieves and validates a verification code from the database.

    Returns: A tuple containing the record ID of the code if valid and an error message if not.
    """
    try:
        query = """
        SELECT id, expiresat FROM user_verification_codes
        WHERE email = $1 AND code = $2;
        """
        record = await conn.fetch_rows(query, email, code)

        if not record:
            return None, "Invalid email or verification code."

        db_expires_at_dt = record[0]["expiresat"]

        if datetime.now(db_expires_at_dt.tzinfo) > db_expires_at_dt:
            # Code expired, delete it
            await conn.execute("DELETE FROM user_verification_codes WHERE id = $1;", record[0]["id"])
            return None, "Verification code expired."

        return str(record[0]["id"]), None  # Return the record ID if valid
    except Exception as e:
        print(f"Error retrieving/validating verification code for {email}: {e}")
        return None, f"Server error during code validation: {e}"


async def delete_verification_code(conn, record_id: str) -> bool:
    """
    Asynchronously deletes a verification code record from the database.

    Returns:
        bool: True if deletion was successful, False otherwise.
    """
    try:
        status_string = await conn.execute("DELETE FROM user_verification_codes WHERE id = $1;", int(record_id))

        # Check if any rows were actually deleted. The status will be "DELETE 1" for one row.
        if status_string.split()[-1] == "0":
            print(f"Warning: Verification code record '{record_id}' not found for deletion.")
            return False

        print(f"Deleted verification code record '{record_id}'.")
        return True
    except Exception as e:
        print(f"Error deleting verification code record '{record_id}': {e}")
        return False


async def get_current_user_id(request: Request, token: str = Depends(oauth2_scheme)) -> str:
    pool = request.app.state.db.pool

    async with pool.acquire() as conn:
        user_id, expires_at, error = await validate_token(conn, token)
        if error is not None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=error,
                headers={"WWW-Authenticate": "Bearer"},
            )
        if user_id.startswith("rec_"):
            user_id = await conn.fetch("SELECT accountid FROM users WHERE xata_id=$1", user_id)
            user_id = str(user_id[0]["accountid"])

        return user_id


async def get_current_user_id_ws(websocket: WebSocket) -> str:
    """
    Manually extracts the token from the WebSocket query params and validates it.
    This works when called directly inside the websocket endpoint.
    """
    # 1. Manually get token string from QueryParams
    #    This fixes the issue where 'token' was a Query() object or None.
    token = websocket.query_params.get("token")
    if not token:
        print("WS Auth Failed: Missing 'token' query parameter")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Missing token")
        raise Exception("Missing token")

    # 2. Access DB pool from App State
    pool = websocket.app.state.db.pool

    try:
        async with pool.acquire() as conn:
            # 3. Validate Token
            user_id, expires_at, error = await validate_token(conn, token)
            if error is not None:
                print(f"WS Token Error: {error}")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=f"Auth failed: {error}")
                raise Exception(error)

            # 4. Handle 'rec_' ID conversion (if using Xata IDs) - This is old and needs to be fixed in DB and backend no more xata and rec_
            if user_id.startswith("rec_"):
                row = await conn.fetchrow("SELECT accountid FROM users WHERE xata_id=$1", user_id)
                if row:
                    user_id = str(row["accountid"])
                else:
                    await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User mismatch")
                    raise Exception("User record not found")

            return user_id

    except Exception as e:
        print(f"WS Auth Exception: {e}")
        # Only close if not already closed
        if websocket.client_state != WebSocketState.DISCONNECTED:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        raise


class RequireAuth:
    def __init__(
        self,
        required_permissions: Optional[List[str]] = None,
        is_single_user_endpoint: bool = False
    ):
        self.required_permissions = required_permissions or []
        self.is_single_user_endpoint = is_single_user_endpoint

    async def __call__(
        self,
        request: Request,
        token_header: Optional[str] = Header(None, alias="X-Data-Token"),
        token_query: Optional[str] = Query(None, alias="token"),
    ) -> str:
        raw_token = token_header or token_query
        if not raw_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required.",
            )

        final_token = raw_token.removeprefix("Bearer ")
        pool = request.app.state.db.pool

        q_params = request.query_params
        requested_users_str = q_params.get("usernames") or q_params.get("username")
        requested_users = [u.strip() for u in requested_users_str.split(",") if u.strip()] if requested_users_str else None

        async with pool.acquire() as conn:
            req_perm = self.required_permissions[0] if self.required_permissions else None

            # ==================================================================
            # PATH A: STATELESS JWT (Web Access Tokens)
            # ==================================================================
            try:
                user_id, _, error = await validate_token(conn, final_token)
                
                if user_id and not error:
                    request.state.rate_limit_key = user_id
                    me_username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)
                    
                    if requested_users:
                        sql_targets = """
                            SELECT DISTINCT u.username
                            FROM users u
                            LEFT JOIN users_data ud ON ud.userid = u.userdataid
                            WHERE (
                                u.accountid = $1
                                OR EXISTS (
                                    SELECT 1
                                    FROM data_group_members gm_target
                                    JOIN data_group_members gm_requester ON gm_requester.group_id = gm_target.group_id
                                    WHERE gm_target.user_id = u.accountid
                                        AND gm_requester.user_id = $1
                                        AND gm_requester.status = 'ACCEPTED'
                                        AND gm_requester.can_read_data = TRUE
                                        AND gm_target.status = 'ACCEPTED'
                                        AND ($2::text IS NULL OR gm_target.granted_permissions @> jsonb_build_array($2::text) OR gm_target.granted_permissions @> '["all"]')
                                )
                            )
                            AND (u.username = ANY($3::text[]) OR ud.displayname = ANY($3::text[]))
                        """
                        valid_rows = await conn.fetch(sql_targets, user_id, req_perm, requested_users)
                        valid_usernames = [r["username"] for r in valid_rows]

                    else:
                        if self.is_single_user_endpoint:
                            valid_usernames = [me_username] if me_username else []
                        else:
                            sql_targets = """
                                SELECT DISTINCT u.username
                                FROM users u
                                WHERE (
                                    u.accountid = $1
                                    OR EXISTS (
                                        SELECT 1
                                        FROM data_group_members gm_target
                                        JOIN data_group_members gm_requester ON gm_requester.group_id = gm_target.group_id
                                        WHERE gm_target.user_id = u.accountid
                                            AND gm_requester.user_id = $1
                                            AND gm_requester.status = 'ACCEPTED'
                                            AND gm_requester.can_read_data = TRUE
                                            AND gm_target.status = 'ACCEPTED'
                                            AND ($2::text IS NULL OR gm_target.granted_permissions @> jsonb_build_array($2::text) OR gm_target.granted_permissions @> '["all"]')
                                    )
                                )
                            """
                            valid_rows = await conn.fetch(sql_targets, user_id, req_perm)
                            valid_usernames = [r["username"] for r in valid_rows]

                    request.state.valid_target_users = valid_usernames
                    return user_id
            except Exception:
                pass

            # ==================================================================
            # PATH B: GROUP TOKEN (Stateful DB Lookup)
            # ==================================================================
            if "grp_" in final_token:
                try:
                    access_key, user_suffix = final_token.rsplit("-", 1)
                except ValueError:
                    raise HTTPException(status_code=403, detail="Invalid token format")

                sql_requester = """
                    SELECT m.user_id, m.status, m.can_read_data, g.id as group_id, u.username, u.accountid
                    FROM data_sharing_groups g
                    JOIN data_group_members m ON m.group_id = g.id
                    JOIN users u ON u.accountid = m.user_id
                    WHERE g.access_key = $1 AND m.personal_suffix = $2
                """
                member = await conn.fetchrow(sql_requester, access_key, user_suffix)

                if not member or member["status"] != 'ACCEPTED':
                    raise HTTPException(status_code=403, detail="Invalid Group Token")

                group_id = member["group_id"]
                requester_username = member["username"]
                requester_id = str(member["accountid"])
                can_read_data = member["can_read_data"]

                if requested_users:
                    sql_targets = """
                        SELECT u.username
                        FROM data_group_members gm
                        JOIN users u ON u.accountid = gm.user_id
                        LEFT JOIN users_data ud ON ud.userid = u.userdataid
                        WHERE gm.group_id = $1
                        AND gm.status = 'ACCEPTED'
                        AND (u.username = ANY($2::text[]) OR ud.displayname = ANY($2::text[]))
                        AND ($3::text IS NULL OR gm.granted_permissions @> jsonb_build_array($3::text) OR gm.granted_permissions @> '["all"]')
                        AND ($4::boolean IS TRUE OR gm.user_id = $5)
                    """
                    valid_rows = await conn.fetch(sql_targets, group_id, requested_users, req_perm, can_read_data, requester_id)
                    valid_usernames = [r["username"] for r in valid_rows]

                else:
                    if self.is_single_user_endpoint:
                        valid_usernames = [requester_username]
                    else:
                        sql_targets = """
                            SELECT u.username
                            FROM data_group_members gm
                            JOIN users u ON u.accountid = gm.user_id
                            WHERE gm.group_id = $1
                            AND gm.status = 'ACCEPTED'
                            AND ($2::text IS NULL OR gm.granted_permissions @> jsonb_build_array($2::text) OR gm.granted_permissions @> '["all"]')
                            AND ($3::boolean IS TRUE OR gm.user_id = $4)
                        """
                        valid_rows = await conn.fetch(sql_targets, group_id, req_perm, can_read_data, requester_id)
                        valid_usernames = [r["username"] for r in valid_rows]

                        if not valid_usernames and requester_username:
                            valid_usernames = [requester_username]

                request.state.valid_target_users = valid_usernames
                request.state.rate_limit_key = requester_id
                return requester_id

            # ==================================================================
            # PATH C: PERSONAL API TOKEN (Stateful DB Lookup)
            # ==================================================================
            else:
                query = """
                    SELECT user_id, permissions, allow_group_access
                    FROM user_api_tokens
                    WHERE token_hash = $1 AND group_id IS NULL
                """
                token_data = await conn.fetchrow(query, final_token)
                if not token_data:
                    raise HTTPException(status_code=401, detail="Invalid data token")

                user_id = str(token_data["user_id"])
                allow_group_access = token_data.get("allow_group_access", False)

                raw_perms = token_data["permissions"]
                if isinstance(raw_perms, str):
                    try:
                        parsed_list = json.loads(raw_perms)
                    except json.JSONDecodeError:
                        parsed_list = []
                elif isinstance(raw_perms, list):
                    parsed_list = raw_perms
                else:
                    parsed_list = []

                user_permissions = frozenset([str(p).strip().lower() for p in parsed_list])

                if self.required_permissions and "all" not in user_permissions:
                    req_perms_lower = {p.lower() for p in self.required_permissions}
                    if not user_permissions.issuperset(req_perms_lower):
                        missing = req_perms_lower - user_permissions
                        raise HTTPException(
                            status_code=403, 
                            detail=f"Missing required permissions: {', '.join(missing)}"
                        )

                me_username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)

                if allow_group_access:
                    if requested_users:
                        sql_targets = """
                            SELECT DISTINCT u.username
                            FROM users u
                            LEFT JOIN users_data ud ON ud.userid = u.userdataid
                            WHERE (
                                u.accountid = $1
                                OR EXISTS (
                                    SELECT 1
                                    FROM data_group_members gm_target
                                    JOIN data_group_members gm_requester ON gm_requester.group_id = gm_target.group_id
                                    WHERE gm_target.user_id = u.accountid
                                        AND gm_requester.user_id = $1
                                        AND gm_requester.status = 'ACCEPTED'
                                        AND gm_requester.can_read_data = TRUE
                                        AND gm_target.status = 'ACCEPTED'
                                        AND ($2::text IS NULL OR gm_target.granted_permissions @> jsonb_build_array($2::text) OR gm_target.granted_permissions @> '["all"]')
                                )
                            )
                            AND (u.username = ANY($3::text[]) OR ud.displayname = ANY($3::text[]))
                        """
                        valid_rows = await conn.fetch(sql_targets, user_id, req_perm, requested_users)
                        valid_usernames = [r["username"] for r in valid_rows]

                    else:
                        if self.is_single_user_endpoint:
                            valid_usernames = [me_username] if me_username else []
                        else:
                            sql_targets = """
                                SELECT DISTINCT u.username
                                FROM users u
                                WHERE (
                                    u.accountid = $1
                                    OR EXISTS (
                                        SELECT 1
                                        FROM data_group_members gm_target
                                        JOIN data_group_members gm_requester ON gm_requester.group_id = gm_target.group_id
                                        WHERE gm_target.user_id = u.accountid
                                            AND gm_requester.user_id = $1
                                            AND gm_requester.status = 'ACCEPTED'
                                            AND gm_requester.can_read_data = TRUE
                                            AND gm_target.status = 'ACCEPTED'
                                            AND ($2::text IS NULL OR gm_target.granted_permissions @> jsonb_build_array($2::text) OR gm_target.granted_permissions @> '["all"]')
                                    )
                                )
                            """
                            valid_rows = await conn.fetch(sql_targets, user_id, req_perm)
                            valid_usernames = [r["username"] for r in valid_rows]

                else:
                    if requested_users:
                        sql_self = """
                            SELECT u.username
                            FROM users u
                            LEFT JOIN users_data ud ON ud.userid = u.userdataid
                            WHERE u.accountid = $1
                            AND (u.username = ANY($2::text[]) OR ud.displayname = ANY($2::text[]))
                        """
                        valid_rows = await conn.fetch(sql_self, user_id, requested_users)
                        valid_usernames = [r["username"] for r in valid_rows]
                    else:
                        valid_usernames = [me_username] if me_username else []

                request.state.valid_target_users = valid_usernames
                request.state.rate_limit_key = user_id

                return user_id

# --- Optional Auth ---
class OptionalAuth(RequireAuth):
    """
    Returns None if no token provided (Public Access).
    Validates token if provided (Authenticated Access).
    """
    async def __call__(
        self,
        request: Request,
        token_header: Optional[str] = Header(None, alias="X-Data-Token"),
        token_query: Optional[str] = Query(None, alias="token"),
    ) -> Optional[str]:
        raw_token = token_header or token_query

        if not raw_token:
            return None

        return await super().__call__(request, token_header, token_query)

# ==============================================================================
# Auth Routes
# ==============================================================================

security = HTTPBearer()

ALGORITHM = "HS256"
EXTENSION_TOKEN_EXPIRE_DAYS = 30
class SyncResponse(BaseModel):
    success: bool
    token: str
    username: str
    expires_at: int

@auth_router.post("/extension_sync")
async def extension_sync(
    request: Request,
    x_extension_client: str = Header(None),
    auth: HTTPAuthorizationCredentials = Depends(security)
):
    allowed_clients = {"PrunDataExtension", "PrunDataExtension-Chrome", "PrunDataExtension-Firefox"}

    if x_extension_client not in allowed_clients:
        raise HTTPException(status_code=403, detail="Unauthorized client")

    async with request.app.state.db.pool.acquire() as conn:
        # Validate the WEB token
        user_id, _, error = await validate_token(conn, auth.credentials)
        if error or not user_id:
            raise HTTPException(status_code=401, detail=error or "Invalid web token")

        user_record = await conn.fetchrow("SELECT username FROM users WHERE accountid = $1", user_id)
        if not user_record:
            raise HTTPException(status_code=404, detail="User not found")

        # GENERATE EXTENSION TOKEN (is_website=False)
        token, _, expires_at_ts = await generate_auth_tokens(conn, user_id, request, is_website=False)

        return {
            "success": True,
            "token": token,
            "username": user_record["username"],
            "expires_at": expires_at_ts
        }
    
@auth_router.post("/refresh")
async def refresh_access_token(request: Request, response: Response, refresh_token: Optional[str] = Cookie(None)):
    print("\n--- DEBUG REFRESH ENDPOINT ---")
    print(f"Incoming Cookies: {request.cookies}")
    
    if not refresh_token:
        print("FAILURE: The browser did not send the 'refresh_token' cookie!")
        raise HTTPException(status_code=401, detail="Refresh token missing")

    async with request.app.state.db.pool.acquire() as conn:
        user_id, _, error = await validate_token(conn, refresh_token)
        
        if error or not user_id:
            print(f"FAILURE: Database validation failed. Error: {error}")
            response.delete_cookie("refresh_token")
            raise HTTPException(status_code=401, detail=f"Invalid token: {error}")
        
        print("SUCCESS: Token is valid. Generating new tokens...")
        
        new_access, new_refresh, expires_at = await generate_auth_tokens(conn, user_id, request, is_website=True)
        
        # Hardcode secure=False for local dev
        is_secure = request.url.scheme == "https" and request.client.host not in ["127.0.0.1", "localhost"]
        
        response.set_cookie(
            key="refresh_token",
            value=new_refresh,
            httponly=True,
            secure=is_secure, 
            samesite="lax",
            max_age=30 * 24 * 60 * 60, 
        )
        
        return {"success": True, "token": new_access, "expires_at": expires_at}

@auth_router.post("/logout")
async def logout(response: Response, request: Request, refresh_token: str = Cookie(None)):
    """Clears the cookie and deletes it from the database."""
    if refresh_token:
        async with request.app.state.db.pool.acquire() as conn:
            await conn.execute("DELETE FROM user_tokens WHERE token = $1", refresh_token)
            
    response.delete_cookie("refresh_token")
    return {"success": True, "message": "Logged out successfully"}

@auth_router.post("/register")
async def register(user_data: Dict[str, str], request: Request):
    """
    Handles user registration.
    Expects JSON with 'username', 'email', and 'password'.
    Generates a verification code and sends it to the user's email.
    Stores user with 'isverified' as False.
    """
    conn = request.app.state.db

    username = re.sub(r"[^-a-z0-9_]", "", user_data.get("username", "").lower())
    email = user_data.get("email")
    password = user_data.get("password")

    if not all([username, email, password]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username, email, and password are required.",
        )

    # Check if user with this username or email already exists
    existing_users = await conn.fetch_rows(
        "SELECT username, email FROM users WHERE username = $1 OR email = $2 LIMIT 1;",
        username,
        email,
    )

    for record in existing_users:
        if record["username"] == username:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Username already exists.",
            )
        if record["email"] == email:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Email already registered.",
            )

    password_hash = generate_password_hash(password)

    # Add user to the database with isverified=False
    user_id = await conn.fetch_rows(
        "INSERT INTO users (username, email, password_hash, isverified) VALUES ($1, $2, $3, $4) RETURNING accountid;",
        username,
        email,
        password_hash,
        False,
    )
    if not user_id:
        raise Exception("Failed to get user ID after registration.")

    # Generate and store verification code
    verification_code = generate_verification_code_str()
    server_code = generate_verification_code_str()
    code_stored_id = await store_verification_code(conn, email, verification_code, server_code)

    sent_email = send_verification_email(email, verification_code)
    if not sent_email or not code_stored_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Registration successful, but failed to send verification email. Please contact support.",
        )

    print(f"User '{username}' registered with ID: {user_id[0]['accountid']}. Verification email sent to {email}.")
    return {
        "success": True,
        "message": "Registration successful! Please check your email for a verification code.",
    }


@auth_router.post("/verify_email")
async def verify_email(verification_data: Dict[str, str], request: Request):
    """
    Handles email verification.
    Expects JSON with 'email' and 'code'.
    Validates the code and updates user's 'is_verified' status.
    """
    conn = request.app.state.db
    email = verification_data.get("email")
    code = verification_data.get("code")

    if not all([email, code]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email and verification code are required.",
        )

    # Validate the code against the database
    code_record_id, validation_error = await get_verification_code_from_db(conn, email, code)

    if validation_error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=validation_error)

    # Find the user by email to update their verification status
    user_record = await conn.fetch_rows("SELECT accountid, isverified FROM users WHERE email = $1;", email)

    if not user_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found.")

    if user_record[0].get("isverified"):
        # User is already verified, delete the code and inform
        await delete_verification_code(conn, code_record_id)
        return {"success": True, "message": "Email already verified."}

    # Update user's is_verified status to True
    await conn.execute(
        "UPDATE users SET isverified = TRUE WHERE accountid = $1;",
        user_record[0]["accountid"],
    )

    default_web_settings = [
        {
            "page_context": "DASHBOARD",
            "preferences": {"site_data": True, "ships_data": True, "flight_data": True}
        },
        {
            "page_context": "CORP_PAGE",
            "preferences": {"storage_data": True, "site_data": True, "production_data": True}
        },
        {
            "page_context": "COOPERATION",
            "preferences": {"site_data": True, "storage_data": True, "production_data": True}
        }
    ]

    settings_query = """
        INSERT INTO user_web_settings (user_id, page_context, preferences, updated_at)
        VALUES ($1, $2, $3::jsonb, NOW())
        ON CONFLICT DO NOTHING
    """
    for setting in default_web_settings:
        await conn.execute(
            settings_query,
            user_record[0]["accountid"],
            setting["page_context"],
            json.dumps(setting["preferences"])
        )

    # Delete the used verification code
    await delete_verification_code(conn, code_record_id)

    print(f"Email '{email}' successfully verified.")
    return {"success": True, "message": "Email successfully verified!"}


@auth_router.post("/forget_password")
async def forget_password(forgot_data: Dict[str, str], request: Request):
    """
    Handles password reset requests.
    Generates a verification code and sends it to the user's email.
    """
    conn = request.app.state.db
    email = forgot_data.get("email")

    if not email:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email is required.")

    # Check if a user with this email exists
    user = await conn.fetch_rows("SELECT accountid FROM users WHERE email = $1;", email)
    if not user:
        return {
            "success": True,
            "message": "If this email is registered, a verification code has been sent.",
        }

    # Generate and store a new verification code
    verification_code = generate_verification_code_str()
    server_code = generate_verification_code_str()
    code_stored_id = await store_verification_code(conn, email, verification_code, server_code)

    if not code_stored_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store verification code.",
        )

    send_email = send_password_reset_email(email, verification_code)
    if not send_email:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send verification email. Please contact support.",
        )

    # Send the email with the code (placeholder, needs real server)

    print(f"Password reset code '{verification_code}' sent to {email}.")
    return {
        "success": True,
        "message": "A verification code has been sent to your email.",
    }


@auth_router.post("/code_verification")
async def verify_forgot_password_code(verification_data: Dict[str, str], request: Request):
    """
    Verifies the code sent for the password reset.
    """
    conn = request.app.state.db
    code = verification_data.get("code")

    if not code:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Verification code is required.",
        )

    verification_record = await conn.fetch_rows("SELECT * FROM user_verification_codes WHERE code = $1;", code)

    if not verification_record:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid verification code.",
        )

    # Check if the code has expired
    if datetime.now() > verification_record[0]["expiresat"]:
        await delete_verification_code(conn, str(verification_record[0]["id"]))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Verification code has expired.",
        )

    return {"success": True, "message": "Code verified successfully."}


@auth_router.post("/forget_password_set_new_password")
async def set_new_password(password_data: Dict[str, str], request: Request):
    """
    Sets a new password after a successful verification code check.
    """
    conn = request.app.state.db
    code = password_data.get("code")
    new_password = password_data.get("newPassword")

    if not code or not new_password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Verification code and new password are required.",
        )

    # Validate the code one more time and get the email associated with it
    verification_record = await conn.fetch_rows(
        "SELECT id, email FROM user_verification_codes WHERE code = $1;", code
    )

    if not verification_record:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid verification code.",
        )

    email = verification_record[0]["email"]
    password_hash = generate_password_hash(new_password)
    await conn.execute(
        "UPDATE users SET password_hash = $1 WHERE email = $2;",
        password_hash,
        email,
    )

    # Delete the used verification code
    await delete_verification_code(conn, str(verification_record[0]["id"]))

    return {"success": True, "message": "Password updated successfully."}


@auth_router.post("/login")
async def login(login_data: Dict[str, Any], request: Request, response: Response):
    raw_identifier = login_data.get("username", "")
    password = login_data.get("password")
    is_web_raw = login_data.get("isWebsite")
    is_web_bool = str(is_web_raw).lower() == "true"

    # 1. Sanitize Username/Email
    if "@" in raw_identifier:
        username = raw_identifier.lower()
    else:
        username = re.sub(r"[^-a-z0-9_]", "", raw_identifier.lower())

    if not username or not password:
        raise HTTPException(status_code=400, detail="Username and password are required.")

    async with request.app.state.db.pool.acquire() as conn:
        # 2. Select the correct Query based on source
        if is_web_bool:
            query = """
                SELECT us.accountid, us.username, us.password_hash, us.isverified, 
                        COALESCE(ud.displayname, us.displayname) AS displayname,
                        cd.companyname, cd.companycode, c.name as corpname, us.is_synchronized
                FROM users AS us
                LEFT JOIN users_data AS ud ON ud.userid = us.userdataid
                LEFT JOIN company_data AS cd ON cd.userdataid = ud.userid
                LEFT JOIN corporation_shareholders cs ON cs.companyid = ud.companyid
                LEFT JOIN corporations c ON c.id = cs.corporationid
                WHERE us.username = $1 OR us.email = $1;
            """
        else:
            query = "SELECT accountid, username, password_hash, isverified FROM users WHERE username = $1 OR email = $1;"

        users = await conn.fetch(query, username)

        # 3. Validate Password
        if users and check_password_hash(users[0]["password_hash"], password):
            user = users[0]
            if not user["isverified"]:
                raise HTTPException(status_code=403, detail="Please verify your email address first.")

            # 4. Generate Tokens
            access_token, refresh_token, expires_at = await generate_auth_tokens(conn, str(user["accountid"]), request, is_website=is_web_bool)

            if not access_token:
                raise HTTPException(status_code=500, detail="Failed to generate token")

            # 5. Set the Refresh Token as a secure HttpOnly cookie
            if is_web_bool:
                response.set_cookie(
                    key="refresh_token",
                    value=refresh_token,
                    httponly=True,
                    secure=True,
                    samesite="lax",
                    max_age=30 * 24 * 60 * 60, # 30 days in seconds
                )

            # 6. Build Response (Only send access_token in the JSON body)
            json_response = {
                "success": True,
                "message": "Login successful",
                "token": access_token,
                "expires_at": expires_at,
                "username": user["username"],
            }

            if is_web_bool:
                json_response.update({
                    "displayName": user["displayname"],
                    "companyName": user.get("companyname"),
                    "companyCode": user.get("companycode"),
                    "currentUserId": user["accountid"],
                    "corpName": user.get("corpname"),
                    "isSynchronized": user.get("is_synchronized"),
                })

            return json_response

        else:
            raise HTTPException(status_code=403, detail="Invalid username or password")


@auth_router.put("/change-password-final")
async def change_password_final(
    password_data: Dict[str, str],
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    """
    Handles password change final steps.
    """
    db = request.app.state.db

    current_password = password_data.get("currentPassword")
    new_password = password_data.get("newPassword")
    verification_code = password_data.get("verificationCode")

    if not current_password or not new_password or not verification_code:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password, new password, and verification code are required.",
        )

    async with db.pool.acquire() as conn:
        # Check if the verification code is correct AND belongs to the user
        verification_record = await conn.fetchrow(
            "SELECT * FROM user_verification_codes uvc INNER JOIN users u ON u.email = uvc.email WHERE uvc.code = $1 AND u.accountid = $2;",
            verification_code,
            user_id,
        )

        if not verification_record:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid verification code.",
            )

        if datetime.now() > verification_record["expiresat"]:
            await delete_verification_code(db, str(verification_record["id"]))
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Verification code has expired.",
            )

        password_hash = generate_password_hash(new_password)
        await conn.execute(
            "UPDATE users SET password_hash = $1 WHERE accountid = $2;",
            password_hash,
            user_id,
        )

        # Delete code after successful use
        await conn.execute(
            "DELETE FROM user_verification_codes WHERE id = $1",
            verification_record["id"],
        )

        return {"success": True, "message": "Password changed successfully."}


@auth_router.post("/change-password")
async def change_password(request: Request, user_id: str = Depends(get_current_user_id)):
    """
    Handles password change requests.
    """
    if not user_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email is required.")

    conn = await request.app.state.db.pool.acquire()
    # Check if a user with this email exists
    email = await conn.fetch("SELECT email FROM users WHERE accountid = $1;", user_id)
    if not email:
        return {
            "success": True,
            "message": "If this email is registered, a verification code has been sent.",
        }

    email = email[0]["email"]

    # Generate and store a new verification code
    verification_code = generate_verification_code_str()
    server_code = generate_verification_code_str()
    code_stored_id = await store_verification_code(conn, email, verification_code, server_code)

    if not code_stored_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store verification code.",
        )

    send_email = send_password_reset_email(email, verification_code)
    if not send_email:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send verification email. Please contact support.",
        )

    # Send the email with the code

    print(f"Password change code '{verification_code}' sent to {email}.")
    return {
        "success": True,
        "message": "A verification code has been sent to your email.",
    }