from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Request
from dotenv import load_dotenv
load_dotenv()
import os

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES = 60

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_credentials(username: str, password: str) -> bool:
    """Check username and password against .env values."""
    return username == ADMIN_USERNAME and password == ADMIN_PASSWORD


def create_access_token(data: dict) -> str:
    """Create a signed JWT token."""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_admin(request: Request) -> bool:
    """Check if the current request has a valid admin session token."""
    token = request.cookies.get("admin_token")
    if not token:
        return False
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        return username == ADMIN_USERNAME
    except JWTError:
        return False