"""
Auth routes: /auth/register, /auth/login.
"""
from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel, EmailStr
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()
from auth import (
    get_password_hash,
    verify_password,
    create_access_token,
    get_user_from_token,
    db_get_user_by_email,
    db_create_user,
)

router = APIRouter(prefix="/auth", tags=["auth"])


# ── Request/Response models ───────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: EmailStr
    password: str




class RegisterResponse(BaseModel):
    id: int
    email: str
    message: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class MeResponse(BaseModel):
    id: int
    email: str


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/register", response_model=RegisterResponse)
def register(body: RegisterRequest):
    existing = db_get_user_by_email(body.email)
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")

    pwd_hash = get_password_hash(body.password)
    user = db_create_user(body.email, pwd_hash)
    return RegisterResponse(id=user["id"], email=user["email"], message="registered")


@router.post("/login", response_model=LoginResponse)
def login(body: LoginRequest):
    user = db_get_user_by_email(body.email)
    if not user or not verify_password(body.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token(user["id"], user["email"])
    return LoginResponse(access_token=token)


# ── Protected dependency ───────────────────────────────────────────────────────


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    token = credentials.credentials
    user = get_user_from_token(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return user


@router.get("/me", response_model=MeResponse)
def me(user: dict = Depends(get_current_user)):
    """Protected endpoint — returns the current user info."""
    return MeResponse(id=user["id"], email=user["email"])
