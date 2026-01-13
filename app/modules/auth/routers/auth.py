from fastapi import APIRouter, Depends, HTTPException
from app.modules.auth.schemas.auth import LoginRequest, Token
from app.modules.auth.schemas.user import UserCreate, UserResponse
from app.modules.auth.services.auth_service import AuthService
from app.modules.auth.services.user_service import UserService
from app.core.database import get_database

router = APIRouter(prefix="/auth", tags=["Authentication"])

@router.post("/register", response_model=UserResponse)
def register(user: UserCreate):
    db = get_database()
    user_service = UserService(db)
    
    try:
        created_user = user_service.create_user(user.model_dump())
        return created_user
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/login", response_model=Token)
def login(credentials: LoginRequest):
    db = get_database()
    auth_service = AuthService(db)
    
    return auth_service.login(credentials.email, credentials.password)