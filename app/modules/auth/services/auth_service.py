from datetime import timedelta
from fastapi import HTTPException, status
from app.modules.auth.utils.security import verify_password, create_access_token
from app.modules.auth.services.user_service import UserService
from app.core.config import settings

class AuthService:
    def __init__(self, db):
        self.db = db
        self.user_service = UserService(db)

    def authenticate_user(self, email: str, password: str):
        user = self.user_service.get_user_by_email(email)
        
        if not user:
            return None
        
        if not verify_password(password, user["hashed_password"]):
            return None
        
        return user

    def login(self, email: str, password: str):
        user = self.authenticate_user(email, password)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Credenciales incorrectas"
            )
        
        if not user["is_active"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Usuario inactivo"
            )
        
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": user["username"]},
            expires_delta=access_token_expires
        )
        
        return {
            "access_token": access_token,
            "token_type": "bearer"
        }