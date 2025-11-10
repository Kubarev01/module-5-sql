import os
from typing import Dict, Any

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials



"""
docker-compose run --rm gateway python -c "import jwt, datetime as dt; print(jwt.encode({'sub': 'test-user', 'exp': dt.datetime.utcnow() + dt.timedelta(hours=1)}, 'secretexample', algorithm='HS256'))"
"""

bearer_scheme = HTTPBearer(auto_error=False)

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "dev-secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")


def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> Dict[str, Any]:
    """
    Depends-зависимость, которая:
    - достаёт Authorization: Bearer <token>
    - валидирует токен
    - при проблемах возвращает 401
    - при успехе возвращает payload (dict)
    """

    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    
    

    try:
        payload = jwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM],
        )    
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # if not payload.get("role")== "admin":
    #     raise HTTPException(
    #         status_code=status.HTTP_403_FORBIDDEN, #почему в задании 401 статус?
    #         detail="admin role required",
    #     )

    return payload