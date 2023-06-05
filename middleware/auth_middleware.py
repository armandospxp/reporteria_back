from starlette import status

from auth.auth import verificar_usuario, SECRET_KEY
import jwt
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware


class VerificadorToken(BaseHTTPMiddleware):
    def __init__(
            self,
            app,
    ):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/secure"):
            try:
                # Obtener el encabezado de autorización de la solicitud
                authorization: str = request.headers.get("Authorization")
                scheme, token = authorization.split()

                # Verificar que el esquema de autorización sea Bearer
                if scheme.lower() != "bearer":
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Esquema de autorización inválido",
                        headers={"WWW-Authenticate": "Bearer"},
                    )

                # Decodificar y verificar el token
                decoded_token = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])

                # Aquí puedes hacer cualquier validación adicional con los datos decodificados del token
                verificar_usuario(token)

            except (jwt.exceptions.DecodeError, jwt.exceptions.InvalidTokenError):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Token inválido o expirado",
                    headers={"WWW-Authenticate": "Bearer"},
                )
        response = await call_next(request)
        return response
