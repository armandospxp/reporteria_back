from starlette import status
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.middleware.base import RequestResponseEndpoint, BaseHTTPMiddleware
from fastapi_users import jwt
from fastapi.responses import JSONResponse

from auth.auth import SECRET_KEY, verificar_usuario


class VerificadorToken(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        if not request.url.path.__contains__("/login"):
            try:
                # Obtener el encabezado de autorización de la solicitud
                authorization: str = request.headers.get("Authorization")
                if not authorization or "bearer" not in authorization.lower():
                    return JSONResponse(status_code=401, content="Se requiere un token de autenticación")
                    # return HTTPException(
                    #     status_code=status.HTTP_401_UNAUTHORIZED,
                    #     detail="Se requiere un token de autenticación",
                    #     headers={"WWW-Authenticate": "Bearer"},
                    # )
                scheme, token = authorization.split(" ")

                # Verificar que el esquema de autorización sea Bearer

                if scheme.lower() != "bearer":
                    # return HTTPException(
                    #     status_code=status.HTTP_401_UNAUTHORIZED,
                    #     detail="Esquema de autorización inválido",
                    #     headers={"WWW-Authenticate": "Bearer"},
                    # )
                    return JSONResponse(status_code=401, content="Esquema de autorización inválido")
                # Decodificar y verificar el token
                decoded_token = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])

                # Aquí puedes hacer cualquier validación adicional con los datos decodificados del token
                verificar_usuario(token)

            except (jwt.exceptions.DecodeError, jwt.exceptions.InvalidTokenError, AttributeError):
                return JSONResponse(status_code=401, content="Token Inválido")
                # return HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token Inválido")
        response = await call_next(request)
        return response
