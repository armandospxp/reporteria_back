from auth.auth import verificar_usuario


# Middleware para verificar el token JWT en cada solicitud
@app.middleware("http")
async def verify_token(request: Request, call_next):
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