import jwt


with open("config/public.pem", "r") as f:
    PUBLIC_KEY = f.read()

def verify_jwt_token(token):
    try:
        payload = jwt.decode(
            token,
            PUBLIC_KEY,
            algorithms=["RS256"],
            audience="graphql-api",
            issuer="ZPC"
        )
        return payload, None
    except jwt.ExpiredSignatureError:
        return None, "Token has expired"
    except jwt.ImmatureSignatureError:
        return None, "Token not valid yet (nbf)"
    except jwt.InvalidTokenError as e:
        return None, f"Invalid token: {str(e)}"
    except Exception as e:
        return None, f"Token verification error: {str(e)}"