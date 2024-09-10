import jwt


def get_user_id(blade_auth):
    # noinspection PyBroadException
    try:
        if not blade_auth:
            return None
        token_split = blade_auth.split(" ")
        if not token_split or len(token_split) != 2:
            return None
        payload = jwt.decode(token_split[1], options={"verify_signature": False})
        if not payload or "user_id" not in payload:
            return None
        return payload["user_id"]
    except Exception:
        return None
