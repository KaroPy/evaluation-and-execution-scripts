def sanitize_account_name(name) -> str:
    name = name.replace(" ", "")
    name = name.replace("-", "")
    name = name.replace("ü", "ue")
    return name.lower()
