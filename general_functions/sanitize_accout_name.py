def sanitize_account_name(name) -> str:
    name = name.replace(" ", "")
    name = name.replace("-", "")
    name = name.replace("ü", "ue")
    name = name.replace("ö", "oe")
    return name.lower()
