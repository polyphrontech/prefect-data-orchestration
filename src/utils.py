# You can add helper functions here, e.g. for logging or validation.

def normalize_country_name(name: str) -> str:
    """Normalize country names (basic example)."""
    return name.strip().title()
