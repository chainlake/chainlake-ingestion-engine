import yaml
from pathlib import Path

_profiles_cache = None

def load_kafka_profiles():
    global _profiles_cache

    if _profiles_cache is not None:
        return _profiles_cache

    path = Path(__file__).parent / "kafka_profiles.yaml"

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    _profiles_cache = data.get("profiles", {})
    return _profiles_cache