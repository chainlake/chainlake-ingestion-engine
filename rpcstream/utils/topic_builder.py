def build_topic(adapter_type: str, chain: str, network: str, schema: str) -> str:
    return f"{adapter_type}.{chain}.{network}.raw_{schema}"