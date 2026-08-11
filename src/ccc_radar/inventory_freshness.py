ENDPOINT_INVENTORY_SIGNATURE = "endpoint-inventory-v18-strategy1-kafka-message-service-topics"


def current_endpoint_inventory_signature() -> str:
    return ENDPOINT_INVENTORY_SIGNATURE


def is_endpoint_inventory_stale(stored_signature: str | None) -> bool:
    return stored_signature != ENDPOINT_INVENTORY_SIGNATURE


def endpoint_inventory_warning(
    stored_signature: str | None,
    *,
    scope: str = "ce projet",
    inventory_indexed: bool = True,
) -> str | None:
    if stored_signature is None and not inventory_indexed:
        return None
    if not is_endpoint_inventory_stale(stored_signature):
        return None
    observed = (
        "aucune signature stockée"
        if stored_signature is None
        else f"signature {stored_signature!r}"
    )
    return (
        f"{scope} : inventaire des intégrations potentiellement obsolète ({observed}) ; "
        "relancez `cccr index`."
    )
