from importlib import import_module
from .common import load_registry


def get_product(product_code: str):
    reg = load_registry()
    prod = reg["products"].get(product_code)
    if not prod:
        raise ValueError(f"Unknown product_code: {product_code}")
    return prod


def get_loader_callable(product_code: str):
    prod = get_product(product_code)
    dotted = prod["loader"]  # "module.sub:func"
    mod_name, func_name = dotted.split(":")
    mod = import_module(mod_name)
    return getattr(mod, func_name), prod


