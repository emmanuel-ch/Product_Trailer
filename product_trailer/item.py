""" item.py
Defines class Item: Representation of a physical product with its
history of movements.

Class Item - methods:
"""

from dataclasses import dataclass

@dataclass(slots=True)
class Item:
    id: str
    ini_country: str
    sku: str
    qty: int
    open: bool
    waypoints: list
    unit_value: float
    brand: str
    category: str
