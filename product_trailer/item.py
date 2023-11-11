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

    def to_tuple(self):
        return (
            self.ini_country,
            self.sku,
            self.qty,
            self.open,
            self.waypoints,
            self.unit_value,
            self.brand,
            self.category
        )