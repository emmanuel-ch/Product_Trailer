""" decrement_increment.py
Defines classes Decrement and Incrment: Representation of a physical
product leaving a place (decrement) and landing in another.
"""

from dataclasses import dataclass
from datetime import datetime

@dataclass(slots=True)
class Decrement:
    mvt_index: int
    date: datetime
    company: str
    document: str
    po: str
    mvt_code: str
    sloc: str
    soldto: str
    sku: str
    batch: str
    qty: int

    def to_waypoint(self, mode='standard'):
        if mode == 'burnt':
            return [
                self.date,
                self.company,
                'BURNT ' + self.sloc,
                self.soldto,
                self.mvt_code,
                self.batch
                ]
        elif mode == 'PO part 1':
            return [
                self.date,
                self.company,
                'PO FROM ' + self.sloc + ', mvt ' + self.mvt_code,
                self.soldto,
                self.po,
                self.batch
                ]
        return [
            self.date,
            self.company,
            self.sloc,
            self.soldto,
            self.mvt_code,
            self.batch
            ]
    
