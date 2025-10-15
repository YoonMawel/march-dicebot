from dataclasses import dataclass
from typing import Optional

@dataclass
class Runner:
    handle: str
    nickname: str
    dorm: str
    house_points: int
    last_attend_date: str
    last_confirm_date: str

@dataclass
class ExploreRow:
    area: str
    subarea: str
    place_script: str
    reward_type: str  # 소문 | 갈레온 | 아이템
    min_galleon: int
    max_galleon: int
    item_name: str
    item_qty: int
    rumor_script: str
