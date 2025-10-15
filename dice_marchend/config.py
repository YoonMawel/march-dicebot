from dataclasses import dataclass
import os

@dataclass
class Config:
    BASE_URL: str = os.environ.get("MASTODON_BASE_URL", "https://marchen1210d.site/")
    ACCESS_TOKEN: str = os.environ.get("MASTODON_ACCESS_TOKEN", "5H_uN8qmeruAsc66EGfoN3Fed2TsnHwm5LDd4LXgqko")
    SHEET_NAME: str = os.environ.get("DICE_SHEET_NAME", "다이스")
    SHOP_SHEET_NAME: str = os.environ.get("SHOP_SHEET_NAME", "상점")
    SHOP_BAG_WS: str = os.environ.get("SHOP_BAG_WS", "가방")
    USER_COLUMN_STYLE: str = os.environ.get("USER_COLUMN_STYLE", "without_at")  # with_at | without_at
    TIMEZONE: str = os.environ.get("TZ", "Asia/Seoul")
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO")
    CREDS_PATH: str = os.environ.get("GOOGLE_APPLICATIONS_CREDENTIALS") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "march-credential.json")
