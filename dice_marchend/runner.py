import logging
from mastodon import Mastodon
from .config import Config
from .sheets import Sheets
from .bot import DiceListener

def main():
    cfg = Config()
    logging.basicConfig(level=getattr(logging, cfg.LOG_LEVEL))
    api = Mastodon(
        api_base_url=cfg.BASE_URL,
        access_token=cfg.ACCESS_TOKEN,
        ratelimit_method="pace",
    )
    sheets = Sheets(cfg)
    listener = DiceListener(api, sheets, cfg)
    api.stream_user(listener)

if __name__ == "__main__":
    main()
