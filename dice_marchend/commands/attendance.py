from datetime import datetime
from ..utils import today_ymd, build_user_label

def handle(status, sheets, cfg, is_allowed: bool, root_id: str) -> str:
    if not is_allowed:
        return "출석은 지정된 공지에 대한 답글로만 인정됩니다."

    acct = status.get("account", {}).get("acct")
    today = today_ymd(cfg.TIMEZONE)
    conf = sheets.get_config()

    with sheets.atomic():
        row_idx, runner = sheets.get_runner_row(acct)

        # 하루 1회 체크도 락 안에서!
        if (runner.last_attend_date or "") == today:
            return "이미 오늘 출석했습니다."

        hp = int(conf.get("출석_기숙사점수", 1))
        sheets.update_runner_points(row_idx, runner.house_points + hp)
        sheets.update_runner_last_attend(row_idx, today)

        coins = int(conf.get("출석_통화", 0))
        if coins:
            sheets.add_currency(acct, coins)

    # 메시지 구성은 락 밖에서
    label = build_user_label(acct, runner.nickname, (conf.get("아이디_표기") or "hidden").lower())
    k = conf.get("통화키", "갈레온")
    tail = f" / {k} +{coins}" if coins else ""
    return f"{label}의 출석이 완료되었습니다. 기숙사 점수 +{hp}{tail}"
