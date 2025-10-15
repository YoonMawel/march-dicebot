from datetime import datetime
from ..utils import today_ymd, build_user_label

def handle(status, sheets, cfg, is_allowed: bool, root_id: str) -> str:
    if not is_allowed:
        return "참여 확인은 지정된 공지에 대한 답글로만 인정됩니다."
    acct = status.get("account", {}).get("acct")
    conf = sheets.get_config()

    with sheets.atomic():
        # 공지별 중복 방지도 같은 락에서!
        if root_id and sheets.has_participation("확인", root_id, acct):
            return "이미 해당 이벤트의 참여 확인이 되었습니다."

        row_idx, runner = sheets.get_runner_row(acct)
        hp = int(conf.get("확인_기숙사점수", 1))

        sheets.update_runner_points(row_idx, runner.house_points + hp)
        sheets.update_runner_last_confirm(row_idx, today_ymd(cfg.TIMEZONE))

        coins = int(conf.get("확인_통화", 0))
        if coins:
            sheets.add_currency(acct, coins)

        # 참여기록 남기기까지 같은 락에서
        sheets.append_participation("확인", root_id or "", acct,
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    label = build_user_label(acct, runner.nickname, (conf.get("아이디_표기") or "hidden").lower())
    k = conf.get("통화키", "갈레온")
    tail = f" / {k} +{coins}" if coins else ""

    return f"{label}의 이벤트 참여 확인이 완료되었습니다. 기숙사 점수 +{hp}{tail}"
