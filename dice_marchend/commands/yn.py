# commands/yn.py
import random
from ..utils import build_user_label

def handle(status, sheets, _cfg) -> str:
    # 호출한 러너 식별
    acct = status.get("account", {}).get("acct")
    _, runner = sheets.get_runner_row(acct)

    # 닉네임/아이디 표기 정책 반영
    conf = sheets.get_config()
    label = build_user_label(acct, runner.nickname, (conf.get("아이디_표기") or "hidden").lower())

    # 결과 (한국어 예/아니오)
    result = "Yes" if random.randint(0, 1) else "No"

    # 최종 메시지
    return f"{label}의 결과는 {result} 입니다."