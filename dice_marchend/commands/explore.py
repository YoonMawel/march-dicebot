# commands/explore.py
import random
from datetime import datetime
from ..sheets import Sheets
from ..config import Config
from ..utils import normalize_path, path_parent, path_last

def _format_children_bullets(children):
    """자식 노드를 불릿 리스트로 예쁘게."""
    if not children:
        return ""
    lines = "\n".join(f"- [탐색/{c}]" for c in children)
    return "추가로 조사할 곳:\n" + lines

def _choose_type_uniform(cfg_node):
    """갈레온 / 아이템 / 소문 중 1/3 균등. 비면 가능한 타입으로 폴백."""
    candidates = ["coin", "item", "rumor"]
    random.shuffle(candidates)

    def valid(t):
        if t == "coin":
            return cfg_node["gmin"] > 0 or cfg_node["gmax"] > 0
        if t == "item":
            return bool(cfg_node["item"]) and cfg_node["qty"] > 0
        if t == "rumor":
            return bool(cfg_node["rumor"])
        return False

    # 1차 무작위 → 유효하면 채택
    pick = random.choice(candidates)
    if valid(pick):
        return pick
    # 2차 폴백: 가능한 타입 중 다시 랜덤
    avail = [t for t in candidates if valid(t)]
    return random.choice(avail) if avail else None

def _apply_reward_uniform(cfg_node, sheets: Sheets, handle: str, currency_key: str):
    """
    보상 결정 및 적용.
    반환 (text, paid):
      - text: 출력 메시지
      - paid: 가시적 보상(갈레온/아이템) 지급 여부(True/False)
    """
    base = cfg_node["place"] or ""
    t = _choose_type_uniform(cfg_node)
    if not t:
        return base, False

    if t == "coin":
        lo = max(0, cfg_node["gmin"]);
        hi = max(lo, cfg_node["gmax"])
        amt = random.randint(lo, hi) if hi > 0 else 0
        if amt > 0:
            sheets.add_currency(handle, amt)
            return f"{base}\n획득: {currency_key} +{amt}", True
        return base, False

    if t == "item":
        item, qty = cfg_node["item"], cfg_node["qty"]
        if item and qty > 0:
            sheets.add_item(handle, item, qty)
            return f"{base}\n획득: {item} x{qty}", True
        return base, False

    # rumor
    rumor = cfg_node.get("rumor") or ""
    if rumor:
        return f"{base}\n소문: {rumor}", True  # 소문도 소진으로 처리하려면 True
    return base, False

def handle(acct: str, raw_path: str, sheets: Sheets, cfg: Config) -> str:
    """
    세션/부모구역 탐색 핸들러.
    - 루트/네비게이션만 할 땐 일일 제한 소모 안 함
    - 실제 보상 처리 시점에만 제한 체크/증가
    - 선택지는 불릿 리스트로 출력
    - 가시적 보상(갈레온/아이템) 지급 시 '[보상 처리]' 라벨을 붙임
    """

    res = sheets.get_session_row(acct)
    if not isinstance(res, tuple) or len(res) != 2:
        # 세션 함수가 None 등 잘못된 값을 주면 바로 명확히 터뜨려서 원인 파악
        raise RuntimeError(f"get_session_row invalid return: {res!r}")
    sess_row, cur_path = res

    conf = sheets.get_config()
    currency_key = conf.get("통화키", "갈레온")

    # 현재 세션 경로 불러오기
    cur_path = normalize_path(cur_path)
    token = (raw_path or "").strip()

    # 네비게이션 해석
    if token == "루트":
        new_path = ""
    elif token == "..":
        new_path = path_parent(cur_path)
    elif "/" in token:
        new_path = normalize_path(token)  # 절대경로 점프 허용
    else:
        parent = path_last(cur_path) if cur_path else ""
        if parent:
            children = sheets.list_children(parent)
            new_path = normalize_path(f"{cur_path}/{token}") if token in children else normalize_path(token)
        else:
            new_path = normalize_path(token)

    node = path_last(new_path) if new_path else ""

    # 루트: 제한 소모 없이 선택지만 보여줌
    if not node:
        roots = sheets.list_children("")
        if not roots:
            return "탐색 가능한 루트 구역이 없습니다."
        sheets.set_session_path(sess_row, "", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return "탐색 시작 지점입니다.\n\n" + _format_children_bullets(roots)

    # 노드 검증
    if not sheets.node_exists(node):
        return f"해당 구역을 찾을 수 없습니다: {node}"

    # 여기서 제한 체크(보상 처리 직전). 제한 초과라도 선택지는 보여줌.
    with sheets.atomic():
        used = sheets.get_today_limit(acct)
        limit = int(conf.get("탐색_일일제한", 3))

        if used >= limit:
            children = sheets.list_children(node)
            tips = ("\n\n" + _format_children_bullets(children)) if children else ""
            return f"탐색은 하루 {limit}회까지 가능합니다." + tips

        cfg_node = sheets.get_node_config(node)
        if not cfg_node:
            return f"해당 구역에는 설정 행이 없습니다: {node}"

        text, consumed = _apply_reward_uniform(cfg_node, sheets, acct, currency_key)

        if consumed:
            sheets.inc_today_limit(acct)

    # 락 밖에서 자식 선택지/세션 경로 갱신(필요 시 아래 두 줄도 락 안으로 옮겨도 OK)
    children = sheets.list_children(node)
    if children:
        text += "\n\n" + _format_children_bullets(children)
    #세션 경로 갱신
    sheets.set_session_path(sess_row, new_path, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return text
