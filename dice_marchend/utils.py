import re, random
from datetime import datetime
import pytz

HTML_TAG_RE = re.compile(r"<[^>]+>")
DICE_RE = re.compile(r"\[\s*(\d+)[dD](\d+)(?:\s*([+-]\s*\d+))?\s*\]")

def build_user_label(handle: str, nickname: str, mode: str = "hidden") -> str:
    # mode: hidden | parens | replace
    nn = (nickname or "").strip()
    if mode == "hidden":
        return nn or handle
    if mode == "parens":
        if nn: return f"{nn}(@{handle})"
        return f"@{handle}"
    # replace
    return nn or handle

def html_to_text(html: str) -> str:
    return HTML_TAG_RE.sub(" ", html or "")

def parse_dice(text: str):
    """
    예: [3d6], [1D10+2], [4d5 - 1]
    반환: [(n, m, mod), ...]  # mod는 없으면 0
    """
    out = []
    for n, m, mod in DICE_RE.findall(text or ""):
        n_i = int(n)
        m_i = int(m)
        if mod:
            mod = mod.replace(" ", "")
            mod_i = int(mod)
        else:
            mod_i = 0
        out.append((n_i, m_i, mod_i))
    return out

def roll_ndm(n: int, m: int, mod: int = 0):
    rolls = [random.randint(1, m) for _ in range(n)]
    subtotal = sum(rolls)
    total = subtotal + mod
    return rolls, subtotal, mod, total

def today_ymd(tz_name: str) -> str:
    tz = pytz.timezone(tz_name)
    return datetime.now(tz).strftime("%Y-%m-%d")

def is_reply_to(status: dict, target_status_id: str) -> bool:
    if not target_status_id or target_status_id == "0":
        return True
    return str(status.get("in_reply_to_id") or "") == str(target_status_id)

# ===== 세션 경로 유틸 (탐색/부모구역 방식에 필요) =====
def normalize_path(token: str) -> str:
    """'/''로 구분된 경로를 정규화: 앞뒤 슬래시/공백 제거, 중복 슬래시 제거."""
    token = (token or "").strip().strip("/")
    if not token:
        return ""
    parts = [p.strip() for p in token.split("/") if p.strip()]
    return "/".join(parts)

def path_parent(path: str) -> str:
    """부모 경로 반환. 루트면 빈 문자열."""
    p = normalize_path(path)
    if not p:
        return ""
    parts = p.split("/")
    return "/".join(parts[:-1])

def path_last(path: str) -> str:
    """경로의 마지막 토큰(현재 노드) 반환. 루트면 빈 문자열."""
    p = normalize_path(path)
    return p.split("/")[-1] if p else ""