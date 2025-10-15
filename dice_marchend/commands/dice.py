# commands/dice.py
from ..utils import parse_dice, roll_ndm

# 안전 가드(원하면 조정)
MAX_N = 100        # 한 번에 던질 주사위 개수 상한
MAX_M = 1000       # 면체 상한
MAX_EXPRESSIONS = 10  # 한 메시지에서 처리할 최대 표현식 수

def _clamp(n, lo, hi): return max(lo, min(hi, n))

def handle(text: str) -> list[str]:
    """
    입력 텍스트에서 [NdM(+/-K)?] 패턴을 모두 찾아 결과 문자열 리스트를 반환.
    예: ["[3d6+2] → 2,5,4 = 11; +2 ⇒ 총 13"]
    """
    exprs = parse_dice(text)
    if not exprs:
        return []

    out = []
    for idx, (n, m, mod) in enumerate(exprs[:MAX_EXPRESSIONS], start=1):
        # 간단한 유효성 체크/클램프
        n = _clamp(n, 1, MAX_N)
        m = _clamp(m, 2, MAX_M)

        rolls, subtotal, mod_used, total = roll_ndm(n, m, mod)
        rolls_str = ",".join(str(x) for x in rolls)
        head = f"[{n}d{m}{('+'+str(mod_used)) if mod_used>0 else (str(mod_used) if mod_used<0 else '')}]"
        if mod_used:
            out.append(f"{head} → {rolls_str} = {subtotal} / {mod_used:+d} ⇒ 총 {total}")
        else:
            out.append(f"{head} → {rolls_str} = 총 {total}")
    return out
