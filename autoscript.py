# -*- coding: utf-8 -*-
import time
import logging
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

import pytz
import gspread
from google.oauth2.service_account import Credentials
from mastodon import Mastodon, MastodonAPIError, MastodonNetworkError

# =========================
# 하드코딩 설정
# =========================

# Mastodon
MASTODON_BASE_URL = "https://marchen1210d.site"
MASTODON_ACCESS_TOKEN = "Y_K7iQ_NgMR8OjtNPE03pgzkdUs97Qe7s6eOlEri7_o"

# Google Service Account JSON
GOOGLE_SA_JSON = "march-credential.json"

# 스프레드시트
SHEET_KEY = "100unZZH0e1ykkIHjUxjYCXDi23y0biclvyqT7Q37WJM"  # 권장: 문서 Key
SHEET_NAME = ""                                           # Key를 쓰면 비워도 됨

WS_LIST = "출력목록"    # 행 기반 대본 탭
WS_CTRL = "출력제어"    # 세로 레이아웃 컨트롤 탭 (A열 라벨, B열~ 작업)

# 타임존
KST = pytz.timezone("Asia/Seoul")

# 출력목록 헤더
HDR_ORDER     = "순번"
HDR_TEXT      = "문장"
HDR_POSTED    = "출력여부"
HDR_POSTED_AT = "출력시각"
HDR_SCRIPT_ID = "스크립트ID"   # 선택

TRUTHY = {"TRUE", "T", "1", "Y", "YES", "ON", "참", "예"}

# 출력제어(세로) 라벨들 (A열 고정)
CTRL_ACTIVE   = "활성화"
CTRL_CHECK    = "체크"
CTRL_START_AT = "시작시각"
CTRL_INTERVAL = "간격초"
CTRL_VIS      = "가시성"
CTRL_SCRIPT   = "스크립트ID"
CTRL_MAXCNT   = "최대개수"
CTRL_LOCK     = "잠금"
CTRL_STATUS   = "상태"
CTRL_LASTRUN  = "최근실행"

CTRL_LABELS_ORDER = [
    CTRL_ACTIVE, CTRL_CHECK, CTRL_START_AT, CTRL_INTERVAL, CTRL_VIS,
    CTRL_SCRIPT, CTRL_MAXCNT, CTRL_LOCK, CTRL_STATUS, CTRL_LASTRUN
]

# 폴링 주기(읽기 빈도)
POLL_SEC_WHEN_IDLE = 15   # 대기 중(아무 작업도 없을 때)
POLL_SEC_WHEN_BUSY = 3    # 실행/직후 상태

# 적응형 슬립 상한(초): 예약까지 남은 시간이 길면 최대 이 시간만큼 잠듦
ADAPTIVE_SLEEP_CEIL = 600  # 10분

# =========================
# 시트 클라이언트
# =========================

class Sheets:
    def __init__(self):
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(GOOGLE_SA_JSON, scopes=scopes)
        self.gc = gspread.authorize(creds)
        self.ss = self.gc.open_by_key(SHEET_KEY) if SHEET_KEY else self.gc.open(SHEET_NAME)
        self.ws_list = self.ss.worksheet(WS_LIST)
        self.ws_ctrl = self.ss.worksheet(WS_CTRL)

        # 출력목록: 1행 헤더 맵
        header = [h.strip() for h in self.ws_list.row_values(1)]
        self.hmap_list: Dict[str, int] = {h: i + 1 for i, h in enumerate(header)}  # 1-based
        required = [HDR_ORDER, HDR_TEXT, HDR_POSTED, HDR_POSTED_AT]
        miss = [h for h in required if h not in self.hmap_list]
        if miss:
            raise RuntimeError(f"'{WS_LIST}' 헤더 누락: {miss} (필수: {required})")

        # 출력제어: A열 라벨 → 행번호 매핑
        labels_col = [v.strip() for v in self.ws_ctrl.col_values(1)]
        self.ctrl_rmap: Dict[str, int] = {}
        for label in CTRL_LABELS_ORDER:
            try:
                r = labels_col.index(label) + 1  # 1-based
            except ValueError:
                raise RuntimeError(f"'{WS_CTRL}' A열에 라벨 '{label}' 이(가) 없습니다.")
            self.ctrl_rmap[label] = r

        # 상태 캐시(같은 메시지 반복 쓰기 방지)
        self._last_status: Dict[int, str] = {}

    # ---------- 출력목록 ----------
    def _list_values(self) -> List[List[str]]:
        # 헤더 포함 A~E만 (E: 스크립트ID 선택용)
        return self.ws_list.get_values('A1:E')

    def get_next_unposted(self, script_id: Optional[str]) -> Optional[Tuple[int, str]]:
        values = self._list_values()
        if len(values) <= 1:
            return None
        rows = values[1:]

        idx_order = self.hmap_list[HDR_ORDER] - 1
        idx_text  = self.hmap_list[HDR_TEXT] - 1
        idx_post  = self.hmap_list[HDR_POSTED] - 1
        idx_sid   = self.hmap_list.get(HDR_SCRIPT_ID)
        if idx_sid is not None:
            idx_sid -= 1

        cands = []
        for i, row in enumerate(rows, start=2):
            if len(row) <= max(idx_order, idx_text, idx_post):
                continue
            posted = (row[idx_post] or "").strip().upper() in TRUTHY
            if posted:
                continue
            if script_id:
                if idx_sid is None or len(row) <= idx_sid:
                    continue
                if (row[idx_sid] or "").strip() != script_id:
                    continue
            try:
                order = int((row[idx_order] or "0").strip())
            except ValueError:
                order = 10**9
            text = (row[idx_text] or "").strip()
            if text:
                cands.append((order, i, text))

        if not cands:
            return None
        cands.sort(key=lambda x: (x[0], x[1]))
        _, row_index, text = cands[0]
        return row_index, text

    def mark_posted(self, row_index: int) -> str:
        from gspread.utils import rowcol_to_a1
        ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
        r1 = rowcol_to_a1(row_index, self.hmap_list[HDR_POSTED])  # 체크박스 셀
        r2 = rowcol_to_a1(row_index, self.hmap_list[HDR_POSTED_AT])  # 텍스트/날짜 셀

        # 체크박스엔 True(불리언), 시간은 문자열
        self.ws_list.batch_update(
            [
                {"range": r1, "values": [[True]]},
                {"range": r2, "values": [[ts]]},
            ],
            value_input_option="RAW",
        )
        return ts

    # ---------- 출력제어(세로): 열 단위 Job ----------
    def _get_last_job_col(self) -> int:
        """B열부터 오른쪽으로, 라벨행 중 하나라도 값이 있는 마지막 열 번호를 추정."""
        row_idx = self.ctrl_rmap[CTRL_VIS]
        row_vals = self.ws_ctrl.row_values(row_idx)  # 1회 읽기
        return len(row_vals)  # 1-based length == last used col (approx)

    def iter_job_cols(self) -> List[int]:
        # 최소 호출로 유효 열 판단 (batch_get 사용)
        from gspread.utils import rowcol_to_a1
        r_vis = self.ctrl_rmap[CTRL_VIS]
        vis_row = self.ws_ctrl.row_values(r_vis)  # 1회 읽기
        last = max(2, len(vis_row))
        ranges = [
            f"{rowcol_to_a1(self.ctrl_rmap[CTRL_ACTIVE], 2)}:{rowcol_to_a1(self.ctrl_rmap[CTRL_ACTIVE], last)}",
            f"{rowcol_to_a1(self.ctrl_rmap[CTRL_CHECK], 2)}:{rowcol_to_a1(self.ctrl_rmap[CTRL_CHECK], last)}",
            f"{rowcol_to_a1(self.ctrl_rmap[CTRL_INTERVAL], 2)}:{rowcol_to_a1(self.ctrl_rmap[CTRL_INTERVAL], last)}",
        ]
        blocks = self.ws_ctrl.batch_get(ranges, major_dimension='ROWS')  # 3회 읽기
        act_row   = blocks[0][0] if blocks and blocks[0] else []
        check_row = blocks[1][0] if len(blocks) > 1 and blocks[1] else []
        itv_row   = blocks[2][0] if len(blocks) > 2 and blocks[2] else []

        cols = []
        for c in range(2, last + 1):
            vis   = vis_row[c - 1].strip() if c - 1 < len(vis_row) else ""
            act   = (act_row[c - 2].strip().upper() in TRUTHY) if c - 2 < len(act_row) else False
            check = (check_row[c - 2].strip().upper() in TRUTHY) if c - 2 < len(check_row) else False
            itv   = itv_row[c - 2].strip() if c - 2 < len(itv_row) else ""
            if act or check or vis or itv:
                cols.append(c)
        return cols

    def _get_cell_value(self, r: int, c: int) -> str:
        v = self.ws_ctrl.cell(r, c).value
        return (v or "")

    def read_ctrl_col(self, c: int) -> Dict[str, Any]:
        from gspread.utils import rowcol_to_a1
        ranges = [rowcol_to_a1(self.ctrl_rmap[label], c) for label in CTRL_LABELS_ORDER]
        vals = self.ws_ctrl.batch_get(ranges, major_dimension='ROWS')  # 각 항목이 [['값']] 형태

        def first_cell(range_rows) -> str:
            """batch_get 반환(리스트의 리스트)에서 첫 셀을 문자열로 안전 추출"""
            if not range_rows:
                return ""
            row0 = range_rows[0]  # 예: ['TRUE'] 또는 []
            if isinstance(row0, list):
                cell = row0[0] if row0 else ""  # 예: 'TRUE'
            else:
                cell = row0
            return str(cell).strip()

        def v(label: str) -> str:
            i = CTRL_LABELS_ORDER.index(label)
            return first_cell(vals[i]) if i < len(vals) else ""

        active = v(CTRL_ACTIVE).upper() in TRUTHY
        check = v(CTRL_CHECK).upper() in TRUTHY
        start_s = v(CTRL_START_AT)
        interval = v(CTRL_INTERVAL)
        vis = v(CTRL_VIS) or "unlisted"
        script = v(CTRL_SCRIPT) or ""
        maxcnt_s = v(CTRL_MAXCNT)
        lock = v(CTRL_LOCK)

        return {
            "active": active,
            "check": check,
            "start_at": start_s,
            "interval": int(interval) if interval.isdigit() else 15,
            "visibility": vis,
            "script_id": script or None,
            "max_count": int(maxcnt_s) if maxcnt_s.isdigit() else None,
            "lock": lock,
        }

    def write_ctrl_status(self, c: int, status: str):
        # 동일 메시지면 쓰지 않음(쓰기 호출 절감)
        if self._last_status.get(c) == status:
            return
        self._last_status[c] = status

        from gspread.utils import rowcol_to_a1
        ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
        self.ws_ctrl.batch_update([
            {"range": rowcol_to_a1(self.ctrl_rmap[CTRL_STATUS], c),  "values": [[status]]},
            {"range": rowcol_to_a1(self.ctrl_rmap[CTRL_LASTRUN], c), "values": [[ts]]},
        ])

    def acquire_lock(self, c: int) -> bool:
        # 현재 잠금 확인 후, 배치로 RUNNING 세팅
        from gspread.utils import rowcol_to_a1
        cur = self._get_cell_value(self.ctrl_rmap[CTRL_LOCK], c).strip()
        if cur:
            return False
        rng = rowcol_to_a1(self.ctrl_rmap[CTRL_LOCK], c)
        self.ws_ctrl.batch_update([{"range": rng, "values": [["RUNNING"]]}])
        return True

    def release_lock(self, c: int):
        from gspread.utils import rowcol_to_a1
        rng = rowcol_to_a1(self.ctrl_rmap[CTRL_LOCK], c)
        self.ws_ctrl.batch_update([{"range": rng, "values": [[""]]}])

    def clear_check(self, c: int):
        from gspread.utils import rowcol_to_a1
        rng = rowcol_to_a1(self.ctrl_rmap[CTRL_CHECK], c)
        self.ws_ctrl.update(
            range_name=rng,
            values=[[False]],
            value_input_option="RAW",
        )


# =========================
# Mastodon
# =========================

def create_masto() -> Mastodon:
    api = Mastodon(
        api_base_url=MASTODON_BASE_URL,
        access_token=MASTODON_ACCESS_TOKEN,
        ratelimit_method="pace",
        request_timeout=30,
    )
    me = api.account_verify_credentials()
    logging.info(f"Mastodon 로그인: @{me['acct']}")
    return api


# =========================
# 실행 로직
# =========================

def should_start_now(ctrl: Dict[str, Any]) -> bool:
    if ctrl["check"]:
        return True
    if ctrl["active"]:
        s = ctrl["start_at"]
        if not s:
            return False
        try:
            dt = KST.localize(datetime.strptime(s, "%Y-%m-%d %H:%M:%S"))
            return datetime.now(KST) >= dt
        except Exception:
            return False
    return False

def seconds_until_start(ctrl: Dict[str, Any]) -> Optional[int]:
    try:
        if not (ctrl.get("active") and ctrl.get("start_at")):
            return None
        dt = KST.localize(datetime.strptime(ctrl["start_at"], "%Y-%m-%d %H:%M:%S"))
        delta = (dt - datetime.now(KST)).total_seconds()
        return int(delta) if delta > 0 else 0
    except Exception:
        return None

def run_job_for_col(api: Mastodon, sheets: Sheets, c: int, ctrl: Dict[str, Any]):
    # 체크 트리거는 1회성이므로 해제
    if ctrl["check"]:
        sheets.clear_check(c)

    # 잠금
    if not sheets.acquire_lock(c):
        sheets.write_ctrl_status(c, "잠금 실패(동시 실행)")
        return

    try:
        delay = max(0, int(ctrl["interval"]))
        vis   = ctrl["visibility"]
        sid   = ctrl["script_id"]
        limit = ctrl["max_count"]

        sheets.write_ctrl_status(c, f"시작 대기 {delay}s")
        time.sleep(delay)

        count = 0
        while True:
            nxt = sheets.get_next_unposted(sid)
            if not nxt:
                sheets.write_ctrl_status(c, "미출력 없음 → 종료")
                break
            row_index, text = nxt

            logging.info(f"[col {c}] 대본행 {row_index} 게시: {text!r}")
            status = api.status_post(text, visibility=vis)
            logging.info(f"[col {c}] 게시 완료: status_id={status['id']}")

            ts = sheets.mark_posted(row_index)
            sheets.write_ctrl_status(c, f"게시 완료 @ {ts} (대본행 {row_index})")

            count += 1
            if limit is not None and count >= limit:
                sheets.write_ctrl_status(c, f"최대개수 {limit} 도달 → 종료")
                break

            time.sleep(delay)
    finally:
        sheets.release_lock(c)
        sheets.write_ctrl_status(c, "대기 중")


# =========================
# 메인 루프
# =========================

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not MASTODON_BASE_URL or not MASTODON_ACCESS_TOKEN:
        raise RuntimeError("MASTODON_BASE_URL / MASTODON_ACCESS_TOKEN 설정 필요")
    if not (SHEET_KEY or SHEET_NAME):
        raise RuntimeError("SHEET_KEY 또는 SHEET_NAME 중 하나 설정 필요")

    sheets = Sheets()
    api = create_masto()

    logging.info("세로 레이아웃 컨트롤 모드: A열 라벨, B열부터 작업 열을 스캔합니다.")
    backoff = 10  # Sheets API 오류 시 지수 백오프 시작값(초)

    while True:
        try:
            any_running = False
            cols = sheets.iter_job_cols()

            # 적응형 슬립: 기본은 IDLE 슬립, 예약이 멀면 더 크게
            next_sleep = POLL_SEC_WHEN_IDLE

            for c in cols:
                ctrl = sheets.read_ctrl_col(c)

                # 이미 다른 인스턴스/프로세스가 잠금 중이면 건너뜀
                if ctrl["lock"] and ctrl["lock"].strip():
                    any_running = True
                    continue

                if should_start_now(ctrl):
                    any_running = True
                    run_job_for_col(api, sheets, c, ctrl)
                else:
                    remain = seconds_until_start(ctrl)
                    if remain is not None:
                        # 다음 시작까지 남은 시간 고려 (상한 ADAPTIVE_SLEEP_CEIL)
                        next_sleep = min(max(next_sleep, min(remain, ADAPTIVE_SLEEP_CEIL)), ADAPTIVE_SLEEP_CEIL)

            # 진행 중이면 BUSY 슬립, 아니면 적응형 슬립
            time.sleep(POLL_SEC_WHEN_BUSY if any_running else next_sleep)

            # 성공 루프면 백오프 초기화
            backoff = 10

        except (MastodonNetworkError, MastodonAPIError) as e:
            logging.warning(f"Mastodon 오류: {e}. 20초 후 재시도.")
            time.sleep(20)
        except gspread.exceptions.APIError as e:
            logging.warning(f"Google Sheets API 오류: {e}. {backoff}s 후 재시도.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)  # 최대 2분
        except Exception as e:
            logging.exception(f"예상치 못한 오류: {e}. 10초 후 백오프.")
            time.sleep(10)


if __name__ == "__main__":
    main()
