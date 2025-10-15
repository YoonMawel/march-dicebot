# -*- coding: utf-8 -*-
import time
import logging
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

import pytz
import gspread
from google.oauth2.service_account import Credentials
# gspread.utils 모듈을 사용하여 A1 표기법 변환에 활용
from gspread import utils
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
SHEET_KEY = ""  # 권장: 문서 Key
SHEET_NAME = "스크립트출력"  # Key를 쓰면 비워도 됨

WS_LIST = "출력목록"  # 행 기반 대본 탭
WS_CTRL = "출력제어"  # 세로 레이아웃 컨트롤 탭 (A열 라벨, B열~ 작업)

# 타임존
KST = pytz.timezone("Asia/Seoul")

# 출력목록 헤더
HDR_ORDER = "순번"
HDR_TEXT = "문장"
HDR_POSTED = "출력여부"
HDR_POSTED_AT = "출력시각"
HDR_SCRIPT_ID = "스크립트ID"  # 선택

TRUTHY = {"TRUE", "T", "1", "Y", "YES", "ON", "참", "예"}

# 출력제어(세로) 라벨들 (A열 고정)
CTRL_ACTIVE = "활성화"
CTRL_CHECK = "체크"
CTRL_START_AT = "시작시각"
CTRL_INTERVAL = "간격초"
CTRL_VIS = "가시성"
CTRL_SCRIPT = "스크립트ID"
CTRL_MAXCNT = "최대개수"
CTRL_LOCK = "잠금"
CTRL_STATUS = "상태"
CTRL_LASTRUN = "최근실행"

CTRL_LABELS_ORDER = [
    CTRL_ACTIVE, CTRL_CHECK, CTRL_START_AT, CTRL_INTERVAL, CTRL_VIS,
    CTRL_SCRIPT, CTRL_MAXCNT, CTRL_LOCK, CTRL_STATUS, CTRL_LASTRUN
]

# 폴링 주기
POLL_SEC_WHEN_IDLE = 5
POLL_SEC_WHEN_BUSY = 6


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

        # 출력목록: 1행 헤더 맵 (API 1회)
        header = [h.strip() for h in self.ws_list.row_values(1)]
        self.hmap_list: Dict[str, int] = {h: i + 1 for i, h in enumerate(header)}  # 1-based
        required = [HDR_ORDER, HDR_TEXT, HDR_POSTED, HDR_POSTED_AT]
        miss = [h for h in required if h not in self.hmap_list]
        if miss:
            raise RuntimeError(f"'{WS_LIST}' 헤더 누락: {miss} (필수: {required})")

        # 출력제어: A열 라벨 → 행번호 매핑 (API 1회)
        labels_col = [v.strip() for v in self.ws_ctrl.col_values(1)]
        self.ctrl_rmap: Dict[str, int] = {}
        for label in CTRL_LABELS_ORDER:
            try:
                r = labels_col.index(label) + 1  # 1-based
            except ValueError:
                raise RuntimeError(f"'{WS_CTRL}' A열에 라벨 '{label}' 이(가) 없습니다.")
            self.ctrl_rmap[label] = r

        # 💡 캐시 초기화 및 초기 로딩
        self._cache_ctrl: List[List[str]] = []
        self._cache_list: List[List[str]] = []
        self.refresh_ctrl_cache()  # 제어 탭 초기 로드 (API 1회)

    # -----------------------------------------------------
    # 💡 최적화: 캐시/배치 읽기/쓰기 메소드
    # -----------------------------------------------------

    def refresh_ctrl_cache(self):
        """출력제어(WS_CTRL) 시트 전체를 읽어서 캐시에 저장합니다. (API 1회)"""
        # 429 오류 방지를 위해, 루프 내 개별 셀 읽기 대신 한 번에 가져옴
        self._cache_ctrl = self.ws_ctrl.get_all_values()

    def _get_cell_value_from_cache(self, cache: List[List[str]], r: int, c: int) -> str:
        """API 호출 대신 메모리에 저장된 캐시에서 셀 값을 가져옵니다. (API 0회)"""
        try:
            # r, c는 1-based 인덱스
            v = cache[r - 1][c - 1]
            return (v or "").strip()
        except IndexError:
            return ""

    # ---------- 출력목록 (읽기/쓰기 최적화) ----------
    def _refresh_list_cache(self):
        """출력목록(WS_LIST) 시트 전체를 읽어서 캐시에 저장합니다. (API 1회)"""
        self._cache_list = self.ws_list.get_all_values()

    def get_next_unposted(self, script_id: Optional[str]) -> Optional[Tuple[int, str]]:
        # 💡 매번 시도 시, 최신 상태를 반영하기 위해 캐시 갱신 (API 1회)
        self._refresh_list_cache()
        values = self._cache_list

        if len(values) <= 1:
            return None
        rows = values[1:]

        idx_order = self.hmap_list[HDR_ORDER] - 1
        idx_text = self.hmap_list[HDR_TEXT] - 1
        idx_post = self.hmap_list[HDR_POSTED] - 1
        idx_sid = self.hmap_list.get(HDR_SCRIPT_ID)
        if idx_sid is not None:
            idx_sid -= 1

        cands = []
        for i, row in enumerate(rows, start=2):
            # ... (이하 로직은 동일)
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
                order = 10 ** 9
            text = (row[idx_text] or "").strip()
            if text:
                cands.append((order, i, text))

        if not cands:
            return None
        cands.sort(key=lambda x: (x[0], x[1]))
        _, row_index, text = cands[0]
        return row_index, text

    def mark_posted(self, row_index: int) -> str:
        """출력여부/출력시각을 batch_update로 갱신 (API 1회)"""
        ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")

        requests = [
            # 체크박스 업데이트는 부울 값 True만 전달하면 됩니다.
            {'range': utils.rowcol_to_a1(row_index, self.hmap_list[HDR_POSTED]),
             'values': [[True]]},  # <--- 주의! 'TRUE' (문자열) 대신 True (부울) 사용

            # 출력 시각은 문자열로 전달합니다.
            {'range': utils.rowcol_to_a1(row_index, self.hmap_list[HDR_POSTED_AT]),
             'values': [[ts]]},
        ]
        # 💡 API 2회 호출 대신, 1회 배치 업데이트 호출
        self.ws_list.batch_update(requests)
        return ts

    # ---------- 출력제어(읽기/쓰기 최적화) ----------
    def _get_last_job_col(self) -> int:
        """캐시를 이용해 마지막 사용 열을 추정. (API 0회)"""
        row_idx = self.ctrl_rmap[CTRL_VIS]
        if row_idx - 1 < len(self._cache_ctrl):
            return len(self._cache_ctrl[row_idx - 1])
        return 2

    def iter_job_cols(self) -> List[int]:
        """캐시된 데이터로 유효한 작업 열을 판단. (API 0회)"""
        # 💡 메인 루프에서 refresh_ctrl_cache()가 호출되므로 여기서는 호출하지 않음

        # 작업 열은 B열(2)부터 마지막 사용 열까지
        last = max(2, self._get_last_job_col())
        cols = []
        for c in range(2, last + 1):
            # 💡 API 호출 대신 캐시에서 값 가져오기
            active = (self._get_cell_value_from_cache(self._cache_ctrl, self.ctrl_rmap[CTRL_ACTIVE],
                                                      c).upper() in TRUTHY)
            check = (self._get_cell_value_from_cache(self._cache_ctrl, self.ctrl_rmap[CTRL_CHECK], c).upper() in TRUTHY)
            vis = self._get_cell_value_from_cache(self._cache_ctrl, self.ctrl_rmap[CTRL_VIS], c)
            interval = self._get_cell_value_from_cache(self._cache_ctrl, self.ctrl_rmap[CTRL_INTERVAL], c)

            if active or check or vis or interval:
                cols.append(c)
        return cols

    def _get_cell_value(self, r: int, c: int) -> str:
        """API를 직접 호출하는 대신 캐시에서 값을 가져옴 (API 0회)"""
        return self._get_cell_value_from_cache(self._cache_ctrl, r, c)

    def read_ctrl_col(self, c: int) -> Dict[str, Any]:
        """캐시된 데이터로 작업 제어 정보 읽기. (API 0회)"""

        def gv(label: str) -> str:
            r = self.ctrl_rmap[label]
            # 💡 API 호출 대신 캐시에서 가져옴
            return self._get_cell_value_from_cache(self._cache_ctrl, r, c)

        active = gv(CTRL_ACTIVE).upper() in TRUTHY
        check = gv(CTRL_CHECK).upper() in TRUTHY
        start_s = gv(CTRL_START_AT)
        interval = gv(CTRL_INTERVAL)
        vis = gv(CTRL_VIS) or "unlisted"
        script = gv(CTRL_SCRIPT) or ""
        maxcnt_s = gv(CTRL_MAXCNT)
        lock = gv(CTRL_LOCK)

        return {
            "active": active,
            "check": check,
            "start_at": start_s,
            # 💡 기본 간격을 10초로 변경
            "interval": int(interval) if interval.isdigit() else 10,
            "visibility": vis,
            "script_id": script if script else None,
            "max_count": int(maxcnt_s) if maxcnt_s.isdigit() else None,
            "lock": lock,
        }

    def write_ctrl_status(self, c: int, status: str):
        """상태와 최근실행을 batch_update로 갱신 (API 1회)"""
        ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")

        requests = [
            {'range': utils.rowcol_to_a1(self.ctrl_rmap[CTRL_STATUS], c), 'values': [[status]]},
            {'range': utils.rowcol_to_a1(self.ctrl_rmap[CTRL_LASTRUN], c), 'values': [[ts]]},
        ]
        # 💡 API 2회 호출 대신, 1회 배치 업데이트 호출
        self.ws_ctrl.batch_update(requests)

    def acquire_lock(self, c: int) -> bool:
        """잠금 획득 시 캐시를 갱신하고 상태를 확인 (API 1회)"""
        # 잠금 획득 전 최신 상태 반영 (API 1회)
        self.refresh_ctrl_cache()
        cur = self._get_cell_value(self.ctrl_rmap[CTRL_LOCK], c)

        if cur:
            return False

        # 획득 시만 쓰기 (API 1회)
        self.ws_ctrl.update_cell(self.ctrl_rmap[CTRL_LOCK], c, "RUNNING")
        return True

    def release_lock(self, c: int):
        """잠금 해제 (API 1회)"""
        self.ws_ctrl.update_cell(self.ctrl_rmap[CTRL_LOCK], c, "")

    def clear_check(self, c: int):
        """체크 해제 (API 1회)"""
        self.ws_ctrl.update_cell(self.ctrl_rmap[CTRL_CHECK], c, "FALSE")


# =========================
# Mastodon
# =========================

def create_masto() -> Mastodon:
    api = Mastodon(
        api_base_url=MASTODON_BASE_URL,
        access_token=MASTODON_ACCESS_TOKEN,
        ratelimit_method="pace",
        request_timeout=10,
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


def run_job_for_col(api: Mastodon, sheets: Sheets, c: int, ctrl: Dict[str, Any]):
    # 체크 트리거는 1회성이므로 해제
    if ctrl["check"]:
        sheets.clear_check(c)

    # 잠금
    if not sheets.acquire_lock(c):
        sheets.write_ctrl_status(c, "잠금 실패(동시 실행)")
        return

    try:
        delay = max(0, int(ctrl["interval"]))  # 10초 간격 설정
        vis = ctrl["visibility"]
        sid = ctrl["script_id"]
        limit = ctrl["max_count"]

        # 💡 첫 툿 게시 전 대기 시간 (시작시각 조정 용도)
        sheets.write_ctrl_status(c, f"작업 시작 (간격 {delay}s)")

        count = 0
        is_first_tweet = True

        while True:
            if not is_first_tweet:
                time.sleep(delay)
            is_first_tweet = False
            # 툿 찾기 (API 1회)
            nxt = sheets.get_next_unposted(sid)

            if not nxt:
                sheets.write_ctrl_status(c, "미출력 없음 → 종료")
                break
            row_index, text = nxt

            logging.info(f"[col {c}] 대본행 {row_index} 게시: {text!r}")
            # Mastodon 게시 (API 1회)
            status = api.status_post(text, visibility=vis)
            logging.info(f"[col {c}] 게시 완료: status_id={status['id']}")

            # 출력 목록에 반영 (API 1회)
            ts = sheets.mark_posted(row_index)
            # 제어 탭 상태 갱신 (API 1회)
            sheets.write_ctrl_status(c, f"게시 완료 @ {ts} (대본행 {row_index})")

            count += 1
            if limit is not None and count >= limit:
                sheets.write_ctrl_status(c, f"최대개수 {limit} 도달 → 종료")
                break

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
    while True:
        try:
            # 💡 메인 루프 시작 시 제어 시트 최신 데이터를 한 번만 읽어옴 (API 1회)
            sheets.refresh_ctrl_cache()

            any_running = False
            # 💡 이후 iter_job_cols, read_ctrl_col은 캐시에서 데이터 읽기 (API 0회)
            cols = sheets.iter_job_cols()
            for c in cols:
                ctrl = sheets.read_ctrl_col(c)
                if ctrl["lock"] and ctrl["lock"].strip():
                    any_running = True
                    continue
                if should_start_now(ctrl):
                    any_running = True
                    run_job_for_col(api, sheets, c, ctrl)

            time.sleep(POLL_SEC_WHEN_BUSY if any_running else POLL_SEC_WHEN_IDLE)

        except (MastodonNetworkError, MastodonAPIError) as e:
            logging.warning(f"Mastodon 오류: {e}. 20초 후 재시도.")
            time.sleep(20)
        except gspread.exceptions.APIError as e:
            # 💡 429 오류 발생 시 지수 백오프를 사용하는 것이 좋지만, 10초 대기는 유지
            logging.warning(f"Google Sheets API 오류: {e}. 10초 후 재시도.")
            time.sleep(10)
        except Exception as e:
            logging.exception(f"예상치 못한 오류: {e}. 10초 후 백오프.")
            time.sleep(10)


if __name__ == "__main__":
    main()