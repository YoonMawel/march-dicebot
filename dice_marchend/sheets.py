from __future__ import annotations
import os, gspread
import time
import random
import threading

from oauth2client.service_account import ServiceAccountCredentials
from typing import Dict, Tuple, List, Optional
from .models import Runner, ExploreRow
from .config import Config
from .utils import today_ymd
from gspread.exceptions import APIError

class Sheets:
    def __init__(self, cfg: Config):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(cfg.CREDS_PATH, scope)
        self.client = gspread.authorize(creds)

        self.cfg = cfg
        self.doc = self.client.open(cfg.SHEET_NAME)
        self.ws_runner = self.doc.worksheet("러너")
        self.ws_limits = self.doc.worksheet("제한")
        self.ws_explore = self.doc.worksheet("탐색")
        self.ws_session = self.doc.worksheet("세션")
        self.ws_particip = self.doc.worksheet("참여기록")
        self.ws_config = self.doc.worksheet("설정")

        self.ws_bag = None
        if cfg.SHOP_SHEET_NAME:
            try:
                shop_doc = self.client.open(cfg.SHOP_SHEET_NAME)
                self.ws_bag = shop_doc.worksheet(cfg.SHOP_BAG_WS)
            except Exception:
                self.ws_bag = None

        self._config_map: Optional[Dict[str, str]] = None
        self._config_loaded_at = 0.0
        self._config_ttl_sec = int(os.environ.get("CONFIG_TTL_SEC", "1800"))  # 기본 30분

        self._config_lock = threading.Lock()  # 설정 캐시 보호용
        self._locks_master = threading.Lock()  # per-user 락 딕셔너리 보호용
        self._locks = {}  # handle(또는 key) -> threading.Lock()
        self._sheet_cache = {}  # { key: (loaded_at, rows) }
        self._sheet_cache_ttl = 3.0  # 초 단위(2~5초 권장). 짧은 ‘마이크로 캐시’.

    def lock_for(self, key: str):
        """key(보통 handle) 기준의 per-user 락을 돌려준다."""
        if not key:
            # 방어: 빈 키면 전역락처럼 동작
            return self._locks_master

        with self._locks_master:
            lk = self._locks.get(key)

            if lk is None:
                lk = threading.Lock()
                self._locks[key] = lk

            return lk

    def atomic(self):
        return self._locks_master

    def force_reload(self):
        """다음 get_config() 호출 때 다시 불러오도록 캐시 무효화"""
        with self._config_lock:
            self._config_map = None
            self._config_loaded_at = 0.0

    # ---------- 설정 ----------
    def get_config(self) -> Dict[str, str]:
        now = time.time()

        if (self._config_map is not None) and (now - self._config_loaded_at <= self._config_ttl_sec):
            return self._config_map

        rows = self._with_retry(self.ws_config.get_all_values)

        mp: Dict[str, str] = {}
        for r in rows[1:]:
            if len(r) >= 2 and r[0].strip():
                mp[r[0].strip()] = (r[1].strip() if len(r) > 1 else "")

        with self._config_lock:
            self._config_map = mp
            self._config_loaded_at = now
        return mp

    # ---------- 러너 ----------
    def get_runner_row(self, handle: str) -> Tuple[int, Runner]:

        vals = self._read_all_cached(self.ws_runner, "러너")

        header = {k: i for i, k in enumerate(vals[0])}
        cu = header.get("유저명"); cn = header.get("닉네임")
        cd = header.get("기숙사"); cp = header.get("기숙사점수")
        ca = header.get("출석마지막일"); cc = header.get("이벤트확인마지막일")

        if None in (cu, cn, cd, cp, ca, cc):
            raise RuntimeError("시트 리딩 오류.")

        for r, row in enumerate(vals[1:], start=2):
            if (row[cu] or "").strip() == handle:
                return r, Runner(
                    handle=handle,
                    nickname=row[cn] or "",
                    dorm=row[cd] or "",
                    house_points=int(row[cp] or 0),
                    last_attend_date=row[ca] or "",
                    last_confirm_date=row[cc] or "",
                )
        # 없으면 추가: [유저명, 닉네임, 기숙사, 점수, 출석, 확인]
        self.ws_runner.append_row([handle, "", "", "0", "", ""], value_input_option="USER_ENTERED")
        self._invalidate_cache("러너")
        return self.get_runner_row(handle)

    def update_runner_nickname(self, row_idx: int, nickname: str):
        self._with_retry(self.ws_runner.update_cell, row_idx, 2, nickname)  # 2=닉네임
        self._invalidate_cache("러너")

    def update_runner_points(self, row_idx: int, new_points: int):
        self._with_retry(self.ws_runner.update_cell, row_idx, 4, new_points)
        self._invalidate_cache("러너")

    def update_runner_last_attend(self, row_idx: int, ymd: str):
        self._with_retry(self.ws_runner.update_cell, row_idx, 5, ymd)
        self._invalidate_cache("러너")
        # 4 = 출석마지막일 (1-based index)

    def update_runner_last_confirm(self, row_idx: int, ymd: str):
        self._with_retry(self.ws_runner.update_cell, row_idx, 6, ymd)
        self._invalidate_cache("러너")
        # 5 = 이벤트확인마지막일 (1-based index)

    # ---------- 제한(탐색 하루 N회) ----------
    def get_today_limit(self, handle: str) -> int:
        ymd = today_ymd(self.cfg.TIMEZONE)
        vals = self._read_all_cached(self.ws_limits, "제한")
        header = {k: i for i, k in enumerate(vals[0])}
        cu = header.get("유저명"); cd = header.get("날짜"); cc = header.get("탐색_사용횟수")

        if None in (cu, cd, cc):
            raise RuntimeError("제한 확인")

        for r, row in enumerate(vals[1:], start=2):
            if (row[cu] or "").strip() == handle and (row[cd] or "").strip() == ymd:
                return int(row[cc] or 0)

        return 0

    def inc_today_limit(self, handle: str):
        ymd = today_ymd(self.cfg.TIMEZONE)

        # 1) 읽기는 마이크로 캐시 사용
        vals = self._read_all_cached(self.ws_limits, "제한")
        header = {k: i for i, k in enumerate(vals[0])}
        cu = header.get("유저명");
        cd = header.get("날짜");
        cc = header.get("탐색_사용횟수")
        if None in (cu, cd, cc):
            raise RuntimeError("제한 시트 헤더(유저명/날짜/탐색_사용횟수)를 확인하세요.")

        # 2) 오늘 행이 있으면 +1 (⚠ gspread는 1-based 인덱스)
        for r, row in enumerate(vals[1:], start=2):
            if (row[cu] or "").strip() == handle and (row[cd] or "").strip() == ymd:
                cur = int(row[cc] or 0) + 1
                self._with_retry(self.ws_limits.update_cell, r, cc + 1, cur)
                self._invalidate_cache("제한")  # ← 쓰기 후 캐시 무효화
                return

        # 3) 없으면 새 행 추가
        self._with_retry(self.ws_limits.append_row, [handle, ymd, 1], value_input_option="USER_ENTERED")
        self._invalidate_cache("제한")

    # ---------- 탐색(부모구역/세션 방식) ----------
    def node_exists(self, area: str) -> bool:
        vals = self._read_all_cached(self.ws_explore, "탐색")
        header = {k: i for i, k in enumerate(vals[0])}
        ia = header.get("구역")

        if ia is None:
            raise RuntimeError("탐색 헤더에 '구역'이 없습니다.")
        for row in vals[1:]:
            if (row[ia] or "").strip() == area:
                return True

        return False

    def get_node_config(self, area: str):
        """
        '탐색' 시트에서 구역==area 인 첫 행을 설정으로 읽어 dict로 반환.
        헤더: 구역 | 부모구역 | 장소스크립트 | 갈레온_최소 | 갈레온_최대 | 아이템명 | 아이템수량 | 소문스크립트
        """
        vals = self._read_all_cached(self.ws_explore, "탐색")
        header = {k: i for i, k in enumerate(vals[0])}

        def idx(k):
            v = header.get(k)
            if v is None:
                raise RuntimeError("탐색 헤더를 확인하세요. (구역/부모구역/장소스크립트/갈레온_최소/갈레온_최대/아이템명/아이템수량/소문스크립트)")
            return v

        ia, ipar, ips, imin, imax, iitem, iqty, irum = map(idx, [
            "구역", "부모구역", "장소스크립트", "갈레온_최소", "갈레온_최대", "아이템명", "아이템수량", "소문스크립트"
        ])

        for row in vals[1:]:
            if (row[ia] or "").strip() == area:
                def to_int(s, default=0):
                    s = (s or "").strip()
                    try:
                        return int(s)
                    except:
                        return default

                return {
                    "area": area,
                    "place": row[ips] or "",
                    "gmin": to_int(row[imin], 0),
                    "gmax": to_int(row[imax], 0),
                    "item": (row[iitem] or "").strip(),
                    "qty": max(0, to_int(row[iqty], 0)),
                    "rumor": (row[irum] or "").strip(),
                }
        return None

    def list_children(self, parent: str) -> List[str]:
        """부모구역 == parent 인 모든 행의 '구역' 이름을 유니크 집합으로 반환."""
        vals = self._read_all_cached(self.ws_explore, "탐색")
        header = {k: i for i, k in enumerate(vals[0])}
        ia = header.get("구역"); ipar = header.get("부모구역")

        if None in (ia, ipar):
            raise RuntimeError("탐색 헤더에 '구역' 또는 '부모구역'이 없습니다.")

        children = set()

        for row in vals[1:]:
            if (row[ipar] or "").strip() == (parent or ""):
                child = (row[ia] or "").strip()
                if child:
                    children.add(child)

        return sorted(children)

    # ---------- 세션 ----------
    def get_session_row(self, handle: str):
        """세션 워크시트에서 해당 유저의 (row_idx, 현재경로)를 반환. 없으면 생성."""
        vals = self._read_all_cached(self.ws_session, "세션")
        header = {k: i for i, k in enumerate(vals[0])}
        cu = header.get("유저명"); cp = header.get("현재경로")
        if None in (cu, cp):
            raise RuntimeError("세션 워크시트 헤더를 확인하세요. (유저명/현재경로)")
        for r, row in enumerate(vals[1:], start=2):
            if (row[cu] or "").strip() == handle:
                return r, (row[cp] or "")
        # 없으면 생성
        self._with_retry(self.ws_session.append_row, [handle, "", ""], value_input_option="USER_ENTERED")
        self._invalidate_cache("세션")
        return self.get_session_row(handle)

    def set_session_path(self, row_idx: int, path: str, updated_at: str):
        # 2=현재경로, 3=마지막업데이트 (1-based)
        self._with_retry(self.ws_session.update_cell, row_idx, 2, path)
        self._with_retry(self.ws_session.update_cell, row_idx, 3, updated_at)
        self._invalidate_cache("세션")

    # ---------- 가방(통화/아이템) ----------
    def _bag_user_col(self, handle: str) -> Optional[int]:
        if not self.ws_bag:
            return None
        header = self._with_retry(self.ws_bag.row_values, 1)
        target = f"@{handle}" if self.cfg.USER_COLUMN_STYLE == "with_at" else handle
        for idx, name in enumerate(header, start=1):
            if name.strip() == target:
                return idx
        # 새 열
        next_col = len(header) + 1
        self._with_retry(self.ws_bag.update_cell, 1, next_col, target)
        self._invalidate_cache("가방")
        return next_col

    def _bag_row_of(self, item_name: str) -> Optional[int]:
        names = self._with_retry(self.ws_bag.col_values, 1)
        for r, v in enumerate(names[1:], start=2):
            if (v or "").strip() == item_name:
                return r
        # 새 행
        new_r = len(names) + 1
        self._with_retry(self.ws_bag.update_cell, new_r, 1, item_name)
        self._invalidate_cache("가방")
        return new_r

    def add_currency(self, handle: str, amount: int):
        if not self.ws_bag or amount == 0:
            return
        key = self.get_config().get("통화키", "갈레온")
        col = self._bag_user_col(handle)
        row = self._bag_row_of(key)
        cur_val = self._with_retry(self.ws_bag.cell, row, col).value or 0
        cur = int(cur_val)
        self._with_retry(self.ws_bag.update_cell, row, col, cur + amount)
        self._invalidate_cache("가방")

    def add_item(self, handle: str, item: str, qty: int):
        if not self.ws_bag or qty == 0:
            return
        col = self._bag_user_col(handle)
        row = self._bag_row_of(item)
        cur_val = self._with_retry(self.ws_bag.cell, row, col).value or 0
        cur = int(cur_val)
        self._with_retry(self.ws_bag.update_cell, row, col, cur + qty)
        self._invalidate_cache("가방")

    def has_participation(self, typ: str, notice_id: str, handle: str) -> bool:
        vals = self._read_all_cached(self.ws_particip, "참여기록")
        hdr = {k: i for i, k in enumerate(vals[0])}
        it, iid, iu = hdr.get("유형"), hdr.get("공지ID"), hdr.get("유저명")
        for row in vals[1:]:
            if row[it] == typ and row[iid] == str(notice_id) and row[iu] == handle:
                return True
        return False

    def append_participation(self, typ: str, notice_id: str, handle: str, ts: str):
        self._with_retry(self.ws_particip.append_row, [typ, str(notice_id), handle, ts],
                         value_input_option="USER_ENTERED")
        self._invalidate_cache("참여기록")

    def _with_retry(self, func, *args, **kwargs):
        """gspread 호출용 지수 백오프 래퍼 (429/500/503 재시도)"""
        delay = 0.5
        for attempt in range(4):  # 최대 4회
            try:
                return func(*args, **kwargs)
            except APIError as e:
                # 일부 gspread 버전은 e.response.status가 없을 수도 있어 안전하게 처리
                resp = getattr(e, "response", None)
                code = getattr(resp, "status", None) if resp else None
                if code in (429, 500, 503):
                    if attempt == 3:
                        raise
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise

    def _read_all_cached(self, ws, key: str):
        """ws.get_all_values()에 짧은 TTL 캐시를 적용."""
        now = time.time()
        hit = self._sheet_cache.get(key)
        if hit and (now - hit[0] <= self._sheet_cache_ttl):
            return hit[1]
        rows = self._with_retry(ws.get_all_values)
        self._sheet_cache[key] = (now, rows)
        return rows

    def _invalidate_cache(self, key: str):
        """해당 키 캐시 무효화 (쓰기 직후 호출)"""
        self._sheet_cache.pop(key, None)

