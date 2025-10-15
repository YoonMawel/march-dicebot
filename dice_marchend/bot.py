import logging, threading, heapq, time, re
import queue
from mastodon import Mastodon, StreamListener
from .config import Config
from .sheets import Sheets
from .utils import html_to_text
from .commands import dice as cmd_dice, yn as cmd_yn, attendance as cmd_att, explore as cmd_exp, confirm as cmd_cf

PROCESS_WORKERS = 6  # 동시에 처리할 핸들러 스레드 수
SEND_GAP_GLOBAL = 8.0     # 전역 최소 간격(초) — 모든 응답 사이
SEND_GAP_PER_ACCT = 8.0   # 계정별 최소 간격(초) — 같은 유저에게 연속 응답 시
RELOAD_INTERVAL_SEC = 1200.0  # 설정 재로딩 주기(초). 이것도 코드 상수로 고정

CMD_RE = re.compile(r"\[(.*?)\]")
DICE_ANY_RE = re.compile(r"\[\s*\d+[dD]\d+(?:\s*[+-]\s*\d+)?\s*\]")
YN_ANY_RE   = re.compile(r"\[(?:\s*YN\s*)\]|\bYN\b", re.I)  # 소문자 yn 포함

class DiceListener(StreamListener):
    def __init__(self, api: Mastodon, sheets: Sheets, cfg: Config):
        super().__init__()
        self.api = api
        self.sheets = sheets
        self.cfg = cfg
        me = self.api.account_verify_credentials()
        self.me = me["acct"]
        logging.info(f"Bot login @{self.me}")

        # 전송 큐(페이싱)
        self._pq = []   # (ready_time, seq, in_reply_to_id, text)
        self._last = {} # acct -> last ready_time
        self._seq = 0
        self._cv = threading.Condition()

        # 텀(초): 전역/계정별 둘 다 적용 가능 (환경변수로 조정)
        self._gap_global = SEND_GAP_GLOBAL
        self._gap_acct = SEND_GAP_PER_ACCT
        self._last["_global"] = time.monotonic()

        # 발송 스레드
        t = threading.Thread(target=self._sender, daemon=True)
        t.start()

        #설정 리로드 타이머
        self._reload_interval = RELOAD_INTERVAL_SEC
        rt = threading.Thread(target=self._reloader, daemon=True)
        rt.start()

        self._inbox = queue.Queue(maxsize=10000)
        for _ in range(PROCESS_WORKERS):
            threading.Thread(target=self._worker, daemon=True).start()

    def _reloader(self):
        while True:
            time.sleep(self._reload_interval)
            try:
                self.sheets.force_reload()
                logging.info("Sheets config cache invalidated (periodic).")
            except Exception as e:
                logging.exception("config reload failed: %s", e)

    def _enqueue(self, acct: str, reply_to_id: str, text: str):
        key = acct or "_anon"

        with self._cv:  # 계산~push까지 원자화
            now = time.monotonic()
            ready = max(
                now,
                self._last.get("_global", 0.0) + self._gap_global,
                self._last.get(key, 0.0) + self._gap_acct,
            )

            self._last["_global"] = ready
            self._last[key] = ready
            self._seq += 1

            heapq.heappush(self._pq, (ready, self._seq, reply_to_id, text))

            self._cv.notify()

    def _sender(self):
        while True:
            with self._cv:
                while not self._pq:
                    self._cv.wait()
                rt, seq, irt, text = heapq.heappop(self._pq)
                now = time.monotonic()
                if rt > now:
                    self._cv.wait(timeout=rt - now)
                    heapq.heappush(self._pq, (rt, seq, irt, text))
                    continue
            try:
                self.api.status_post(text, in_reply_to_id=irt, visibility="public")
            except Exception as e:
                logging.exception("send failed: %s", e)

    def _maybe_update_nickname(self, status, row_idx, runner):
        conf = self.sheets.get_config()
        policy = (conf.get("닉네임_업데이트") or "missing").lower()
        display_name = (status.get("account", {}).get("display_name") or "").strip()
        if not display_name:
            return
        if policy == "always":
            self.sheets.update_runner_nickname(row_idx, display_name)
        elif policy == "missing" and not (runner.nickname or "").strip():
            self.sheets.update_runner_nickname(row_idx, display_name)

    def on_notification(self, notif: dict):
        if notif.get("type") != "mention":
            return
        try:
            self._inbox.put(notif, timeout=1.0)  # 0.5초 대기 후 포기
        except queue.Full:
            status = notif.get("status") or {}
            acct = (status.get("account", {}) or {}).get("acct") or ""
            logging.warning("inbox full: dropping mention from %s", acct)

    def _worker(self):
        while True:
            notif = self._inbox.get()
            try:
                status = notif.get("status") or {}
                acct = status.get("account", {}).get("acct") or ""
                text = html_to_text(status.get("content", ""))
                reply_to = status.get("id")

                # 러너 로드 & 닉네임 정책 (유저행 추가/갱신이 있을 수 있어 유저락)
                with self.sheets.lock_for(acct):
                    row_idx, runner = self.sheets.get_runner_row(acct)
                    self._maybe_update_nickname(status, row_idx, runner)

                # 1) NdM(+/-K) 선처리
                if DICE_ANY_RE.search(text):
                    lines = cmd_dice.handle(text)
                    if lines:
                        msg = "\n".join(lines)
                        if acct: msg = f"@{acct} {msg}"
                        self._enqueue(acct, reply_to, msg)
                    continue

                # 2) YN (대괄호/소문자 허용)
                if YN_ANY_RE.search(text):
                    msg = cmd_yn.handle(status, self.sheets, self.cfg)
                    if acct: msg = f"@{acct} {msg}"
                    self._enqueue(acct, reply_to, msg)
                    continue

                # 3) 대괄호 커맨드 파싱
                cmds = CMD_RE.findall(text)
                if not cmds:
                    continue
                cmd = (cmds[0] or "").strip()

                if re.fullmatch(r"\d+[dD]\d+(?:\s*[+-]\s*\d+)?", cmd):
                    msg = "\n".join(cmd_dice.handle(f"[{cmd}]"))

                elif cmd.casefold() == "yn":
                    msg = cmd_yn.handle(status, self.sheets, self.cfg)

                elif cmd == "출석":
                    allowed, root = self._is_allowed_reply(status, "출석")
                    # 유저별 쓰기(점수/날짜/통화) 구간은 락으로 감싸기
                    with self.sheets.lock_for(acct):
                        msg = cmd_att.handle(status, self.sheets, self.cfg, allowed, str(root.get("id") or ""))

                elif cmd.startswith("탐색/"):
                    area = cmd.split("/", 1)[1].strip()
                    # 탐색은 핸들러 내부에서 보상 처리 시점에 유저락을 잡도록 구현됨
                    msg = cmd_exp.handle(acct, area, self.sheets, self.cfg)

                elif cmd == "참여 확인":
                    allowed, root = self._is_allowed_reply(status, "확인")
                    with self.sheets.lock_for(acct):
                        msg = cmd_cf.handle(status, self.sheets, self.cfg, allowed, str(root.get("id") or ""))

                else:
                    continue

                if acct:
                    msg = f"@{acct} {msg}"
                self._enqueue(acct, reply_to, msg)

            except Exception as e:
                logging.exception("worker error: %s", e)
                try:
                    status = notif.get("status") or {}
                    acct = (status.get("account", {}) or {}).get("acct") or ""
                    reply_to = status.get("id")
                    err = f"오류: {e}"
                    if acct: err = f"@{acct} {err}"
                    self._enqueue(acct, reply_to, err)
                except Exception:
                    pass
            finally:
                self._inbox.task_done()

    def _get_thread_root(self, status: dict):
        root = status
        hops = 0
        try:
            while root.get("in_reply_to_id") and hops < 10:
                root = self.api.status(root["in_reply_to_id"])
                hops += 1
        except Exception:
            pass
        return root

    def _is_allowed_reply(self, status: dict, purpose: str) -> tuple[bool, dict]:
        conf = self.sheets.get_config()
        root = self._get_thread_root(status)

        explicit_id_key = "출석_허용_상태ID" if purpose == "출석" else "확인_허용_상태ID"
        explicit_id = (conf.get(explicit_id_key) or "").strip()
        if explicit_id and explicit_id != "0":
            if str(status.get("in_reply_to_id") or "") == explicit_id:
                return True, root

        allowed_accounts = [
            a.strip() for a in (conf.get("공지_발신자_허용", "") or "").split(",") if a.strip()
        ]
        kw_key = "출석_공지_키워드" if purpose == "출석" else "확인_공지_키워드"
        kw = (conf.get(kw_key) or "").strip()

        if not allowed_accounts and not kw and not explicit_id:
            return True, root

        root_acct = (root.get("account", {}) or {}).get("acct", "") or ""
        root_text = html_to_text(root.get("content", "") or "")

        acct_ok = (not allowed_accounts) or (root_acct in allowed_accounts)
        kw_ok   = (not kw) or (kw in root_text)

        return (acct_ok and kw_ok), root