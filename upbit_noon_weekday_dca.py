# -*- coding: utf-8 -*-
"""
Upbit DCA Bot for GitHub Actions

요구사항:
1) 새벽 4시~오전 10시(KST) '매 정시' 실행 → 정각~+30분 내에서만 매수 시도
2) WINDOW_MINUTES=30 (env로 조정 가능)
3) 그날 이미 주문된(접수/체결) 종목은 이후 시도에서 스킵
4) 주문이 발생하면 텔레그램으로 즉시 알림

필수 Secrets/ENV:
  UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
의존성: requests, tzdata, pyjwt
"""

import os, json, uuid, time, hashlib, logging
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import jwt
import requests
from requests import HTTPError

# ===== 기본 설정 =====
KST = ZoneInfo("Asia/Seoul")

# 시간 설정 (워크플로 env로 제어)
WINDOW_MINUTES    = int(os.environ.get("WINDOW_MINUTES", "30"))   # 정각~+30분
STRICT_TIME_ONLY  = os.environ.get("STRICT_TIME_ONLY", "true").lower() == "true"
ALLOWED_HOURS_KST = os.environ.get("ALLOWED_HOURS_KST", "4,5,6,7,8,9,10")
ALLOWED_HOURS     = {int(h.strip()) for h in ALLOWED_HOURS_KST.split(",") if h.strip()}

# 예산/수수료/기타
DAILY_BUDGET_KRW  = float(os.environ.get("DAILY_BUDGET_KRW", "40000"))
PAIRS             = [("KRW-BTC", 0.5), ("KRW-ETH", 0.5)]
FEE_RATE          = float(os.environ.get("UPBIT_KRW_FEE", "0.0005"))
MIN_ORDER_KRW     = float(os.environ.get("UPBIT_MIN_ORDER_KRW", "5000"))
DCA_PAUSE         = os.environ.get("DCA_PAUSE", "0") == "1"

# Upbit API
API               = "https://api.upbit.com"
ENDPOINT_ORDER    = "/v1/orders"
ENDPOINT_GET_ONE  = "/v1/order"   # GET ?identifier=...

# Keys
ACCESS_KEY        = os.environ.get("UPBIT_ACCESS_KEY")
SECRET_KEY        = os.environ.get("UPBIT_SECRET_KEY")

# Telegram
TG_TOKEN          = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

def _require_env():
    missing = [k for k in ("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY")]
    for k in missing:
        if not os.environ.get(k):
            raise RuntimeError(f"환경변수 누락: {k}")

def _is_weekday_kst(now: datetime) -> bool:
    return now.weekday() <= 4  # Mon=0..Sun=6

def _is_target_window(now: datetime) -> bool:
    if not STRICT_TIME_ONLY:
        return True
    return (now.hour in ALLOWED_HOURS) and (0 <= now.minute <= WINDOW_MINUTES)

def _amount_net_of_fee(budget: float, fee_rate: float, min_total: float) -> int:
    price = int(budget / (1.0 + fee_rate))
    return max(price, int(min_total))

def _jwt_for_params(params: dict) -> str:
    query_string = urlencode(params).encode()
    query_hash = hashlib.sha512(query_string).hexdigest()
    payload = {
        "access_key": ACCESS_KEY,
        "nonce": str(uuid.uuid4()),
        "query_hash": query_hash,
        "query_hash_alg": "SHA512",
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def _is_duplicate_identifier_error(resp_json: dict) -> bool:
    if not isinstance(resp_json, dict):
        return False
    err = resp_json.get("error") or {}
    msg = (err.get("message") or "").lower()
    if "identifier" in msg and ("already" in msg or "taken" in msg or "exists" in msg):
        return True
    if "errors" in err:
        try:
            s = json.dumps(err["errors"]).lower()
            return "identifier" in s and ("already" in s or "taken" in s or "exists" in s)
        except Exception:
            pass
    return False

def _order_exists_by_identifier(identifier: str) -> bool:
    """identifier 주문이 있으면 True (그날 이미 주문됨으로 간주)"""
    params = {"identifier": identifier}
    token = _jwt_for_params(params)
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(API + ENDPOINT_GET_ONE, headers=headers, params=params, timeout=12)
        return r.status_code == 200
    except Exception:
        # 조회 실패 시 보수적으로 False → 주문 시도
        return False

def _place_market_buy(market: str, price_krw: int, identifier: str) -> dict:
    params = {
        "market": market,
        "side": "bid",
        "ord_type": "price",
        "price": str(price_krw),
        "identifier": identifier,
    }
    token = _jwt_for_params(params)
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    r = requests.post(API + ENDPOINT_ORDER, headers=headers, params=params, timeout=12)
    try:
        r.raise_for_status()
        return r.json()
    except HTTPError as e:
        try:
            resp_json = e.response.json()
        except Exception:
            resp_json = {"error_text": getattr(e.response, "text", str(e))}
        if _is_duplicate_identifier_error(resp_json):
            return {"result": "duplicate_identifier_accepted", "identifier": identifier, "market": market}
        raise

def _send_telegram(text: str):
    if not (TG_TOKEN and TG_CHAT_ID):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT_ID, "text": text},
            timeout=10,
        )
    except Exception:
        pass  # 알림 실패는 주문 로직에 영향 X

def main() -> int:
    if DCA_PAUSE:
        print("Paused by DCA_PAUSE=1")
        return 0

    _require_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    now = datetime.now(KST)

    if not _is_weekday_kst(now):
        logging.info("스킵: 주말/휴일 %s", now.isoformat())
        return 0
    if not _is_target_window(now):
        logging.info("스킵: 허용시간대 아님 (허용시각=%s, 윈도우=%d분) now=%s",
                     sorted(ALLOWED_HOURS), WINDOW_MINUTES, now.isoformat())
        return 0

    date_tag = now.strftime("%Y%m%d")

    # 오늘 이미 주문된 종목은 스킵
    markets_todo = []
    for market, weight in PAIRS:
        identifier = f"dca-{date_tag}-{market}"
        if _order_exists_by_identifier(identifier):
            logging.info("[%s] 오늘(identifier=%s) 주문 존재 → 스킵", market, identifier)
        else:
            markets_todo.append((market, weight))

    if not markets_todo:
        logging.info("오늘 모든 종목이 이미 주문 완료로 확인되어 종료.")
        return 0

    # 주문 실행
    errors = 0
    results = []
    for market, weight in markets_todo:
        budget = DAILY_BUDGET_KRW * weight
        price_krw = _amount_net_of_fee(budget, FEE_RATE, MIN_ORDER_KRW)
        identifier = f"dca-{date_tag}-{market}"
        try:
            res = _place_market_buy(market, price_krw, identifier)
            ok = res.get("result") in (None, "success", "duplicate_identifier_accepted")
            results.append({"market": market, "price_krw": price_krw, "identifier": identifier, "api_result": res})
            logging.info("[%s] 주문 결과: %s", market, res.get("result", "success"))

            if ok:
                _send_telegram(
                    f"[Upbit DCA]\n체결 요청 완료: {market}\n금액: {price_krw} KRW\n식별자: {identifier}\n시간: {now.strftime('%Y-%m-%d %H:%M:%S')} KST"
                )
            else:
                errors += 1
            time.sleep(0.25)
        except Exception as e:
            logging.exception("[%s] 주문 실패: %s", market, e)
            errors += 1

    print(json.dumps({
        "timestamp_kst": now.isoformat(),
        "weekday": now.weekday(),
        "allowed_hours_kst": sorted(ALLOWED_HOURS),
        "window_minutes": WINDOW_MINUTES,
        "daily_budget_krw": DAILY_BUDGET_KRW,
        "pairs": PAIRS,
        "fee_rate": FEE_RATE,
        "results": results,
        "errors": errors,
    }, ensure_ascii=False, indent=2))

    return 1 if errors else 0

if __name__ == "__main__":
    raise SystemExit(main())
