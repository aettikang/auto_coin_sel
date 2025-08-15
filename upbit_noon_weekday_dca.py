# -*- coding: utf-8 -*-
"""
GitHub Actions/서버용 업비트 DCA 봇
- 평일(월~금) TARGET_HOUR_KST 시각에 KRW-BTC, KRW-ETH 시장가 매수
- 수수료율 반영(총지출 ≤ 일일예산), 최소 주문 5,000원 보장
필수 Secrets/환경변수:
  UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY
선택 환경변수:
  UPBIT_KRW_FEE(기본 0.0005), DAILY_BUDGET_KRW(기본 40000)
  TARGET_HOUR_KST(기본 10), STRICT_TIME_ONLY(true/false, 기본 true), WINDOW_MINUTES(기본 15)
  DCA_PAUSE(1이면 즉시 종료)
의존성: requests, tzdata, pyjwt
"""

import os, json, uuid, time, hashlib, logging
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import jwt          # PyJWT
import requests
from requests import HTTPError

# ===== 설정 =====
KST = ZoneInfo("Asia/Seoul")

# 목표 시각 및 윈도우(분) — 환경변수로 제어
TARGET_HOUR_KST  = int(os.environ.get("TARGET_HOUR_KST", "10"))
STRICT_TIME_ONLY = os.environ.get("STRICT_TIME_ONLY", "true").lower() == "true"
WINDOW_MINUTES   = int(os.environ.get("WINDOW_MINUTES", "15"))

TIMEOUT = 12  # HTTP 타임아웃(초)

# 일일 예산(수수료 포함)과 비중
DAILY_BUDGET_KRW = float(os.environ.get("DAILY_BUDGET_KRW", "40000"))
PAIRS = [("KRW-BTC", 0.5), ("KRW-ETH", 0.5)]

# 기본 수수료율(환경변수로 덮어쓰기 가능) - KRW 마켓 0.05% 가정
FEE_RATE = float(os.environ.get("UPBIT_KRW_FEE", "0.0005"))

# KRW 마켓 최소 주문 총액
MIN_ORDER_KRW = float(os.environ.get("UPBIT_MIN_ORDER_KRW", "5000"))

# 일시정지 토글(선택): '1'이면 바로 정상 종료
DCA_PAUSE = os.environ.get("DCA_PAUSE", "0") == "1"

# API
API = "https://api.upbit.com"
ENDPOINT_ORDER = "/v1/orders"

ACCESS_KEY = os.environ.get("UPBIT_ACCESS_KEY")
SECRET_KEY = os.environ.get("UPBIT_SECRET_KEY")


def _require_env():
    missing = [k for k in ("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY") if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"환경변수 누락: {', '.join(missing)}")


def _is_weekday_kst(now: datetime) -> bool:
    return now.weekday() <= 4  # Mon=0 .. Sun=6


def _is_target_window(now: datetime) -> bool:
    if not STRICT_TIME_ONLY:
        return True
    return (now.hour == TARGET_HOUR_KST) and (0 <= now.minute <= WINDOW_MINUTES)


def _amount_net_of_fee(budget: float, fee_rate: float, min_total: float) -> int:
    """총지출 = price*(1+fee) ≤ budget → price = budget/(1+fee). 업비트 시장가 매수는 KRW 정수 권장."""
    raw = budget / (1.0 + fee_rate)
    price = int(raw)
    if price < int(min_total):
        price = int(min_total)
    return price


def _jwt_for_params(params: dict) -> str:
    """
    Upbit: query_string → SHA512(query_hash) → JWT(payload: access_key, nonce, query_hash, query_hash_alg)
    (주문은 쿼리 파라미터 방식이 가장 호환이 좋습니다)
    """
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
    """동일 identifier 재사용 시 이미 주문된 것으로 간주(멱등)."""
    if not isinstance(resp_json, dict):
        return False
    err = resp_json.get("error") or {}
    msg = (err.get("message") or "").lower()
    if "identifier" in msg and ("already" in msg or "taken" in msg or "exists" in msg):
        return True
    if "errors" in err:
        try:
            serialized = json.dumps(err["errors"]).lower()
            if "identifier" in serialized and ("already" in serialized or "taken" in serialized or "exists" in serialized):
                return True
        except Exception:
            pass
    return False


def _place_market_buy(market: str, price_krw: int, identifier: str) -> dict:
    """시장가 매수: side='bid', ord_type='price', price=KRW총액, volume 생략 (쿼리 파라미터 전송)"""
    params = {
        "market": market,
        "side": "bid",
        "ord_type": "price",
        "price": str(price_krw),
        "identifier": identifier,
    }
    token = _jwt_for_params(params)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    try:
        r = requests.post(API + ENDPOINT_ORDER, headers=headers, params=params, timeout=TIMEOUT)
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


def main() -> int:
    if DCA_PAUSE:
        print("Paused by DCA_PAUSE=1")
        return 0

    _require_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    now = datetime.now(KST)

    logging.info(
        "now=%s | weekday=%s | target=%02d:00 | strict=%s | window=%dmin",
        now.isoformat(), now.weekday(), TARGET_HOUR_KST, STRICT_TIME_ONLY, WINDOW_MINUTES
    )

    # 이중 가드
    if not _is_weekday_kst(now):
        logging.info("스킵: 주말/휴일 %s", now.isoformat())
        return 0
    if not _is_target_window(now):
        logging.info("스킵: 목표시각(%02d:00±%d분) 아님 %s",
                     TARGET_HOUR_KST, WINDOW_MINUTES, now.isoformat())
        return 0

    date_tag = now.strftime("%Y%m%d")
    results, errors = [], 0

    for market, weight in PAIRS:
        budget = DAILY_BUDGET_KRW * weight
        price_krw = _amount_net_of_fee(budget, FEE_RATE, MIN_ORDER_KRW)
        identifier = f"dca-{TARGET_HOUR_KST:02d}-{date_tag}-{market}"  # 예: dca-11-20250812-KRW-BTC

        try:
            res = _place_market_buy(market, price_krw, identifier)
            ok = res.get("result") in (None, "success", "duplicate_identifier_accepted")
            logging.info("[%s] price=%s, fee=%.5f → api_result=%s",
                         market, price_krw, FEE_RATE, res.get("result", "success"))
            results.append({
                "market": market,
                "budget_krw": budget,
                "price_krw": price_krw,
                "fee_rate": FEE_RATE,
                "identifier": identifier,
                "api_result": res,
            })
            if not ok:
                errors += 1
            time.sleep(0.25)
        except Exception as e:
            logging.exception("[%s] 주문 실패: %s", market, e)
            errors += 1

    # 요약 출력 (Actions 로그/요약에 그대로 남음)
    print(json.dumps({
        "timestamp_kst": now.isoformat(),
        "weekday": now.weekday(),
        "target_hour_kst": TARGET_HOUR_KST,
        "daily_budget_krw": DAILY_BUDGET_KRW,
        "pairs": PAIRS,
        "fee_rate": FEE_RATE,
        "results": results,
        "errors": errors,
    }, ensure_ascii=False, indent=2))

    # 에러가 있으면 비정상 종료 → GitHub Actions가 '실패'로 표시
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
