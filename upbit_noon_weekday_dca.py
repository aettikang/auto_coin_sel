# -*- coding: utf-8 -*-
"""
GitHub Actions/서버용 업비트 DCA 봇
- 평일(월~금) 정오 12:00 KST에 KRW-BTC, KRW-ETH 시장가 매수
- 수수료율 반영(총지출 ≤ 일일예산), 최소 주문 5,000원 보장
- 잔액/평가액 조회 없이 '주문만' 수행
필수 Secrets/환경변수:
  UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY
선택 환경변수:
  UPBIT_KRW_FEE (기본 0.0005 = 0.05%), DAILY_BUDGET_KRW (기본 40000)
의존성: requests, tzdata, pyjwt
"""

import os, json, uuid, time, hashlib, logging
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import jwt          # PyJWT
import requests

# ===== 설정값 =====
KST = ZoneInfo("Asia/Seoul")
STRICT_NOON_ONLY   = True   # 정오만 집행
NOON_MINUTE_WINDOW = 2      # 12:00~12:02 허용
TIMEOUT            = 12     # HTTP 타임아웃(초)

# 일일 예산(수수료 포함)과 비중
DAILY_BUDGET_KRW = float(os.environ.get("DAILY_BUDGET_KRW", "40000"))
PAIRS = [("KRW-BTC", 0.5), ("KRW-ETH", 0.5)]

# 수수료율 (KRW 마켓 기본 0.05%로 가정, 필요 시 환경변수로 덮어쓰기)
FEE_RATE = float(os.environ.get("UPBIT_KRW_FEE", "0.0005"))

# 최소 주문 총액 (업비트 KRW 마켓: 5,000원)
MIN_ORDER_KRW = float(os.environ.get("UPBIT_MIN_ORDER_KRW", "5000"))

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


def _is_noon_window(now: datetime) -> bool:
    if not STRICT_NOON_ONLY:
        return True
    return (now.hour == 12) and (0 <= now.minute <= NOON_MINUTE_WINDOW)


def _amount_net_of_fee(budget: float, fee_rate: float, min_total: float) -> int:
    """
    총지출 = price * (1 + fee_rate) <= budget  →  price = budget / (1 + fee_rate)
    업비트 시장가 매수(ord_type='price')에서는 'price'가 원화 총액(정수 권장).
    """
    raw = budget / (1.0 + fee_rate)
    price = int(raw)  # KRW 정수 하향
    if price < int(min_total):
        price = int(min_total)
    return price


def _jwt_for_body(params: dict) -> str:
    """
    Upbit 사양: query_string -> SHA512(query_hash) → JWT(payload: access_key, nonce, query_hash, query_hash_alg)
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


def _place_market_buy(market: str, price_krw: int, identifier: str) -> dict:
    """
    시장가 매수: side='bid', ord_type='price', price=KRW총액, volume 생략/None
    """
    body = {
        "market": market,
        "side": "bid",
        "ord_type": "price",
        "price": str(price_krw),
        "identifier": identifier,
    }
    token = _jwt_for_body(body)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    r = requests.post(API + ENDPOINT_ORDER, headers=headers, data=json.dumps(body), timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def main() -> int:
    _require_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    now = datetime.now(KST)

    # 이중 가드 (스케줄 오류 대비)
    if not _is_weekday_kst(now):
        logging.info("스킵: 주말/휴일 %s", now.isoformat())
        return 0
    if not _is_noon_window(now):
        logging.info("스킵: 정오(12:00±%d분) 아님 %s", NOON_MINUTE_WINDOW, now.isoformat())
        return 0

    date_tag = now.strftime("%Y%m%d")
    results, errors = [], 0

    for market, weight in PAIRS:
        budget = DAILY_BUDGET_KRW * weight
        price_krw = _amount_net_of_fee(budget, FEE_RATE, MIN_ORDER_KRW)
        identifier = f"dca-noon-{date_tag}-{market}"

        try:
            res = _place_market_buy(market, price_krw, identifier)
            logging.info("[%s] price=%s, fee=%.5f → result=%s", market, price_krw, FEE_RATE, res.get("uuid", res.get("error", {})))
            results.append({
                "market": market,
                "budget_krw": budget,
                "price_krw": price_krw,
                "fee_rate": FEE_RATE,
                "identifier": identifier,
                "api_result": res,
            })
            time.sleep(0.3)  # 짧은 간격
        except Exception as e:
            logging.exception("[%s] 주문 실패: %s", market, e)
            errors += 1

    print(json.dumps({
        "timestamp_kst": now.isoformat(),
        "weekday": now.weekday(),
        "daily_budget_krw": DAILY_BUDGET_KRW,
        "pairs": PAIRS,
        "fee_rate": FEE_RATE,
        "results": results,
        "errors": errors,
    }, ensure_ascii=False, indent=2))

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
