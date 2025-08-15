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
    return (now.ho
