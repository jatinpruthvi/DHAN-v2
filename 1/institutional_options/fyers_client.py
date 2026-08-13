"""Dependency-free Fyers v3 REST client for the paper trading runner.

Implements the endpoints validated against the live Fyers API:

  * POST /api/v3/validate-authcode      auth-code -> access + refresh token
  * POST /api/v3/validate-refresh-token refresh token -> new access token
  * GET  /data/options-chain-v3         option chain + expiry calendar + VIX
  * GET  /data/quotes                   quotes for up to 50 symbols
  * GET  /data/history                  historical candles
  * GET  https://public.fyers.in/sym_details/NSE_FO.csv  symbol master (no auth)

Notes on the Fyers v3 quirks that were verified live:

  * ``appIdHash`` is SHA-256 of ``appId-appType:appSecret`` (hex digest), NOT
    base64.  appId here is the full "3YCSQXVJFP-100" string including app type.
  * Market data endpoints live under /data (not /api/v3) and are GET.
  * The Authorization header for data endpoints is ``<app_id>:<access_token>``.

This module never places orders. It only reads market data and manages the
session tokens needed to do so.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

AUTH_BASE = "https://api-t1.fyers.in/api/v3"
DATA_BASE = "https://api-t1.fyers.in/data"
SYMBOL_MASTER_URL = "https://public.fyers.in/sym_details/NSE_FO.csv"
REDIRECT_URI = "https://trade.fyers.in/api-login/redirect-uri/index.html"
UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


class FyersAPIError(RuntimeError):
    pass


@dataclass(frozen=True)
class FyersCredentials:
    app_id: str          # full "APPID-100" string
    secret_id: str
    pin: str = ""        # trading PIN required by Fyers v3 validate-refresh-token

    @property
    def app_id_hash(self) -> str:
        return hashlib.sha256(f"{self.app_id}:{self.secret_id}".encode()).hexdigest()

    @classmethod
    def from_env(cls, app_id_env: str = "FYERS_APP_ID", secret_env: str = "FYERS_SECRET_ID",
                 pin_env: str = "FYERS_PIN") -> "FyersCredentials":
        app_id = os.getenv(app_id_env, "")
        secret = os.getenv(secret_env, "")
        if not app_id or not secret:
            raise FyersAPIError(f"Missing Fyers credentials in {app_id_env}/{secret_env}.")
        return cls(app_id=app_id, secret_id=secret, pin=os.getenv(pin_env, "") or "")


@dataclass
class TokenStore:
    """Persists access + refresh tokens to a JSON file (gitignored directory)."""

    path: str | Path
    access_token: str = ""
    refresh_token: str = ""
    updated_at: Optional[str] = None

    def load(self) -> None:
        p = Path(self.path)
        if not p.exists():
            return
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            self.access_token = raw.get("access_token", "")
            self.refresh_token = raw.get("refresh_token", "")
            self.updated_at = raw.get("updated_at")
        except (json.JSONDecodeError, OSError):
            self.access_token = ""
            self.refresh_token = ""

    def save(self, access_token: str, refresh_token: str) -> None:
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.updated_at = datetime.now(timezone.utc).isoformat()
        p = Path(self.path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({
            "access_token": access_token,
            "refresh_token": refresh_token,
            "updated_at": self.updated_at,
        }, indent=2), encoding="utf-8")

    def clear(self) -> None:
        p = Path(self.path)
        if p.exists():
            p.unlink()


def _req(url: str, method: str = "GET", body: Optional[Mapping[str, Any]] = None,
         auth_header: str = "", timeout: int = 60, raw_text: bool = False):
    headers = dict(UA)
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if auth_header:
        headers["Authorization"] = auth_header
        headers["version"] = "3"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if raw_text:
                return resp.status, raw.decode("utf-8", "replace")
            try:
                return resp.status, json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return resp.status, raw[:500].decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:500]
        raise FyersAPIError(f"Fyers HTTP {e.code} on {url}: {detail}") from e
    except urllib.error.URLError as e:
        raise FyersAPIError(f"Fyers URL error on {url}: {e}") from e


def login_url(app_id: str, state: str = "paper-runner") -> str:
    return (f"{AUTH_BASE}/generate-authcode?"
            + urllib.parse.urlencode({"client_id": app_id, "redirect_uri": REDIRECT_URI,
                                      "response_type": "code", "state": state}))


class FyersRestClient:
    """Read-only Fyers market data client (never places orders)."""

    def __init__(self, credentials: FyersCredentials, token_store: TokenStore,
                 timeout: int = 60):
        self.credentials = credentials
        self.tokens = token_store
        self.tokens.load()
        self.timeout = timeout
        self._auth_header = ""

    # -- session management ---------------------------------------------------

    def ensure_session(self, interactive: bool = True) -> str:
        """Return a usable Authorization header, refreshing or logging in as needed."""
        if self._auth_header and self.tokens.access_token:
            return self._auth_header
        if not self.tokens.access_token:
            self._interactive_login() if interactive else self._raise_no_session()
            return self._auth_header
        # Try the saved access token; refresh if it fails.
        header = self._combined_header()
        if self._probe(header):
            self._auth_header = header
            return header
        if self.tokens.refresh_token:
            try:
                self._refresh()
                header = self._combined_header()
                if self._probe(header):
                    self._auth_header = header
                    return header
            except FyersAPIError:
                pass
        self.tokens.clear()
        if interactive:
            self._interactive_login()
            return self._auth_header
        self._raise_no_session()

    def _combined_header(self) -> str:
        return f"{self.credentials.app_id}:{self.tokens.access_token}"

    def _probe(self, header: str) -> bool:
        try:
            status, _ = _req(f"{DATA_BASE}/quotes?" + urllib.parse.urlencode({"symbols": "NSE:NIFTY50-INDEX"}),
                             auth_header=header, timeout=20)
            return status == 200
        except FyersAPIError:
            return False

    def _pin(self) -> str:
        """Fyers v3 refresh requires the trading PIN. Prefer the FYERS_PIN env
        var; fall back to a FYERS_PIN= line in creds.env next to the token file
        (paper_state/creds.env, gitignored)."""
        if self.credentials.pin:
            return self.credentials.pin
        try:
            env_path = Path(self.tokens.path).parent / "creds.env"
            if env_path.exists():
                for line in env_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line.startswith("FYERS_PIN=") and len(line) > len("FYERS_PIN="):
                        return line[len("FYERS_PIN="):].strip().strip('"').strip("'")
        except OSError:
            pass
        return ""

    def _refresh(self) -> None:
        body = {
            "grant_type": "refresh_token",
            "appIdHash": self.credentials.app_id_hash,
            "refresh_token": self.tokens.refresh_token,
        }
        pin = self._pin()
        if pin:
            body["pin"] = pin
        status, resp = _req(f"{AUTH_BASE}/validate-refresh-token", "POST", body)
        if not isinstance(resp, dict) or resp.get("s") != "ok" or not resp.get("access_token"):
            raise FyersAPIError(f"Refresh failed: {resp}")
        self.tokens.save(resp["access_token"], resp.get("refresh_token") or self.tokens.refresh_token)

    def _interactive_login(self) -> None:
        print("\n" + "=" * 70)
        print("FYERS LOGIN REQUIRED (one-time; token is saved to paper_state/)")
        print("Open this URL in your browser:")
        print(login_url(self.credentials.app_id))
        print("=" * 70)
        auth_code = input("Paste the auth_code from the address bar: ").strip()
        status, resp = _req(f"{AUTH_BASE}/validate-authcode", "POST", {
            "grant_type": "authorization_code",
            "appIdHash": self.credentials.app_id_hash,
            "code": auth_code,
        })
        if not isinstance(resp, dict) or resp.get("s") != "ok" or not resp.get("access_token"):
            raise FyersAPIError(f"Auth-code exchange failed: {resp}")
        self.tokens.save(resp["access_token"], resp.get("refresh_token", ""))
        self._auth_header = self._combined_header()

    def _raise_no_session(self) -> None:
        raise FyersAPIError("No Fyers session. Provide FYERS_APP_ID/FYERS_SECRET_ID "
                            "env vars and run interactively to log in once.")

    # -- market data (read-only) ---------------------------------------------

    def option_chain(self, symbol: str, strikecount: int = 30, expiry_timestamp: str = "",
                     header: str = "") -> Any:
        params = {"symbol": symbol, "strikecount": str(strikecount)}
        if expiry_timestamp:
            params["timestamp"] = expiry_timestamp
        url = f"{DATA_BASE}/options-chain-v3?" + urllib.parse.urlencode(params)
        status, resp = _req(url, auth_header=header or self._auth_header, timeout=self.timeout)
        return resp

    def quotes(self, symbols: list[str], header: str = "") -> Any:
        url = f"{DATA_BASE}/quotes?" + urllib.parse.urlencode({"symbols": ",".join(symbols)})
        status, resp = _req(url, auth_header=header or self._auth_header, timeout=self.timeout)
        return resp

    def history(self, symbol: str, resolution: str = "1",
                range_from: str | int = "", range_to: str | int = "",
                cont_flag: str = "1", header: str = "") -> Any:
        params = {"symbol": symbol, "resolution": resolution,
                  "date_format": "1", "range_from": range_from, "range_to": range_to,
                  "cont_flag": cont_flag}
        url = f"{DATA_BASE}/history?" + urllib.parse.urlencode(params)
        status, resp = _req(url, auth_header=header or self._auth_header, timeout=self.timeout)
        return resp

    # -- symbol master ---------------------------------------------------------

    def download_symbol_master(self, output_path: str | Path) -> Path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        status, text = _req(SYMBOL_MASTER_URL, raw_text=True, timeout=120)
        out.write_text(text, encoding="utf-8")
        return out

    def fetch_symbol_master(self, output_path: str | Path, max_age_hours: float = 20.0) -> Path:
        """Download the NSE_FO master once per day (it refreshes nightly)."""
        out = Path(output_path)
        if out.exists() and (time.time() - out.stat().st_mtime) < max_age_hours * 3600:
            return out
        return self.download_symbol_master(out)


@dataclass(frozen=True)
class FyersInstrument:
    """One option contract from the NSE_FO symbol master."""

    fyers_symbol: str      # e.g. NSE:NIFTY26AUG5000CE
    token: str
    underlying: str        # NIFTY / BANKNIFTY / FINNIFTY / MIDCPNIFTY
    expiry_date: Optional[date]
    expiry_ts: int
    strike: Optional[float]
    option_type: Optional[str]
    lot_size: int
    tick_size: float


class FyersSymbolMaster:
    """Parsed NSE_FO.csv: option contracts with expiry, lot and tick per underlying.

    Column layout (0-indexed) of NSE_FO.csv:
      0 token, 1 display name, 2 exchange, 3 lot size, 4 tick, 5 -, 6 hours,
      7 date string, 8 expiry unix ts, 9 Fyers symbol (NSE:...), 12 strike/token,
      13 underlying, ...
    """

    COL_LOT = 3
    COL_TICK = 4
    COL_EXPIRY_TS = 8
    COL_SYM = 9
    COL_UND = 13

    def __init__(self, instruments: list[FyersInstrument]):
        self.instruments = tuple(instruments)
        self._by_und_expiry: dict[tuple[str, date], list[FyersInstrument]] = {}
        for inst in self.instruments:
            if inst.expiry_date is None:
                continue
            self._by_und_expiry.setdefault((inst.underlying.upper(), inst.expiry_date), []).append(inst)

    @classmethod
    def from_csv(cls, path: str | Path) -> "FyersSymbolMaster":
        out: list[FyersInstrument] = []
        with Path(path).open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.reader(f):
                if len(row) <= cls.COL_SYM:
                    continue
                sym = row[cls.COL_SYM]
                if not sym.startswith("NSE:"):
                    continue
                underlying = row[cls.COL_UND].strip().upper()
                if underlying not in {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}:
                    continue
                try:
                    expiry_ts = int(float(row[cls.COL_EXPIRY_TS]))
                    lot = int(float(row[cls.COL_LOT]))
                    tick = float(row[cls.COL_TICK])
                except (ValueError, IndexError):
                    continue
                expiry_date = datetime.fromtimestamp(expiry_ts).date()
                strike, opt = cls._parse_option_symbol(sym)
                out.append(FyersInstrument(sym, row[0], underlying, expiry_date, expiry_ts,
                                           strike, opt, lot, tick))
        return cls(out)

    @staticmethod
    def _parse_option_symbol(sym: str) -> tuple[Optional[float], Optional[str]]:
        # Fyers option symbols: NSE:NIFTY26AUG5000CE / NSE:BANKNIFTY26AUG50000PE
        body = sym[4:]
        if body.endswith("CE") or body.endswith("PE"):
            opt = body[-2:]
            digits = "".join(ch for ch in body[:-2] if ch.isdigit())
            # strike is the trailing digits that do not belong to the yymon code.
            import re
            m = re.search(r"(\d{5,})$", body[:-2])
            if m:
                return float(m.group(1)), opt
            return (float(digits[-6:]) if digits else None), opt
        return None, None

    def expiry_dates(self, underlying: str) -> tuple[date, ...]:
        return tuple(sorted({e for (u, e) in self._by_und_expiry if u == underlying.upper()}))

    def lot_size(self, underlying: str, expiry: date) -> int:
        rows = self._by_und_expiry.get((underlying.upper(), expiry), [])
        lots = {r.lot_size for r in rows}
        if not lots:
            raise FyersAPIError(f"No instrument rows for {underlying} {expiry}")
        # Prefer the most common lot size; some expiries carry multiple.
        from collections import Counter
        return Counter(lots).most_common(1)[0][0]

    def tick_size(self, underlying: str, expiry: date) -> float:
        rows = self._by_und_expiry.get((underlying.upper(), expiry), [])
        if not rows:
            raise FyersAPIError(f"No instrument rows for {underlying} {expiry}")
        ticks = {r.tick_size for r in rows}
        return min(ticks)

    def symbol_for(self, underlying: str, expiry: date, strike: float, option_type: str) -> str:
        rows = self._by_und_expiry.get((underlying.upper(), expiry), [])
        for r in rows:
            if r.strike is not None and abs(r.strike - strike) < 1e-6 and (r.option_type or "").upper() == option_type.upper():
                return r.fyers_symbol
        raise FyersAPIError(f"No Fyers symbol for {underlying} {expiry} {strike} {option_type}")
