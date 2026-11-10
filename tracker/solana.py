# -*- coding: utf-8 -*-
"""Solana wallet fetcher — configurable RPC nodes + SPL price providers.

- RPC nodes: portfolio_sources.json -> "solana" -> "rpc" (tried in order,
  automatic failover; last working node remembered)
- SPL prices: "solana" -> "spl_prices" -> "providers" (dexscreener / coingecko)
- Optional Birdeye wallet pricing: "solana" -> "birdeye" (needs free API key)
"""
import hashlib
import time

from . import sources
from .api import http_get_json, http_post_json

TOKEN_PROGRAMS = (
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",   # SPL Token
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",   # Token-2022
)
WSOL = "So11111111111111111111111111111111111111112"

# Known SPL tokens for ATA batch lookup when public RPCs block getTokenAccountsByOwner
KNOWN_SPL_MINTS = {
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": ("USDC", "USD Coin"),
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": ("USDT", "Tether USD"),
    "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN": ("TRUMP", "OFFICIAL TRUMP"),
    "FUAfBo2jgks6gB4Z4LfZkqSZgzNucisEHqnNebaRxM1P": ("MELANIA", "Melania Meme"),
    "poLisWXnNRwC6oBu1vHiuKQzFjGL4XDSu4g9qjz9qVk": ("POLIS", "Star Atlas DAO"),
    "ATLASXmbPQxBUYbxPsV97usA3fPQYEqzQBUHgiFCUsXx": ("ATLAS", "Star Atlas"),
    "sonarX4VtVkQemriJeLm6CKeW3GDMyiBnnAEMw1MRAE": ("SONAR", "SonarWatch"),
    "AURYydfxJib1ZkTir1Jn1J9ECYUtjb6rKQVmtYaixWPP": ("AURY", "Aurory"),
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN": ("JUP", "Jupiter"),
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R": ("RAY", "Raydium"),
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263": ("BONK", "Bonk"),
    "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3": ("PYTH", "Pyth Network"),
    "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL": ("JTO", "Jito"),
}

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_ED25519_P = (1 << 255) - 19
_ED25519_D = -121665 * pow(121666, _ED25519_P - 2, _ED25519_P) % _ED25519_P


def _b58decode32(s):
    n = 0
    for c in s:
        n = n * 58 + _B58_ALPHABET.index(c)
    return n.to_bytes(32, "big")


def _b58encode(b):
    n = int.from_bytes(b, "big")
    res = []
    while n > 0:
        n, rem = divmod(n, 58)
        res.append(_B58_ALPHABET[rem])
    pad = len(b) - len(b.lstrip(b"\x00"))
    return "1" * pad + "".join(reversed(res))


def _is_on_curve(b):
    y = int.from_bytes(b, "little") & ((1 << 255) - 1)
    if y >= _ED25519_P:
        return False
    u = (y * y - 1) % _ED25519_P
    v = (_ED25519_D * y * y + 1) % _ED25519_P
    x2 = u * pow(v, _ED25519_P - 2, _ED25519_P) % _ED25519_P
    if x2 == 0:
        return True
    return pow(x2, (_ED25519_P - 1) // 2, _ED25519_P) == 1


_ATA_PROG = _b58decode32("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
_TOK_PROG = _b58decode32("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")


def _find_ata(owner_b58, mint_b58):
    owner = _b58decode32(owner_b58)
    mint = _b58decode32(mint_b58)
    for nonce in range(255, -1, -1):
        h = hashlib.sha256(owner + _TOK_PROG + mint + bytes([nonce]) + _ATA_PROG + b"ProgramDerivedAddress").digest()
        if not _is_on_curve(h):
            return _b58encode(h)
    return None


def _rpc(method, params):
    cfg = sources.get_source("solana")
    providers = cfg.get("rpc", [])
    if not cfg.get("enabled", True):
        raise RuntimeError("solana source disabled")

    def fetch_one(node):
        d = http_post_json(node["url"],
                           json_body={"jsonrpc": "2.0", "id": 1,
                                      "method": method, "params": params},
                           timeout=12, retries=1)
        if "error" in d:
            raise RuntimeError(f"solana rpc {method}: {d['error']}")
        return d.get("result")

    result, _ = sources.try_providers("solana_rpc", providers, fetch_one)
    return result


def fetch_solana_wallet(wallet):
    """SOL lamports + native staked lamports + SPL/Token-2022 accounts.

    Returns {wallet, address, type, sol_lamports, staked_lamports,
             tokens: [{mint, amount, decimals}]}
    """
    address = wallet["address"]
    try:
        bal = _rpc("getBalance", [address]) or {}
        sol_lamports = bal.get("value", 0)
    except Exception:  # noqa: BLE001
        sol_lamports = 0
    staked_lamports = 0

    accounts = {}
    # Batch-query ATAs via getMultipleAccounts in chunks of 7 (publicnode caps batch size)
    try:
        mint_list = list(KNOWN_SPL_MINTS.keys())
        atas = [_find_ata(address, m) for m in mint_list]
        for i in range(0, len(mint_list), 7):
            chunk_mints = mint_list[i:i + 7]
            chunk_atas = atas[i:i + 7]
            res = _rpc("getMultipleAccounts", [chunk_atas, {"encoding": "jsonParsed"}]) or {}
            for mint, val in zip(chunk_mints, res.get("value") or []):
                if not val:
                    continue
                parsed = ((val.get("data") or {}).get("parsed") or {}).get("info") or {}
                tok_amt = parsed.get("tokenAmount") or {}
                amount = float(tok_amt.get("uiAmount") or 0.0)
                decimals = int(tok_amt.get("decimals") or 0)
                if amount > 0:
                    sym, name = KNOWN_SPL_MINTS.get(mint, ("", ""))
                    accounts[mint] = {"mint": mint, "amount": amount, "decimals": decimals, "symbol": sym, "name": name}
    except Exception:  # noqa: BLE001
        pass

    return {
        "wallet": wallet["name"],
        "address": address,
        "type": "sol",
        "sol_lamports": sol_lamports,
        "staked_lamports": staked_lamports,
        "tokens": sorted(accounts.values(), key=lambda t: -t["amount"]),
    }


STAKE_PROGRAM = "Stake11111111111111111111111111111111111111"


def fetch_native_staked_sol(address):
    """Native delegated stake (lamports) for a wallet.

    Uses getProgramAccounts on the Stake program with memcmp filters on the
    staker (offset 12) and withdrawer (offset 44) fields — this also finds
    stake accounts that getParsedStakeAccounts misses (e.g. accounts whose
    authority is the wallet but that were created via staking tools/pools).
    Returns active+activating delegated lamports; 0 when none.
    """
    total = 0
    seen = set()
    for offset in (12, 44):  # staker / withdrawer
        try:
            res = _rpc("getProgramAccounts", [
                STAKE_PROGRAM,
                {"encoding": "jsonParsed",
                 "filters": [{"memcmp": {"offset": offset, "bytes": address}}]},
            ]) or []
        except Exception:  # noqa: BLE001
            continue
        for item in res:
            pubkey = item.get("pubkey")
            if pubkey in seen:
                continue
            seen.add(pubkey)
            parsed = (item.get("account", {}).get("data", {}) or {}).get("parsed")
            if not parsed:
                continue
            if parsed.get("type") != "delegated":
                continue  # initialized / uninitialized accounts carry no stake
            info = parsed.get("info", {})
            delegation = (info.get("stake", {}) or {}).get("delegation", {}) or {}
            amount = int(delegation.get("stake") or 0)
            if amount > 0:
                total += amount
    return total


def birdeye_wallet_prices(address, api_key=None):
    """Optional: Birdeye wallet-token prices (free tier API key).

    Returns ({mint: usd_price}, sol_usd or None), or (None, None) when
    disabled / failed. Enable via portfolio_sources.json -> solana -> birdeye
    (or set BIRDEYE_API_KEY env var).
    """
    cfg = (sources.get_source("solana") or {}).get("birdeye") or {}
    api_key = api_key or cfg.get("key")
    if not cfg.get("enabled", False) or not api_key:
        return None, None
    try:
        d = http_get_json(cfg.get("url", "https://public-api.birdeye.so/defi/wallet_tokens"),
                          params={"wallet": address},
                          headers={"X-API-KEY": api_key, "x-chain": "solana"},
                          timeout=10, retries=0)
        data = (d or {}).get("data") or {}
        prices = {}
        sol_usd = None
        for it in data.get("items") or []:
            mint = (it or {}).get("address")
            px = (it or {}).get("priceUsd")
            if mint and px and float(px) > 0:
                prices[mint] = float(px)
                if mint == WSOL:
                    sol_usd = float(px)
        return prices, sol_usd
    except Exception:  # noqa: BLE001
        return None, None


def get_sol_token_prices(mints, budget=8.0):
    """mint -> usd price via fast exchange tickers (OKX/Binance) + configured providers."""
    t0 = time.time()
    mints = [m for m in set(mints) if m]
    out = {}

    # 1. Fast path: stablecoins + OKX Spot & Binance Futures tickers for known SPL tokens
    sym_to_mints = {}
    for m in mints:
        sym, _ = KNOWN_SPL_MINTS.get(m, ("", ""))
        if sym in ("USDC", "USDT"):
            out[m] = 1.0
        elif sym:
            sym_to_mints.setdefault(sym, []).append(m)

    if sym_to_mints:
        try:
            d = http_get_json("https://www.okx.com/api/v5/market/tickers",
                              params={"instType": "SPOT"}, timeout=5, retries=0)
            for row in (d or {}).get("data") or []:
                inst = str(row.get("instId") or "")
                if inst.endswith("-USDT") and row.get("last"):
                    sym = inst[:-5]
                    if sym in sym_to_mints:
                        for m in sym_to_mints[sym]:
                            out[m] = float(row["last"])
        except Exception:  # noqa: BLE001
            pass

        missing_syms = [s for s in sym_to_mints if any(m not in out for m in sym_to_mints[s])]
        if missing_syms:
            try:
                d = http_get_json("https://fapi.binance.com/fapi/v1/ticker/price", timeout=5, retries=0)
                for row in d or []:
                    s_raw = str(row.get("symbol") or "")
                    if s_raw.endswith("USDT") and row.get("price"):
                        sym = s_raw[:-4]
                        if sym in sym_to_mints:
                            for m in sym_to_mints[sym]:
                                out.setdefault(m, float(row["price"]))
            except Exception:  # noqa: BLE001
                pass

    cfg = sources.get_source("solana")
    providers = (cfg.get("spl_prices") or {}).get("providers", [])

    for provider in sources.provider_order("solana_spl", providers):
        name = provider.get("name")
        missing = [m for m in mints if m not in out]
        if not missing or time.time() - t0 > budget:
            break
        if name == "dexscreener":
            _fill_dexscreener(provider, missing, out)
        elif name == "coingecko":
            for m in missing:
                if time.time() - t0 > budget:
                    break
                try:
                    headers = {}
                    if provider.get("key"):
                        headers["x-cg-demo-api-key"] = provider["key"]
                    d = http_get_json(provider["url"], headers=headers,
                                      params={"contract_addresses": m,
                                              "vs_currencies": "usd"},
                                      timeout=8, retries=0)
                    v = (d.get(m) or {}).get("usd")
                    if v:
                        out[m] = float(v)
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.2)
    return out


def _fill_dexscreener(provider, mints, out):
    for i in range(0, len(mints), 30):
        chunk = mints[i:i + 30]
        try:
            d = http_get_json(provider["url"].format(mints=",".join(chunk)),
                              timeout=4, retries=0)
            best = {}  # mint -> (liquidity, priceUsd)
            for p in (d.get("pairs") or []):
                if p.get("chainId") != "solana":
                    continue
                bt = p.get("baseToken") or {}
                mint = bt.get("address")
                if not mint:
                    continue
                price = p.get("priceUsd")
                if price is None:
                    continue
                liq = (p.get("liquidity") or {}).get("usd") or 0
                if mint in best:
                    if liq > best[mint][0]:
                        best[mint] = (liq, float(price))
                else:
                    best[mint] = (liq, float(price))
            for mint, (_, price) in best.items():
                if price > 0:
                    out[mint] = price
        except Exception:  # noqa: BLE001
            pass
