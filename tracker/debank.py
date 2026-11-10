# -*- coding: utf-8 -*-
"""EVM wallet fetcher via DeBank (multi-chain).

Two providers are configured in portfolio_sources.json -> "debank" -> "providers":
  - debank-pro    (type "pro",    AccessKey header, pro-openapi.debank.com, PAID)
  - debank-public (type "public", keyless,          api.debank.com,         FREE, rate-limited)

They are tried in order with automatic failover: if the paid provider fails
(e.g. invalid key / quota), the free public API is used. The public API is
heavily rate-limited, so it fetches chains sequentially with spacing + retries.
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from . import config, sources
from .api import http_get_json
from .blacklist import filter_rows

_chain_cache = None
_chain_lock = threading.Lock()


def _chain_list_url():
    cfg = sources.get_source("debank")
    return cfg.get("chain_list_url") or "https://api.debank.com/chain/list"


def get_chains(force=False):
    """Return the full DeBank chain list (id + name), cached."""
    global _chain_cache
    with _chain_lock:
        if _chain_cache is None or force:
            data = http_get_json(_chain_list_url(), timeout=30)
            _chain_cache = data.get("data", {}).get("chains", [])
        return list(_chain_cache)


def chain_ids():
    return [c["id"] for c in get_chains()]


def chain_names():
    return {c["id"]: c.get("name", c["id"]) for c in get_chains()}


# --------------------------------------------------------------------------
# per-provider chain fetch
# --------------------------------------------------------------------------

def _fetch_pro_chain(provider, address, chain_id, session):
    headers = {"AccessKey": provider.get("key"), "accept": "application/json"}
    return http_get_json(
        f"{provider['base_url']}/v1/user/token_list",
        headers=headers,
        params={"id": address, "chain_id": chain_id, "is_all": "true"},
        session=session,
    )


def _fetch_public_chain(provider, address, chain_id):
    # free public API: fail fast if DeBank blocks keyless requests with HTTP 429/401/403
    return http_get_json(
        f"{provider['base_url']}/token/balance_list",
        params={"user": address, "chain_id": chain_id, "is_all": "true"},
        timeout=5, retries=0,
    )


PUBLIC_EVM_RPCS = (
    ("eth", "ETH", "Ethereum", "https://ethereum-rpc.publicnode.com", "ethereum"),
    ("arb", "ETH", "Arbitrum ETH", "https://arbitrum-one-rpc.publicnode.com", "ethereum"),
    ("op", "ETH", "Optimism ETH", "https://optimism-rpc.publicnode.com", "ethereum"),
    ("base", "ETH", "Base ETH", "https://base-rpc.publicnode.com", "ethereum"),
    ("bsc", "BNB", "BNB", "https://bsc-rpc.publicnode.com", "bnb"),
    ("matic", "POL", "Polygon POL", "https://polygon-bor-rpc.publicnode.com", "matic"),
)


def _fetch_public_rpc_fallback(wallet):
    """Fast keyless multi-chain RPC fallback when DeBank Pro has no key and DeBank Public is 429-blocked."""
    from .prices import get_native_prices
    address = wallet["address"]
    native = get_native_prices()
    eth_price = native.get("ethereum") or 0.0
    # Fetch BNB/POL spot price from Binance if needed
    extra_prices = {"ethereum": eth_price, "bnb": 0.0, "matic": 0.0}
    try:
        d = http_get_json("https://api.binance.com/api/v3/ticker/price",
                          params={"symbols": '["BNBUSDT","POLUSDT"]'}, timeout=5, retries=0)
        for item in d or []:
            if item.get("symbol") == "BNBUSDT":
                extra_prices["bnb"] = float(item["price"])
            elif item.get("symbol") == "POLUSDT":
                extra_prices["matic"] = float(item["price"])
    except Exception:  # noqa: BLE001
        pass

    rows = []
    chain_totals = {}

    def probe_chain(entry):
        cid, sym, name, url, pkey = entry
        try:
            r = requests.post(
                url,
                json={"jsonrpc": "2.0", "id": 1, "method": "eth_getBalance", "params": [address, "latest"]},
                timeout=5,
            ).json()
            wei = int(r.get("result") or "0x0", 16)
            if wei <= 0:
                return None
            amount = wei / 1e18
            price = extra_prices.get(pkey) or 0.0
            usd = round(amount * price, 6)
            return {
                "wallet": wallet["name"],
                "chain": cid,
                "symbol": sym,
                "name": name,
                "amount": amount,
                "price": price,
                "usd": usd,
                "logo": "",
                "token_id": cid,
                "protocol_id": "",
                "is_verified": True,
            }
        except Exception:  # noqa: BLE001
            return None

    with ThreadPoolExecutor(max_workers=len(PUBLIC_EVM_RPCS)) as ex:
        for row in ex.map(probe_chain, PUBLIC_EVM_RPCS):
            if row:
                rows.append(row)
                chain_totals[row["chain"]] = round(chain_totals.get(row["chain"], 0.0) + row["usd"], 2)

    return {
        "wallet": wallet["name"],
        "address": address,
        "type": "evm",
        "total_usd": round(sum(chain_totals.values()), 2),
        "chain_totals": chain_totals,
        "tokens": rows,
        "chain_errors": [],
        "provider_used": "publicnode-rpc",
    }


def _fetch_with_provider(provider, wallet, chains, max_workers):
    """Fetch all chains for one wallet with a given provider."""
    address = wallet["address"]
    ptype = provider.get("type", "pro")
    if ptype == "pro" and not (provider.get("key") or "").strip():
        raise RuntimeError("debank-pro: no AccessKey configured")
    results = {}
    errors = []
    if ptype == "public":
        # Probe first chain; if keyless public endpoint is rate-limited (429), fail fast immediately
        consecutive_fails = 0
        for cid in chains:
            try:
                results[cid] = _fetch_public_chain(provider, address, cid)
                consecutive_fails = 0
            except Exception as e:  # noqa: BLE001
                errors.append(cid)
                consecutive_fails += 1
                if consecutive_fails >= 2 and not results:
                    raise RuntimeError(f"debank-public rate-limited: {e}") from e
            time.sleep(0.2)
    else:
        workers = max_workers or config.MAX_EVM_WORKERS
        sessions = [requests.Session() for _ in range(workers)]

        def work(idx_chain):
            idx, cid = idx_chain
            try:
                tokens = _fetch_pro_chain(provider, address, cid, sessions[idx % workers])
                return cid, tokens
            except Exception as e:  # noqa: BLE001
                return cid, {"__error__": f"{type(e).__name__}: {e}"}

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(work, (i, c)) for i, c in enumerate(chains)]
            for f in as_completed(futs):
                cid, tokens = f.result()
                results[cid] = tokens

    # A provider that failed on EVERY chain is a provider-level failure, not an
    # empty wallet. Returning an empty result here used to make try_providers()
    # record the dead provider as a success: the public fallback never ran, and
    # the wallet was reported as fetched with a $0 balance instead of an error.
    ok_chains = [c for c, t in results.items()
                 if not (isinstance(t, dict) and t.get("__error__"))]
    if chains and not ok_chains:
        first = next((t.get("__error__") for t in results.values()
                      if isinstance(t, dict) and t.get("__error__")), None)
        raise RuntimeError(
            "provider %r failed on all %d chains%s"
            % (provider.get("name"), len(chains), ": " + first if first else ""))

    rows = []
    for cid, tokens in results.items():
        if isinstance(tokens, dict) and tokens.get("__error__"):
            errors.append(cid)
            continue
        for t in tokens or []:
            amount = t.get("amount") or 0
            price = t.get("price") or 0
            usd = amount * price
            symbol = (t.get("symbol") or t.get("optimized_symbol")
                      or t.get("display_symbol") or t.get("name") or "?")
            rows.append({
                "wallet": wallet["name"],
                "chain": cid,
                "symbol": symbol,
                "name": t.get("name") or "",
                "amount": amount,
                "price": price,
                "usd": round(usd, 6),
                "logo": t.get("logo_url") or "",
                "token_id": t.get("id") or "",
                "protocol_id": t.get("protocol_id") or "",
                "is_verified": bool(t.get("is_verified")),
            })

    # exclude blacklisted (phishing/fake) tokens, then recompute per-chain totals
    rows = filter_rows(rows)
    chain_totals = {}
    for r in rows:
        chain_totals[r["chain"]] = round(chain_totals.get(r["chain"], 0.0) + r["usd"], 2)

    return {
        "wallet": wallet["name"],
        "address": address,
        "type": "evm",
        "total_usd": round(sum(chain_totals.values()), 2),
        "chain_totals": chain_totals,
        "tokens": rows,
        "chain_errors": errors,
    }


def fetch_evm_wallet(wallet, api_key=None, chains=None, max_workers=None):
    """Fetch all token holdings of one EVM wallet across every DeBank chain,
    trying configured providers in order (paid pro first, free public fallback)."""
    cfg = sources.get_source("debank")
    if not cfg.get("enabled", True):
        return {"wallet": wallet["name"], "address": wallet["address"], "type": "evm",
                "total_usd": 0.0, "chain_totals": {}, "tokens": [], "chain_errors": []}
    providers = [p for p in (cfg.get("providers") or [])
                 if p.get("enabled", True) and (p.get("type") != "pro" or (p.get("key") or "").strip())]
    if not providers:
        return _fetch_public_rpc_fallback(wallet)
    chains = chains or chain_ids()
    allowed = cfg.get("chains")
    if allowed:
        if isinstance(allowed, str):
            allowed = [c.strip() for c in allowed.split(",") if c.strip()]
        chains = [c for c in chains if c in allowed]
    max_workers = max_workers or config.MAX_EVM_WORKERS

    def fetch_one(provider):
        return _fetch_with_provider(provider, wallet, chains, max_workers)

    try:
        result, used = sources.try_providers("debank", providers, fetch_one)
        result["provider_used"] = used.get("name", "")
        return result
    except Exception:  # noqa: BLE001
        try:
            return _fetch_public_rpc_fallback(wallet)
        except Exception as e:  # noqa: BLE001
            return {"wallet": wallet["name"], "address": wallet["address"], "type": "evm",
                    "total_usd": 0.0, "chain_totals": {}, "tokens": [],
                    "chain_errors": ["all-providers"], "error": f"{type(e).__name__}: {e}"}
