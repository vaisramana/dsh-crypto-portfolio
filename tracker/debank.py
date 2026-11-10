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


PUBLIC_EVM_RPCS = {
    "eth": ("ETH", "Ethereum", "https://ethereum-rpc.publicnode.com", "ethereum"),
    "arb": ("ETH", "Arbitrum ETH", "https://arbitrum-one-rpc.publicnode.com", "ethereum"),
    "op": ("ETH", "Optimism ETH", "https://optimism-rpc.publicnode.com", "ethereum"),
    "base": ("ETH", "Base ETH", "https://base-rpc.publicnode.com", "ethereum"),
    "bsc": ("BNB", "BNB", "https://bsc-rpc.publicnode.com", "bnb"),
    "avax": ("AVAX", "Avalanche", "https://avalanche-c-chain-rpc.publicnode.com", "avax"),
    "sonic": ("S", "Sonic", "https://sonic-rpc.publicnode.com", "sonic"),
    "matic": ("POL", "Polygon POL", "https://polygon-bor-rpc.publicnode.com", "matic"),
}

# (chain_id, symbol, display_name, contract_address, decimals, price_key, protocol_id)
PUBLIC_ERC20_CONTRACTS = (
    # Ethereum Mainnet Spot & Aave V3
    ("eth", "WBTC", "Wrapped BTC", "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", 8, "bitcoin", ""),
    ("eth", "USDT", "Tether USD", "0xdAC17F958D2ee523a2206206994597C13D831ec7", 6, "usd", ""),
    ("eth", "USDC", "USD Coin", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 6, "usd", ""),
    ("eth", "WETH", "Wrapped Ether", "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", 18, "ethereum", ""),
    ("eth", "stETH", "Lido Staked ETH", "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", 18, "ethereum", "lido"),
    ("eth", "LINK", "ChainLink Token", "0x514910771AF9Ca656af840dff83E8264EcF986CA", 18, "link", ""),
    ("eth", "USDT", "Aave V3 USDT", "0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a", 6, "usd", "aave3"),
    ("eth", "WETH", "Aave V3 WETH", "0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8", 18, "ethereum", "aave3"),
    ("eth", "USDC", "Aave V3 USDC", "0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c", 6, "usd", "aave3"),
    ("eth", "WBTC", "Aave V3 WBTC", "0x5Ee5bf7ae06D1Be5997A1A72006FE6C607eC6DE8", 8, "bitcoin", "aave3"),
    ("eth", "3Crv", "Curve.fi DAI/USDC/USDT", "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490", 18, "usd", "curve"),
    # Arbitrum Spot & Aave V3
    ("arb", "ARB", "Arbitrum", "0x912CE59144191C1204E64559FE8253a0e49E6548", 18, "arb", ""),
    ("arb", "USDC", "USD Coin", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", 6, "usd", ""),
    ("arb", "USDT", "Tether USD", "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", 6, "usd", ""),
    ("arb", "WETH", "Aave V3 WETH (Arb)", "0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8", 18, "ethereum", "aave3"),
    ("arb", "USDC", "Aave V3 USDC (Arb)", "0x724dc807b04555b71ed48a6896b6F41593b8C637", 6, "usd", "aave3"),
    ("arb", "USDT", "Aave V3 USDT (Arb)", "0x6ab707Aca953eDAeFBc4fD23bA73294241490620", 6, "usd", "aave3"),
    # Polygon
    ("matic", "USDC", "USDC (Bridged)", "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", 6, "usd", ""),
    ("matic", "USDC", "USD Coin", "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359", 6, "usd", ""),
)


def _eth_call_uint256(rpc_url, contract, data):
    r = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [{"to": contract, "data": data}, "latest"]},
        timeout=5,
    ).json()
    res = r.get("result") or "0x0"
    if not res or res == "0x":
        return 0
    return int(res[:66], 16)


def _fetch_public_rpc_fallback(wallet):
    """Fast keyless multi-chain RPC fallback reading native coins, ERC-20s, Aave V3, Convex & Venus."""
    from .prices import get_native_prices
    address = wallet["address"]
    pad_addr = address[2:].lower().rjust(64, "0")
    bal_call = "0x70a08231" + pad_addr

    native = get_native_prices()
    prices = {
        "usd": 1.0,
        "bitcoin": native.get("bitcoin") or 0.0,
        "ethereum": native.get("ethereum") or 0.0,
        "bnb": 0.0,
        "avax": 0.0,
        "arb": 0.0,
        "link": 0.0,
        "sonic": 0.0,
        "matic": 0.0,
    }
    try:
        d = http_get_json("https://www.okx.com/api/v5/market/tickers",
                          params={"instType": "SPOT"}, timeout=6, retries=0)
        okx_map = {
            x["instId"][:-5]: float(x["last"])
            for x in (d or {}).get("data") or []
            if str(x.get("instId") or "").endswith("-USDT") and x.get("last")
        }
        if not prices["bitcoin"] and okx_map.get("BTC"):
            prices["bitcoin"] = okx_map["BTC"]
        if not prices["ethereum"] and okx_map.get("ETH"):
            prices["ethereum"] = okx_map["ETH"]
        prices["bnb"] = okx_map.get("BNB", 0.0)
        prices["avax"] = okx_map.get("AVAX", 0.0)
        prices["arb"] = okx_map.get("ARB", 0.0)
        prices["link"] = okx_map.get("LINK", 0.0)
        prices["sonic"] = okx_map.get("S", 0.0)
        prices["matic"] = okx_map.get("POL") or okx_map.get("MATIC") or 0.0
    except Exception:  # noqa: BLE001
        pass

    tasks = []

    # 1. Native balances
    for cid, (sym, name, url, pkey) in PUBLIC_EVM_RPCS.items():
        def make_native_task(cid=cid, sym=sym, name=name, url=url, pkey=pkey):
            def run():
                r = requests.post(
                    url,
                    json={"jsonrpc": "2.0", "id": 1, "method": "eth_getBalance", "params": [address, "latest"]},
                    timeout=5,
                ).json()
                wei = int((r.get("result") or "0x0")[:66], 16)
                if wei <= 0:
                    return None
                amount = wei / 1e18
                price = prices.get(pkey) or 0.0
                usd = round(amount * price, 6)
                if usd < 0.01:
                    return None
                return {
                    "wallet": wallet["name"], "chain": cid, "symbol": sym, "name": name,
                    "amount": amount, "price": price, "usd": usd, "logo": "",
                    "token_id": cid, "protocol_id": "", "is_verified": True,
                }
            return run
        tasks.append(make_native_task())

    # 2. ERC-20 & Aave V3 aTokens
    for entry in PUBLIC_ERC20_CONTRACTS:
        cid, sym, name, contract, dec, pkey, proto = entry
        rpc_url = PUBLIC_EVM_RPCS[cid][2]
        def make_erc20_task(cid=cid, sym=sym, name=name, contract=contract, dec=dec, pkey=pkey, proto=proto, rpc_url=rpc_url):
            def run():
                raw = _eth_call_uint256(rpc_url, contract, bal_call)
                if raw <= 0:
                    return None
                amount = raw / (10 ** dec)
                price = prices.get(pkey) or 0.0
                usd = round(amount * price, 6)
                if usd < 0.05:
                    return None
                return {
                    "wallet": wallet["name"], "chain": cid, "symbol": sym, "name": name,
                    "amount": amount, "price": price, "usd": usd, "logo": "",
                    "token_id": contract.lower(), "protocol_id": proto, "is_verified": True,
                }
            return run
        tasks.append(make_erc20_task())

    # 3. Convex steCRV (ETH + stETH LP staked on Ethereum)
    def fetch_convex_stecrv():
        eth_rpc = PUBLIC_EVM_RPCS["eth"][2]
        lp_raw = _eth_call_uint256(eth_rpc, "0x0A760466E1B4621579a82a39CB56Dda2F4E70f03", bal_call)
        if lp_raw <= 0:
            return None
        vprice_raw = _eth_call_uint256(eth_rpc, "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022", "0xbb7b8b80")
        vprice = (vprice_raw / 1e18) if vprice_raw > 0 else 1.145
        eth_equiv = (lp_raw / 1e18) * vprice
        eth_px = prices.get("ethereum") or 0.0
        usd = round(eth_equiv * eth_px, 6)
        if usd < 0.05:
            return None
        return {
            "wallet": wallet["name"], "chain": "eth", "symbol": "ETH",
            "name": "Convex · ETH+stETH", "amount": eth_equiv, "price": eth_px,
            "usd": usd, "logo": "", "token_id": "convex-stecrv",
            "protocol_id": "convex", "is_verified": True,
        }
    tasks.append(fetch_convex_stecrv)

    # 4. Venus vUSDC on BNB Chain
    def fetch_venus_vusdc():
        bsc_rpc = PUBLIC_EVM_RPCS["bsc"][2]
        vbal = _eth_call_uint256(bsc_rpc, "0xecA88125a5ADbe82614ffC12D0DB554E2e2867C8", bal_call)
        if vbal <= 0:
            return None
        vrate = _eth_call_uint256(bsc_rpc, "0xecA88125a5ADbe82614ffC12D0DB554E2e2867C8", "0x182df0f5")
        usdc_amt = (vbal * vrate) / 1e36
        if usdc_amt < 0.05:
            return None
        return {
            "wallet": wallet["name"], "chain": "bsc", "symbol": "USDC",
            "name": "Venus · USDC", "amount": usdc_amt, "price": 1.0,
            "usd": round(usdc_amt, 6), "logo": "", "token_id": "venus-vusdc",
            "protocol_id": "venus", "is_verified": True,
        }
    tasks.append(fetch_venus_vusdc)

    rows = []
    chain_totals = {}

    def safe_run(fn):
        try:
            return fn()
        except Exception:  # noqa: BLE001
            return None

    with ThreadPoolExecutor(max_workers=10) as ex:
        for row in ex.map(safe_run, tasks):
            if row:
                rows.append(row)
                chain_totals[row["chain"]] = round(chain_totals.get(row["chain"], 0.0) + row["usd"], 2)

    rows = filter_rows(rows)
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
