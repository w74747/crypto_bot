"""
utils/github_checker.py
========================
فلتر نشاط GitHub الذكي
الأولوية: قائمة يدوية → Grok X Search → Gemini → GitHub API
مع Cache ذكي 24 ساعة
"""

import re
import time
import requests
import os
from datetime import datetime, timezone, timedelta
from utils.logger import logger

GITHUB_TOKEN          = os.getenv("GITHUB_TOKEN", "")
GROK_API_KEY          = os.getenv("GROK_API_KEY", "")
GEMINI_API_KEY        = os.getenv("GEMINI_API_KEY", "")
GITHUB_MAX_INACTIVE_DAYS = int(os.getenv("GITHUB_MAX_INACTIVE_DAYS", "90"))

# ==========================================
# القائمة اليدوية — أولوية قصوى
# ==========================================
COIN_GITHUB_MAP = {
    "BTC":   "bitcoin/bitcoin",
    "ETH":   "ethereum/go-ethereum",
    "BNB":   "bnb-chain/bsc",
    "SOL":   "anza-xyz/agave",
    "ADA":   "input-output-hk/cardano-node",
    "DOT":   "paritytech/polkadot",
    "AVAX":  "ava-labs/avalanchego",
    "ATOM":  "cosmos/cosmos-sdk",
    "NEAR":  "near/nearcore",
    "FTM":   "Fantom-foundation/go-opera",
    "ALGO":  "algorand/go-algorand",
    "XRP":   "XRPLF/rippled",
    "XLM":   "stellar/stellar-core",
    "TRX":   "tronprotocol/java-tron",
    "LTC":   "litecoin-project/litecoin",
    "BCH":   "bitcoin-cash-node/bitcoin-cash-node",
    "ETC":   "etclabscore/core-geth",
    "ZEC":   "zcash/zcash",
    "DASH":  "dashpay/dash",
    "XMR":   "monero-project/monero",
    "MATIC": "maticnetwork/bor",
    "POL":   "maticnetwork/bor",
    "OP":    "ethereum-optimism/optimism",
    "ARB":   "OffchainLabs/arbitrum-one",
    "IMX":   "immutable/imx-core-sdk",
    "STRK":  "starkware-libs/cairo",
    "MANTA": "Manta-Network/Manta",
    "SCROLL":"scroll-tech/scroll",
    "UNI":   "Uniswap/v3-core",
    "LINK":  "smartcontractkit/chainlink",
    "AAVE":  "aave/aave-v3-core",
    "MKR":   "makerdao/dss",
    "SNX":   "Synthetixio/synthetix",
    "COMP":  "compound-finance/compound-protocol",
    "SUSHI": "sushiswap/sushiswap",
    "GMX":   "gmx-io/gmx-contracts",
    "DYDX":  "dydxprotocol/v4-chain",
    "LDO":   "lidofinance/lido-dao",
    "RPL":   "rocket-pool/rocketpool",
    "YFI":   "yearn/yearn-vaults",
    "FIL":   "filecoin-project/lotus",
    "AR":    "ArweaveTeam/arweave",
    "GRT":   "graphprotocol/graph-node",
    "OCEAN": "oceanprotocol/ocean.py",
    "LPT":   "livepeer/go-livepeer",
    "HNT":   "helium/helium-program-library",
    "WLD":   "worldcoin/world-id-contracts",
    "AXS":   "axieinfinity/ronin",
    "SAND":  "thesandboxgame/sandbox-smart-contracts",
    "MANA":  "decentraland/marketplace",
    "FET":   "fetchai/fetchd",
    "TAO":   "opentensor/bittensor",
    "RNDR":  "rendernetwork/foundation-contract",
    "SCRT":  "scrtlabs/SecretNetwork",
    "ROSE":  "oasisprotocol/oasis-core",
    "EGLD":  "multiversx/mx-chain-go",
    "VET":   "vechain/thor",
    "HBAR":  "hashgraph/hedera-services",
    "KAVA":  "Kava-Labs/kava",
    "CELO":  "celo-org/celo-monorepo",
    "XTZ":   "tezos/tezos",
    "FLOW":  "onflow/flow-go",
    "THETA": "thetatoken/theta-protocol-ledger",
    "NEO":   "neo-project/neo",
    "ZIL":   "Zilliqa/Zilliqa",
    "DOGE":  "dogecoin/dogecoin",
    "SUI":   "MystenLabs/sui",
    "APT":   "aptos-labs/aptos-core",
    "SEI":   "sei-protocol/sei-chain",
    "INJ":   "InjectiveLabs/injective-core",
    "TIA":   "celestiaorg/celestia-node",
    "JUP":   "jup-ag/jupiter-core",
    "W":     "wormhole-foundation/wormhole",
    "ENA":   "ethena-labs/ethena-core",
    "EIGEN": "Layr-Labs/eigenlayer-contracts",
    "ZRO":   "LayerZero-Labs/LayerZero",
    "ZETA":  "zeta-chain/node",
}

# Cache: symbol → (repo أو None, is_active, timestamp)
_cache: dict[str, tuple[str | None, bool, float]] = {}
CACHE_TTL = 24 * 3600  # 24 ساعة


# ==========================================
# استخراج رابط GitHub من النص
# ==========================================
def _extract_github_repo(text: str) -> str | None:
    """يستخرج owner/repo من أي نص يحتوي رابط GitHub"""
    patterns = [
        r'github\.com/([a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+)',
        r'([a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+)\s+(?:repo|repository|github)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            repo = match.group(1).rstrip('/')
            # تنظيف: احذف .git من النهاية
            repo = repo.replace('.git', '')
            # تجاهل روابط غير منطقية
            if len(repo.split('/')) == 2 and '.' not in repo.split('/')[-1]:
                return repo
    return None


# ==========================================
# البحث عبر Grok X Search
# ==========================================
def _find_github_via_grok(symbol: str) -> str | None:
    """يبحث على X عن الحساب الرسمي ورابط GitHub"""
    if not GROK_API_KEY:
        return None

    prompt = (
        f"Find the official GitHub repository for the cryptocurrency {symbol}. "
        f"Search X/Twitter for the official {symbol} crypto project account "
        f"and their GitHub link. Return ONLY the GitHub URL in format: "
        f"https://github.com/owner/repo — nothing else. "
        f"If not found, return: NOT_FOUND"
    )

    try:
        r = requests.post(
            "https://api.x.ai/v1/messages",
            headers={
                "x-api-key":         GROK_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "grok-3-fast",
                "max_tokens": 100,
                "tools":      [{"type": "x_search"}],
                "tool_choice": "auto",
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=20,
        )
        r.raise_for_status()
        content = r.json().get("content", [])
        text    = " ".join(c.get("text","") for c in content if c.get("type") == "text")

        if "NOT_FOUND" in text:
            return None

        repo = _extract_github_repo(text)
        if repo:
            logger.info(f"[Grok] {symbol} → {repo}")
        return repo

    except Exception as e:
        logger.debug(f"[Grok GitHub] خطأ لـ {symbol}: {e}")
        return None


# ==========================================
# التحقق عبر Gemini
# ==========================================
def _verify_github_via_gemini(symbol: str, repo: str | None) -> str | None:
    """
    إذا وجد Grok رابطاً: Gemini يتحقق منه
    إذا لم يجد Grok: Gemini يبحث بنفسه
    """
    if not GEMINI_API_KEY:
        return repo  # إذا لا Gemini، نقبل نتيجة Grok كما هي

    if repo:
        prompt = (
            f"Is 'https://github.com/{repo}' the official GitHub repository "
            f"for the {symbol} cryptocurrency project? "
            f"Answer with ONLY: YES or NO"
        )
    else:
        prompt = (
            f"What is the official GitHub repository URL for the {symbol} "
            f"cryptocurrency? Return ONLY the URL like: https://github.com/owner/repo "
            f"If unknown, return: NOT_FOUND"
        )

    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 80, "temperature": 0.1},
            },
            timeout=15,
        )
        r.raise_for_status()
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()

        if repo:
            # تحقق من صحة الرابط
            if "YES" in text.upper():
                logger.info(f"[Gemini] ✅ تأكيد {symbol} → {repo}")
                return repo
            else:
                logger.info(f"[Gemini] ❌ رفض {repo} لـ {symbol}")
                return None
        else:
            # Gemini يبحث بنفسه
            if "NOT_FOUND" in text:
                return None
            found = _extract_github_repo(text)
            if found:
                logger.info(f"[Gemini] {symbol} → {found}")
            return found

    except Exception as e:
        logger.debug(f"[Gemini GitHub] خطأ لـ {symbol}: {e}")
        return repo  # في حالة الخطأ، نقبل نتيجة Grok


# ==========================================
# GitHub Search API — الاحتياطي
# ==========================================
def _find_github_via_api(symbol: str) -> str | None:
    """البحث المباشر في GitHub Search — آخر خيار"""
    if not GITHUB_TOKEN:
        return None

    query = f"{symbol} cryptocurrency blockchain in:name"
    url   = f"https://api.github.com/search/repositories?q={query}&sort=stars&order=desc&per_page=3"

    try:
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {GITHUB_TOKEN}",
        }
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None

        items = r.json().get("items", [])
        for item in items:
            if item.get("stargazers_count", 0) >= 200 and not item.get("archived"):
                repo = item["full_name"]
                logger.info(f"[GitHub API] {symbol} → {repo} ({item['stargazers_count']}⭐)")
                return repo
        return None
    except Exception:
        return None


# ==========================================
# التحقق من نشاط الـ Repo
# ==========================================
def _check_repo_activity(repo: str) -> bool:
    """يتحقق من تاريخ آخر commit"""
    url = f"https://api.github.com/repos/{repo}/commits?per_page=1"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    try:
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 403:
            return True  # Rate limit — نعتبره نشطاً
        if r.status_code != 200:
            return True

        commits = r.json()
        if not commits:
            return False

        last_date = datetime.fromisoformat(
            commits[0]["commit"]["committer"]["date"].replace("Z", "+00:00")
        )
        cutoff   = datetime.now(timezone.utc) - timedelta(days=GITHUB_MAX_INACTIVE_DAYS)
        days_ago = (datetime.now(timezone.utc) - last_date).days
        active   = last_date > cutoff

        logger.info(
            f"[GitHub] {repo}: "
            f"{'✅ نشط' if active else f'❌ غير نشط ({days_ago} يوم)'}"
        )
        return active

    except Exception:
        return True  # في الشك، لا نستبعد


# ==========================================
# الدالة الرئيسية
# ==========================================
def is_github_active(coin_symbol: str) -> bool:
    """
    يتحقق من نشاط العملة على GitHub
    الترتيب:
    1. Cache (24 ساعة)
    2. القائمة اليدوية
    3. Grok X Search → Gemini يتحقق
    4. GitHub Search API
    5. إذا لم يجد شيئاً → يعتبرها نشطة
    """
    symbol = coin_symbol.upper().replace("/USDT","").replace("USDT","").strip()

    # 1. Cache
    if symbol in _cache:
        repo, active, ts = _cache[symbol]
        if time.time() - ts < CACHE_TTL:
            logger.debug(f"[Cache] {symbol}: {'✅' if active else '❌'}")
            return active

    # 2. القائمة اليدوية
    if symbol in COIN_GITHUB_MAP:
        repo      = COIN_GITHUB_MAP[symbol]
        is_active = _check_repo_activity(repo)
        _cache[symbol] = (repo, is_active, time.time())
        return is_active

    # 3. Grok يبحث → Gemini يتحقق
    logger.info(f"[GitHub AI] {symbol}: البحث الذكي...")
    grok_repo     = _find_github_via_grok(symbol)
    verified_repo = _verify_github_via_gemini(symbol, grok_repo)
    time.sleep(0.3)

    if verified_repo:
        is_active = _check_repo_activity(verified_repo)
        _cache[symbol] = (verified_repo, is_active, time.time())
        return is_active

    # 4. GitHub Search API كاحتياطي
    api_repo = _find_github_via_api(symbol)
    if api_repo:
        is_active = _check_repo_activity(api_repo)
        _cache[symbol] = (api_repo, is_active, time.time())
        return is_active

    # 5. لم يجد شيئاً — يعتبرها نشطة (لا نريد استبعاد بدون دليل)
    logger.info(f"[GitHub AI] {symbol}: لم يُعثر على repo — يُعتبر نشطاً")
    _cache[symbol] = (None, True, time.time())
    return True
