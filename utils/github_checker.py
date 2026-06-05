"""
utils/github_checker.py
========================
فلتر نشاط GitHub - نسخة محسّنة
"""

import requests
import time
from datetime import datetime, timezone, timedelta
from utils.logger import logger
from config.settings import GITHUB_TOKEN, GITHUB_MAX_INACTIVE_DAYS


COIN_GITHUB_MAP = {
    # --- Layer 1 الكبيرة ---
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

    # --- Layer 2 و Scaling ---
    "MATIC": "maticnetwork/bor",
    "POL":   "maticnetwork/bor",
    "OP":    "ethereum-optimism/optimism",
    "ARB":   "OffchainLabs/arbitrum-one",
    "IMX":   "immutable/imx-core-sdk",
    "STRK":  "starkware-libs/cairo",
    "MANTA": "Manta-Network/Manta",
    "SCROLL":"scroll-tech/scroll",
    "METIS": "MetisProtocol/metis",

    # --- DeFi ---
    "UNI":   "Uniswap/v3-core",
    "LINK":  "smartcontractkit/chainlink",
    "AAVE":  "aave/aave-v3-core",
    "CRV":   "curvefi/curve-contract",
    "MKR":   "makerdao/dss",
    "SNX":   "Synthetixio/synthetix",
    "COMP":  "compound-finance/compound-protocol",
    "BAL":   "balancer-labs/balancer-v2-monorepo",
    "SUSHI": "sushiswap/sushiswap",
    "GMX":   "gmx-io/gmx-contracts",
    "DYDX":  "dydxprotocol/v4-chain",
    "LDO":   "lidofinance/lido-dao",
    "RPL":   "rocket-pool/rocketpool",
    "FXS":   "FraxFinance/frax-solidity",
    "CVX":   "convex-finance/convex-platform",
    "YFI":   "yearn/yearn-vaults",
    "1INCH": "1inch/1inch-v3",

    # --- Infrastructure ---
    "FIL":   "filecoin-project/lotus",
    "AR":    "ArweaveTeam/arweave",
    "GRT":   "graphprotocol/graph-node",
    "BAND":  "bandprotocol/chain",
    "OCEAN": "oceanprotocol/ocean.py",
    "LPT":   "livepeer/go-livepeer",
    "GLM":   "golemfactory/golem",
    "STORJ": "storj/storj",
    "HNT":   "helium/helium-program-library",
    "WLD":   "worldcoin/world-id-contracts",
    "ANKR":  "Ankr-network/ankr-chain",

    # --- Gaming & Metaverse ---
    "AXS":   "axieinfinity/ronin",
    "SAND":  "thesandboxgame/sandbox-smart-contracts",
    "MANA":  "decentraland/marketplace",
    "ENJ":   "enjin/enjin-java-sdk",
    "GALA":  "galachain/sdk",

    # --- AI & Data ---
    "FET":   "fetchai/fetchd",
    "AGIX":  "singnet/snet-daemon",
    "TAO":   "opentensor/bittensor",
    "RNDR":  "rendernetwork/foundation-contract",

    # --- Privacy ---
    "SCRT":  "scrtlabs/SecretNetwork",
    "ROSE":  "oasisprotocol/oasis-core",
    "NYM":   "nymtech/nym",

    # --- Interoperability ---
    "KSM":   "paritytech/polkadot",
    "IOTA":  "iotaledger/iota-core",
    "EGLD":  "multiversx/mx-chain-go",
    "ICX":   "icon-project/goloop",
    "VET":   "vechain/thor",
    "HBAR":  "hashgraph/hedera-services",
    "KAVA":  "Kava-Labs/kava",
    "CELO":  "celo-org/celo-monorepo",
    "XTZ":   "tezos/tezos",
    "FLOW":  "onflow/flow-go",
    "THETA": "thetatoken/theta-protocol-ledger",
    "WAVES": "wavesplatform/Waves",
    "NEO":   "neo-project/neo",
    "ZIL":   "Zilliqa/Zilliqa",
    "QTUM":  "qtumproject/qtum",

    # --- Meme ---
    "DOGE":  "dogecoin/dogecoin",
    "SHIB":  "shibaswap/shibaswap",

    # --- أحدث العملات على MEXC ---
    "SUI":   "MystenLabs/sui",
    "APT":   "aptos-labs/aptos-core",
    "SEI":   "sei-protocol/sei-chain",
    "INJ":   "InjectiveLabs/injective-core",
    "TIA":   "celestiaorg/celestia-node",
    "PYTH":  "pyth-network/pyth-sdk-solidity",
    "JUP":   "jup-ag/jupiter-core",
    "W":     "wormhole-foundation/wormhole",
    "ENA":   "ethena-labs/ethena-core",
    "EIGEN": "Layr-Labs/eigenlayer-contracts",
    "ZRO":   "LayerZero-Labs/LayerZero",
    "ZETA":  "zeta-chain/node",
    "IO":    "ionet-official/io-net",
}


# Cache لتجنب تكرار الطلبات — يحتفظ بالنتائج 6 ساعات
_cache: dict[str, tuple[bool, float]] = {}
CACHE_TTL_SECONDS = 6 * 3600


def _get_headers() -> dict:
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    return headers


def _check_repo_activity(repo: str) -> bool:
    url = f"https://api.github.com/repos/{repo}/commits?per_page=1"
    try:
        r = requests.get(url, headers=_get_headers(), timeout=8)
        if r.status_code == 403:
            logger.warning("[GitHub] تجاوز حد الطلبات — يُعتبر نشطاً")
            return True
        if r.status_code == 404:
            return False
        if r.status_code != 200:
            return True

        commits = r.json()
        if not commits:
            return False

        last_date_str = commits[0]["commit"]["committer"]["date"]
        last_date     = datetime.fromisoformat(last_date_str.replace("Z", "+00:00"))
        cutoff        = datetime.now(timezone.utc) - timedelta(days=GITHUB_MAX_INACTIVE_DAYS)
        days_ago      = (datetime.now(timezone.utc) - last_date).days
        is_active     = last_date > cutoff
        status        = "✅ نشط" if is_active else f"❌ غير نشط ({days_ago} يوم)"
        logger.info(f"[GitHub] {repo}: {status}")
        return is_active

    except requests.exceptions.Timeout:
        logger.warning(f"[GitHub] timeout لـ {repo} — يُعتبر نشطاً")
        return True
    except Exception as e:
        logger.error(f"[GitHub] خطأ {repo}: {e}")
        return True


def _search_github(coin_symbol: str) -> tuple[str | None, bool]:
    if not GITHUB_TOKEN:
        logger.debug(f"[GitHub Search] لا يوجد توكن — تخطي {coin_symbol}")
        return None, True

    query = f"{coin_symbol} blockchain cryptocurrency in:name,description"
    url   = f"https://api.github.com/search/repositories?q={query}&sort=stars&order=desc&per_page=3"

    try:
        r = requests.get(url, headers=_get_headers(), timeout=10)
        if r.status_code != 200:
            return None, True

        results = r.json().get("items", [])
        if not results:
            logger.info(f"[GitHub Search] لم يُعثر على مشروع لـ {coin_symbol}")
            return None, True

        for repo in results:
            stars    = repo.get("stargazers_count", 0)
            repo_name = repo.get("full_name", "")
            archived = repo.get("archived", False)

            if stars >= 100 and not archived:
                logger.info(f"[GitHub Search] {coin_symbol} → {repo_name} ({stars}⭐)")
                is_active = _check_repo_activity(repo_name)
                return repo_name, is_active

        logger.info(f"[GitHub Search] {coin_symbol}: لا نتائج كافية — يُعتبر نشطاً")
        return None, True

    except Exception as e:
        logger.error(f"[GitHub Search] خطأ لـ {coin_symbol}: {e}")
        return None, True


def is_github_active(coin_symbol: str) -> bool:
    symbol = coin_symbol.upper().replace("/USDT", "").replace("USDT", "").strip()

    # فحص الـ Cache
    if symbol in _cache:
        result, cached_at = _cache[symbol]
        if time.time() - cached_at < CACHE_TTL_SECONDS:
            logger.debug(f"[GitHub Cache] {symbol}: {'✅' if result else '❌'}")
            return result

    # البحث في القائمة اليدوية
    if symbol in COIN_GITHUB_MAP:
        repo      = COIN_GITHUB_MAP[symbol]
        is_active = _check_repo_activity(repo)
        _cache[symbol] = (is_active, time.time())
        return is_active

    # البحث التلقائي
    logger.info(f"[GitHub] {symbol}: غير موجود في القائمة — جاري البحث...")
    _, is_active = _search_github(symbol)
    time.sleep(0.5)
    _cache[symbol] = (is_active, time.time())
    return is_active
