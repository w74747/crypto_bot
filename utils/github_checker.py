"""
utils/github_checker.py
========================
يتحقق من نشاط مشاريع العملات على GitHub
"""

import requests
from datetime import datetime, timezone, timedelta
from utils.logger import logger
from config.settings import GITHUB_TOKEN, GITHUB_MAX_INACTIVE_DAYS

# خريطة ربط رمز العملة باسم مشروعها على GitHub
# أضف عملات أكثر حسب الحاجة
COIN_GITHUB_MAP = {
    "BTC":  "bitcoin/bitcoin",
    "ETH":  "ethereum/go-ethereum",
    "BNB":  "bnb-chain/bsc",
    "SOL":  "solana-labs/solana",
    "ADA":  "input-output-hk/cardano-node",
    "DOT":  "paritytech/polkadot",
    "AVAX": "ava-labs/avalanchego",
    "MATIC":"maticnetwork/bor",
    "LINK": "smartcontractkit/chainlink",
    "UNI":  "Uniswap/v3-core",
    "ATOM": "cosmos/cosmos-sdk",
    "LTC":  "litecoin-project/litecoin",
    "NEAR": "near/nearcore",
    "FTM":  "Fantom-foundation/go-opera",
    "ALGO": "algorand/go-algorand",
    "XRP":  "XRPLF/rippled",
    "DOGE": "dogecoin/dogecoin",
    "SHIB": "shib-token/shib",
    "TRX":  "tronprotocol/java-tron",
    "XLM":  "stellar/stellar-core",
}

def get_github_headers() -> dict:
    """يُعيد headers الطلب مع التوكن إن وُجد"""
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    return headers

def is_github_active(coin_symbol: str) -> bool:
    """
    يتحقق إن كان المشروع نشطاً على GitHub
    
    Args:
        coin_symbol: رمز العملة مثل "BTC" أو "ETH"
    
    Returns:
        True إذا كان المشروع نشطاً أو غير موجود في الخريطة
        False إذا كان غير نشط (لم يُحدَّث منذ 3 أشهر)
    """
    # نظّف الرمز (أزل USDT أو /USDT)
    symbol = coin_symbol.upper().replace("/USDT", "").replace("USDT", "")
    
    # إذا لم يكن في الخريطة، نفترض أنه نشط (لا نريد استبعاده بسبب جهلنا)
    if symbol not in COIN_GITHUB_MAP:
        logger.info(f"[GitHub] {symbol}: غير موجود في الخريطة، يُعتبر نشطاً")
        return True
    
    repo = COIN_GITHUB_MAP[symbol]
    url  = f"https://api.github.com/repos/{repo}/commits?per_page=1"
    
    try:
        response = requests.get(url, headers=get_github_headers(), timeout=10)
        
        # إذا تجاوزنا حد API، نعتبر العملة نشطة
        if response.status_code == 403:
            logger.warning(f"[GitHub] تجاوز حد الطلبات - {symbol} يُعتبر نشطاً")
            return True
        
        if response.status_code != 200:
            logger.warning(f"[GitHub] خطأ {response.status_code} لـ {symbol}")
            return True
        
        commits = response.json()
        if not commits:
            logger.warning(f"[GitHub] لا توجد commits لـ {symbol}")
            return False
        
        # تاريخ آخر commit
        last_commit_date_str = commits[0]["commit"]["committer"]["date"]
        last_commit_date = datetime.fromisoformat(
            last_commit_date_str.replace("Z", "+00:00")
        )
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=GITHUB_MAX_INACTIVE_DAYS)
        is_active   = last_commit_date > cutoff_date
        days_since  = (datetime.now(timezone.utc) - last_commit_date).days
        
        status = "✅ نشط" if is_active else "❌ غير نشط"
        logger.info(f"[GitHub] {symbol}: آخر تحديث منذ {days_since} يوم - {status}")
        
        return is_active
        
    except requests.exceptions.Timeout:
        logger.warning(f"[GitHub] انتهت مهلة الاتصال لـ {symbol} - يُعتبر نشطاً")
        return True
    except Exception as e:
        logger.error(f"[GitHub] خطأ غير متوقع لـ {symbol}: {e}")
        return True  # في حالة الشك، لا نستبعد
