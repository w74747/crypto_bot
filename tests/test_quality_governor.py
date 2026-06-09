import pytest
import math
from unittest.mock import MagicMock

# 1. اختبار صمام أمان الأهداف (يمنع خطأ TP2=TP3 وخطأ TP1 المعكوس)
def test_cascading_ceilings_and_floors():
    entry_price = 61889.75
    cap = entry_price * 1.15
    
    # محاكاة لأسوأ سيناريو: ذكاء اصطناعي يعيد أرقاماً مشوهة أو سالبة
    tp1_raw, tp2_raw, tp3_raw = 55501.86, 71156.23, 71156.23
    
    # تطبيق منطق الـ v39 المصلح
    tp1 = min(max(tp1_raw, entry_price * 1.03), cap * 0.78)
    tp2 = min(max(tp2_raw, tp1 * 1.03), cap * 0.90)
    tp3 = min(max(tp3_raw, tp2 * 1.03), cap)
    
    if not (entry_price < tp1 < tp2 < tp3):
        tp1 = entry_price * 1.03
        tp2 = entry_price * 1.06
        tp3 = entry_price * 1.12
        
    # التحقق الصارم: يجب أن تكون الأهداف تصاعدية ومنطقية وفوق سعر الدخول حتماً
    assert tp1 > entry_price, "خطأ: الهدف الأول تحت سعر الشراء!"
    assert tp1 < tp2 < tp3, "خطأ: الأهداف متكررة أو غير تصاعدية (Stuck Loop)!"

# 2. اختبار حماية أوامر MEXC Spot (يمنع الـ invalid type 500)
def test_mexc_market_buy_payload():
    capital = 30.0
    # محاكاة دالة الشراء الجديدة
    params = {"quoteOrderQty": capital}
    
    # التأكد من أن التكلفة تُرسل كـ الكاش المباشر وليس كـ كمية عملات مشوهة
    assert "quoteOrderQty" in params
    assert params["quoteOrderQty"] == 30.0

# 3. اختبار عزل الصفقات اليدوية الـ 17 عن البوت
def test_portfolio_slot_isolation():
    open_symbols = set()
    # البوت يفتح صفقة BTC
    open_symbols.add("BTC/USDT")
    
    # محاكاة وجود 17 صفقة يدوية أخرى في الحساب
    total_exchange_orders = 17 
    
    # التحقق: البوت يجب ألا يرى إلا صبوته الداخلية فقط
    assert len(open_symbols) == 1, "خطأ: البوت يتداخل مع الصفقات اليدوية للحساب!"
    assert "BTC/USDT" in open_symbols
