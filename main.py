"""
main.py
========
نقطة الدخول الرئيسية - يشغّل البوت بالكامل

كيفية التشغيل:
    python main.py

ما يفعله:
1. يتحقق من الإعدادات
2. يشغل Scanner في الخلفية (كل ساعة)
3. يشغل Telegram Bot للاستماع للأزرار
"""

import asyncio
import time
import threading
from datetime import datetime

from config.settings import validate_config, SCAN_INTERVAL_MINUTES
from core.scanner import MarketScanner
from core.telegram_bot import (
    TelegramNotifier,
    register_opportunity,
    build_application,
)
from utils.logger import logger


# ==========================================
# حلقة المسح في الخلفية
# ==========================================
async def scanner_loop(notifier: TelegramNotifier):
    """
    يعمل في الخلفية ويفحص السوق كل SCAN_INTERVAL_MINUTES
    """
    scanner = MarketScanner()
    interval_seconds = SCAN_INTERVAL_MINUTES * 60
    
    await notifier.send_plain_message(
        f"🤖 البوت يعمل الآن!\n"
        f"⏰ سيفحص السوق كل {SCAN_INTERVAL_MINUTES} دقيقة\n"
        f"🕐 الفحص الأول: الآن"
    )
    
    while True:
        scan_start = datetime.now()
        logger.info(f"\n{'='*50}")
        logger.info(f"🔄 دورة مسح جديدة: {scan_start.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*50}")
        
        try:
            opportunities = scanner.scan_market()
            
            if not opportunities:
                logger.info("😐 لم يُعثر على فرص في هذه الدورة")
                await notifier.send_plain_message(
                    f"🔍 اكتمل الفحص - لا توجد فرص حالياً\n"
                    f"⏰ {scan_start.strftime('%H:%M')} | "
                    f"الفحص القادم بعد {SCAN_INTERVAL_MINUTES} دقيقة"
                )
            else:
                # إرسال كل فرصة مع أزرار التنفيذ
                for opp in opportunities:
                    register_opportunity(opp)          # حفظ للتنفيذ اللاحق
                    await notifier.send_opportunity(opp)  # إرسال لـ Telegram
                    await asyncio.sleep(1)  # فاصل بين الرسائل
        
        except Exception as e:
            logger.error(f"❌ خطأ في دورة المسح: {e}", exc_info=True)
            await notifier.send_plain_message(f"⚠️ خطأ في المسح: {str(e)[:100]}")
        
        # الانتظار حتى الدورة القادمة
        logger.info(f"⏳ الانتظار {SCAN_INTERVAL_MINUTES} دقيقة حتى الدورة القادمة...")
        await asyncio.sleep(interval_seconds)


# ==========================================
# الدالة الرئيسية
# ==========================================
async def main():
    """يشغّل المسح وبوت Telegram معاً"""
    
    # 1. التحقق من الإعدادات
    print("\n" + "="*50)
    print("🤖 Crypto Bottom Fisher Bot")
    print("="*50)
    validate_config()
    
    # 2. إنشاء الكائنات
    notifier = TelegramNotifier()
    app      = build_application()
    
    # 3. تشغيل كلاهما معاً
    async with app:
        await app.start()
        
        # تشغيل polling للأزرار
        await app.updater.start_polling(drop_pending_updates=True)
        
        logger.info("✅ Telegram Polling يعمل")
        
        # تشغيل حلقة المسح
        await scanner_loop(notifier)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⛔ تم إيقاف البوت يدوياً")
        logger.info("البوت أُوقف بواسطة المستخدم")
    except Exception as e:
        logger.critical(f"خطأ حرج: {e}", exc_info=True)
        raise
