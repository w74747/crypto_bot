"""
utils/logger.py
===============
نظام تسجيل الأحداث - يكتب السجلات للملف والشاشة معاً
"""

import logging
import os
from datetime import datetime

def setup_logger(name: str = "CryptoBot") -> logging.Logger:
    """
    ينشئ logger منسق يكتب في الملف وفي الكونسول
    
    Args:
        name: اسم الـ logger
    
    Returns:
        logging.Logger جاهز للاستخدام
    """
    # إنشاء مجلد السجلات إن لم يكن موجوداً
    os.makedirs("logs", exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # تجنب إضافة handlers مكررة
    if logger.handlers:
        return logger
    
    # تنسيق الرسالة
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Handler للملف
    file_handler = logging.FileHandler("logs/bot.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    # Handler للكونسول (الشاشة)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# إنشاء logger عام يمكن استيراده من أي مكان
logger = setup_logger()
