# 🤖 دليل الإعداد الكامل — Crypto Bottom Fisher Bot

---

## 📋 الخطوة صفر: المتطلبات الأساسية

قبل أي شيء، تأكد من توفر:
- **Python 3.10 أو أحدث** — [تحميل من python.org](https://python.org)
- **حساب Bybit** مع إكمال التحقق (KYC)
- **حساب Telegram** عادي

---

## 🗂️ الخطوة 1: إنشاء هيكل المشروع

### على Windows:
افتح **Command Prompt** أو **PowerShell** وشغّل:
```
mkdir crypto_bot
cd crypto_bot
mkdir core utils config logs
```

### على Mac/Linux:
افتح **Terminal** وشغّل:
```bash
mkdir -p crypto_bot/{core,utils,config,logs}
cd crypto_bot
```

### انسخ جميع الملفات في أماكنها الصحيحة:
```
crypto_bot/
├── main.py                     ← الملف الرئيسي
├── test_setup.py               ← سكريبت الاختبار
├── requirements.txt            ← قائمة المكتبات
├── .env                        ← مفاتيح API (أنشئه من .env.example)
├── config/
│   ├── __init__.py
│   └── settings.py
├── core/
│   ├── __init__.py
│   ├── scanner.py
│   ├── executor.py
│   └── telegram_bot.py
├── utils/
│   ├── __init__.py
│   ├── logger.py
│   └── github_checker.py
└── logs/                       ← يُنشأ تلقائياً
```

---

## 📦 الخطوة 2: تثبيت المكتبات

في مجلد `crypto_bot`، شغّل:
```bash
pip install -r requirements.txt
```

إذا واجهت مشكلة في `pandas-ta`، جرب:
```bash
pip install pandas-ta --no-deps
pip install pandas numpy
```

---

## 🔑 الخطوة 3: إنشاء ملف .env

انسخ الملف:
- Windows: `copy .env.example .env`
- Mac/Linux: `cp .env.example .env`

ثم افتح `.env` بأي محرر نصوص وأكمل البيانات.

---

## 🏦 الخطوة 4: إنشاء Bybit API Keys

### 4.1 — الدخول للحساب
1. اذهب إلى [bybit.com](https://bybit.com) وادخل لحسابك
2. اضغط على أيقونة حسابك (أعلى اليمين)
3. اختر **"API Management"**

### 4.2 — إنشاء مفتاح جديد
1. اضغط **"Create New Key"**
2. اختر نوع المفتاح: **"API Transaction"** (ليس System-generated)
3. أعطه اسماً مثل: `BottomFisherBot`

### 4.3 — تحديد الصلاحيات (مهم جداً!)
فعّل **فقط** هذه الصلاحيات:
- ✅ **Read** (تحت Unified Trading)
- ✅ **Trade** (تحت Unified Trading)
- ❌ لا تفعّل Withdraw أبداً
- ❌ لا تفعّل Transfer

### 4.4 — قيود الـ IP (اختياري لكن موصى به)
- إذا كان السيرفر ثابت IP: أضف عنوان IP في خانة "IP Restriction"
- إذا كنت على IP متغير: اتركها فارغة

### 4.5 — حفظ المفاتيح
ستظهر لك:
- **API Key**: شيء مثل `xxxxxxxxxxx`
- **API Secret**: شيء مثل `xxxxxxxxxxxxxxxx`

**احفظهما فوراً** — السر يظهر مرة واحدة فقط!

### 4.6 — إضافة للـ .env
```
BYBIT_API_KEY=المفتاح_الذي_حصلت_عليه
BYBIT_API_SECRET=السر_الذي_حصلت_عليه
BYBIT_ENV=testnet
```

> ⚠️ **ابدأ دائماً بـ testnet** للاختبار. غيّر إلى mainnet فقط بعد التأكد من أن كل شيء يعمل.

---

## 💬 الخطوة 5: إنشاء Telegram Bot

### 5.1 — إنشاء البوت عبر BotFather
1. افتح Telegram وابحث عن **@BotFather**
2. أرسل: `/start`
3. أرسل: `/newbot`
4. اختر اسماً للبوت مثل: `My Crypto Alerts`
5. اختر username يجب أن ينتهي بـ `bot` مثل: `mycryptoalerts_bot`
6. ستحصل على **توكن** شكله: `1234567890:ABCdef...`

أضفه في `.env`:
```
TELEGRAM_BOT_TOKEN=التوكن_الذي_حصلت_عليه
```

### 5.2 — الحصول على Chat ID
1. ابحث عن **@userinfobot** على Telegram
2. أرسل: `/start`
3. ستحصل على رسالة بها **Id** — هذا هو CHAT_ID الخاص بك

أضفه في `.env`:
```
TELEGRAM_CHAT_ID=الرقم_الذي_حصلت_عليه
```

> ⚠️ **مهم**: أرسل رسالة واحدة لبوتك الجديد أولاً (اضغط Start) وإلا لن يتمكن من إرسال رسائل لك.

---

## 🐙 الخطوة 6: GitHub Token (اختياري)

هذا للتحقق من نشاط المشاريع على GitHub.

1. اذهب إلى [github.com/settings/tokens](https://github.com/settings/tokens)
2. اضغط **"Generate new token (classic)"**
3. اختر فقط: ✅ **public_repo**
4. اضغط **"Generate token"**
5. أضفه في `.env`:
```
GITHUB_TOKEN=التوكن_الذي_حصلت_عليه
```

إذا تركت هذا فارغاً، ستظل الفلترة تعمل لكن ستتجاهل فلتر GitHub تلقائياً.

---

## ✅ الخطوة 7: اختبار الإعداد

شغّل سكريبت الاختبار أولاً:
```bash
python test_setup.py
```

يجب أن ترى:
```
✅ جميع المكتبات موجودة
✅ ملف .env موجود
✅ جميع المتغيرات محددة
✅ متصل بـ Bybit (testnet)
✅ بوت Telegram: @اسم_بوتك
✅ رسالة تجريبية أُرسلت
```

إذا ظهر خطأ في أي خطوة، راجع السبب وأصلحه قبل المتابعة.

---

## 🚀 الخطوة 8: تشغيل البوت

```bash
python main.py
```

ستظهر رسالة على Telegram تؤكد أن البوت يعمل، وسيبدأ الفحص الأول فوراً.

---

## ⚙️ الخطوة 9: ضبط الإعدادات

افتح `config/settings.py` وعدّل حسب رغبتك:

| الإعداد | القيمة الافتراضية | الشرح |
|---------|------------------|-------|
| `MAX_DISTANCE_FROM_LOD` | 0.10 (10%) | البعد عن القاع المسموح |
| `MIN_DAILY_VOLUME_USD` | 5,000,000 | أدنى حجم تداول |
| `RSI_OVERSOLD_THRESHOLD` | 35 | حد RSI للتشبع البيعي |
| `STOP_LOSS_PCT` | 0.08 (8%) | نسبة وقف الخسارة |
| `TRADE_AMOUNT_USD` | 100 | رأس المال لكل صفقة |
| `MAX_OPEN_TRADES` | 3 | أقصى صفقات مفتوحة |
| `SCAN_INTERVAL_MINUTES` | 60 | الفاصل بين كل فحص |

---

## 🛡️ التوصيات الأمنية

1. **لا تشارك ملف `.env`** مع أحد
2. **لا ترفع المشروع** على GitHub مع ملف `.env`
3. **ابدأ برأس مال صغير** (50-100 دولار) حتى تتأكد من أن كل شيء يعمل
4. **استخدم testnet** لمدة أسبوع قبل التبديل للـ mainnet
5. **راجع السجلات** في `logs/bot.log` بانتظام

---

## 🔄 التبديل للتداول الحقيقي (mainnet)

بعد التأكد من عمل كل شيء على testnet:

1. أنشئ مفاتيح API جديدة من حساب Bybit الحقيقي
2. عدّل `.env`:
   ```
   BYBIT_API_KEY=المفتاح_الحقيقي
   BYBIT_API_SECRET=السر_الحقيقي
   BYBIT_ENV=mainnet
   ```
3. شغّل `python test_setup.py` مرة أخرى للتأكد
4. شغّل `python main.py`

---

## ❓ حل المشاكل الشائعة

| المشكلة | الحل |
|---------|-------|
| `ModuleNotFoundError` | شغّل `pip install -r requirements.txt` |
| `Invalid API key` | تأكد من نسخ المفتاح كاملاً بدون مسافات |
| `Telegram Unauthorized` | تأكد من صحة التوكن وأنك أرسلت `/start` للبوت |
| `Insufficient funds` | زد رصيد USDT أو قلل `TRADE_AMOUNT_USD` |
| `Rate limit exceeded` | الفحص يستغرق وقتاً عادياً لجميع العملات |
| `Connection timeout` | تحقق من اتصالك بالإنترنت |
