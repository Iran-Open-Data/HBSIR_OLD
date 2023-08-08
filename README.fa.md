# بودجه‌ی خانوار ایران

[![en](https://img.shields.io/badge/lang-en-red.svg)](https://github.com/Iran-Open-Data/HBSIR/blob/main/README.md)
[![en](https://img.shields.io/badge/lang-fa-green.svg)](https://github.com/Iran-Open-Data/HBSIR/blob/main/README.fa.md)

ابزاری برای تسهیل استفاده از داده‌های بودجه‌ی خانوار ایران

## راهنمای دریافت
###  پیش‌نیازها
کاربران لینوکس در صورت تمایل به استخراج مستقیم داده از فایل‌های خام مرکز آمار، باید ابزارهای لازم برای باز کردن فایل‌های Access را بر روی سیستم عامل خود نصب کنند.

### ایجاد virtual  environment  
بهتر است این پروژه در یک virtual  environment مجزا نصب شود.  
کد زیر شیوه‌ی ایجاد و فعال کردن چنین محیطی را نشان می‌دهد.
```bash
# create a virtual enviornment
python -m venv .venv

# activate (windows)
.venv\Scripts\Activate
```

### دریافت
برای دریافت نسخه‌های پایدار این کتابخانه می‌توانید از PyPI استفاده کنید :

```bash
# download from PyPI
pip install hbsir
```

همچنین می‌توانید آخرین نسخه‌ی در دست توسعه را از پوشه‌ی Github پروژه دریافت نمایید:
```bash
# download from Github  
pip install git+https://github.com/Iran-Open-Data/HBSIR.git
```

## راهنمای استفاده
