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
### باز کردن جداول
جداول با دستور `load_tabe` باز می‌شوند :
```bash
# loads the data table containing information about food from the year 1400
hbsir.load_table("food", 1400)
```
با اجرای دستور بالا چنین جدولی باز می‌شود :

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Year</th>
      <th>ID</th>
      <th>Code</th>
      <th>Provision_Method</th>
      <th>Amount</th>
      <th>Duration</th>
      <th>Price</th>
      <th>Expenditure</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1400</td>
      <td>10004004227</td>
      <td>11112</td>
      <td>Purchase</td>
      <td>10.0</td>
      <td>30</td>
      <td>250000.0</td>
      <td>2500000.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1400</td>
      <td>10004004227</td>
      <td>11142</td>
      <td>Purchase</td>
      <td>10.0</td>
      <td>30</td>
      <td>30000.0</td>
      <td>300000.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1400</td>
      <td>10004004227</td>
      <td>11143</td>
      <td>Purchase</td>
      <td>10.0</td>
      <td>30</td>
      <td>30000.0</td>
      <td>300000.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1400</td>
      <td>10004004227</td>
      <td>11174</td>
      <td>Purchase</td>
      <td>2.0</td>
      <td>30</td>
      <td>350000.0</td>
      <td>700000.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1400</td>
      <td>10004004227</td>
      <td>11211</td>
      <td>Purchase</td>
      <td>2.0</td>
      <td>30</td>
      <td>1300000.0</td>
      <td>2600000.0</td>
    </tr>
  </tbody>
</table>
