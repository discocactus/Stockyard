
# coding: utf-8

# # --- 日付・時間 ---
# 
# http://www.python-izm.com/contents/basis/date.shtml

# In[ ]:

import datetime

today = datetime.date.today()
todaydetail = datetime.datetime.today()

# 今日の日付

print('--------------------------------------------')
print(today)
print(todaydetail)

# 今日に日付：それぞれの値

print('--------------------------------------------')
print(today.year)
print(today.month)
print(today.day)
print(todaydetail.year)
print(todaydetail.month)
print(todaydetail.day)
print(todaydetail.hour)
print(todaydetail.minute)
print(todaydetail.second)
print(todaydetail.microsecond)

# 日付のフォーマット

print('--------------------------------------------')
print(today.isoformat())
print(todaydetail.strftime("%Y/%m/%d %H:%M:%S"))


# # --- python 現在時刻取得 ---
# 
# https://qiita.com/mykysyk@github/items/e15d7b2b1a988b8e29d4

# In[ ]:

from datetime import datetime

datetime.now().strftime("%Y/%m/%d %H:%M:%S")


# ### 現在日時のdatetimeオブジェクトを取得

# In[ ]:

from datetime import datetime

datetime.now()	# datetime.datetime(2014, 1, 2, 3, 4, 5, 000000)


# ### datetimeオブジェクトを加算する

# In[ ]:

from datetime import datetime, timedelta

datetime(2014,1,2,3,4,5) # datetime.datetime(2014, 1, 2, 3, 4, 5)
datetime(2014,1,2,3,4,5) + timedelta(weeks=1) # datetime.datetime(2014, 1, 9, 3, 4, 5)
datetime(2014,1,2,3,4,5) + timedelta(days=1) # datetime.datetime(2014, 1, 3, 3, 4, 5)
datetime(2014,1,2,3,4,5) + timedelta(hours=1) # datetime.datetime(2014, 1, 2, 4, 4, 5)
datetime(2014,1,2,3,4,5) + timedelta(minutes=1) # datetime.datetime(2014, 1, 2, 3, 5, 5)
datetime(2014,1,2,3,4,5) + timedelta(seconds=1) # datetime.datetime(2014, 1, 2, 3, 4, 6)
datetime(2014,1,2,3,4,5) + timedelta(milliseconds=1) # datetime.datetime(2014, 1, 2, 3, 4, 5, 1000)
datetime(2014,1,2,3,4,5) + timedelta(microseconds=1) # datetime.datetime(2014, 1, 2, 3, 4, 5, 1)


# ### datetimeオブジェクトを減算する

# In[ ]:

from datetime import datetime, timedelta

datetime(2014,1,2,3,4,5) # datetime.datetime(2014, 1, 2, 3, 4, 5)
datetime(2014,1,2,3,4,5) + timedelta(weeks=-1) # datetime.datetime(2013, 12, 26, 3, 4, 5)
datetime(2014,1,2,3,4,5) + timedelta(days=-1) # datetime.datetime(2014, 1, 1, 3, 4, 5)
datetime(2014,1,2,3,4,5) + timedelta(hours=-1) # datetime.datetime(2014, 1, 2, 2, 4, 5)
datetime(2014,1,2,3,4,5) + timedelta(minutes=-1) # datetime.datetime(2014, 1, 2, 3, 3, 5)
datetime(2014,1,2,3,4,5) + timedelta(seconds=-1) # datetime.datetime(2014, 1, 2, 3, 4, 4)
datetime(2014,1,2,3,4,5) + timedelta(milliseconds=-1) # datetime.datetime(2014, 1, 2, 3, 4, 4, 999000)
datetime(2014,1,2,3,4,5) + timedelta(microseconds=-1) # datetime.datetime(2014, 1, 2, 3, 4, 4, 999999)


# ### datetimeオブジェクト → 数値

# In[ ]:

from datetime import datetime

datetime(2014,1,2,3,4,5).year # 2014
datetime(2014,1,2,3,4,5).month # 1
datetime(2014,1,2,3,4,5).day # 2
datetime(2014,1,2,3,4,5).hour # 3
datetime(2014,1,2,3,4,5).minute # 4
datetime(2014,1,2,3,4,5).second # 5


# ### datetimeオブジェクト → 文字列

# In[ ]:

from datetime import datetime

datetime(2014,1,2,3,4,5).strftime('%Y/%m/%d %H:%M:%S') # 2014/01/02 03:04:05	 
datetime(2014,1,2,3,4,5).strftime('%Y') # 2014	年
datetime(2014,1,2,3,4,5).strftime('%y') # 14	年(下2桁)
datetime(2014,1,2,3,4,5).strftime('%m') # 01	月
datetime(2014,1,2,3,4,5).strftime('%d') # 02	日
datetime(2014,1,2,3,4,5).strftime('%H') # 03	時 (24時間表記)
datetime(2014,1,2,3,4,5).strftime('%I') # 03	時 (12時間表記)
datetime(2014,1,2,3,4,5).strftime('%M') # 04	分
datetime(2014,1,2,3,4,5).strftime('%S') # 05	秒
datetime(2014,1,2,3,4,5).strftime('%b') # Jan	短縮月名
datetime(2014,1,2,3,4,5).strftime('%B') # January	月名
datetime(2014,1,2,3,4,5).strftime('%a') # Thu	短縮曜日名
datetime(2014,1,2,3,4,5).strftime('%A') # Thursday	曜日名
datetime(2014,1,2,3,4,5).strftime('%p') # AM	AM or PM
datetime(2014,1,2,3,4,5).isoformat() # 2014-01-02T03:04:05	ISO 8601 形式
datetime(2014,1,2,3,4,5).ctime() # Thu Jan 2 03:04:05 2014	%a %b %d %X %Y
datetime(2014,1,2,3,4,5).strftime('%c') # Thu Jan 2 03:04:05 2014	%a %b %d %X %Y
datetime(2014,1,2,3,4,5).strftime('%x') # 01/02/14	%m/%d/%y
datetime(2014,1,2,3,4,5).strftime('%X') # 03:04:05	%H:%M:%S
datetime(2014,1,2,3,4,5).strftime('%s') # 1388599445	unixtime


# ### 文字列 → datetimeオブジェクト

# In[ ]:

from datetime import datetime

datetime.strptime('2014-01-02 03:04:05', '%Y-%m-%d %H:%M:%S') # datetime.datetime(2014, 1, 2, 4, 3, 5)


# ### datetimeオブジェクト → unixtime

# In[ ]:

from datetime import datetime

datetime(2014,1,2,3,4,5).strftime('%s') # 1388599445


# ### unixtime → datetimeオブジェクト

# In[ ]:

from datetime import datetime

datetime.fromtimestamp(1388599445) # datetime.datetime(2014, 1, 2, 4, 3, 5)


# ### datetimeオブジェクト → dateオブジェクト

# In[ ]:

from datetime import datetime

datetime(2014,1,2,3,4,5).date() # datetime.date(2014, 1, 2)


# ### datetimeオブジェクト → timeオブジェクト

# In[ ]:

from datetime import datetime

datetime(2014,1,2,3,4,5).time() # datetime.time(3, 4, 5)


# # --- Python 日付・時刻ライブラリー 逆引きリファレンス ---
# 
# https://qiita.com/argius/items/0cfc822d378ab77f06b5

# ## APIの基本情報
# 
# 日時に関する標準モジュールとそこに含まれる型について簡単に記載します。  
# 詳しくは、公式リファレンス（参考資料#1）を参照してください。  
# 本文中では、特に説明が無いものについてはfrom datetime import *を施したものとします。
# 
# ### 標準モジュール  
# | 標準モジュール | 説明 |
# |---|---|
# | datetimeモジュール	| 日時に関する標準の型と操作を提供します。 |
# | timeモジュール	       | 低レベル（システムレベル）の時間に関するユーティリティーが含まれます。 |  
# | calendarモジュール	| 主に、テキストカレンダーに関するユーティリティーが含まれます。 |  
# 
# ### 拡張モジュール  
# | 拡張モジュール | 説明 |
# |---|---|
# | dateutil | 標準モジュールでは不足している日時処理を補ってくれるモジュール。   |
# | pytz | タイムゾーンを扱うモジュール。詳細は後述。 |  
# 
# ### 型  
# | 型 | 説明 |
# |---|---|
# | datetime.date	| 日付オブジェクト。 |
# | datetime.datetime	| 日時オブジェクト。 |  
# | datetime.time	| 時刻オブジェクト。timeモジュールと混同しないように。 |  
# | datetime.timedelta | 2つの日時の差を表すオブジェクト。 |  
# | datetime.tzinfo | タイムゾーン情報の抽象基底クラス。 |  
# | datetime.timezone | タイムゾーン情報の固定オフセットによる実装。 |  
# | time.struct_time | UNIXのstruct tm（構造体）に相当する名前付きタプル。 |  
# 
# datetimeモジュールの型階層は以下の通り。  
# ※注：Python3.2未満にはtimezone型は存在しません。
# ```
# object
#     timedelta
#     tzinfo
#         timezone
#     time
#     date
#         datetime
# ```
# 以降、本文中では型のモジュール名を省略します。
# 
# ### naiveとaware（タイムゾーン有無）
# 
# 同じdatetimeオブジェクトでも、タイムゾーンが付いているものと付いていないものがあります。  
# Pythonでは、タイムゾーンが付いているdatetimeをaware（な日時）、付いていないdatetimeをnaive（な日時）と呼びます。
# 
# dateオブジェクトは常にnaive、timeオブジェクトはどちらにもなり得ます。
# 
# タイムゾーン(tzinfo)のインスタンスとして、Python3.2で追加されたtimezoneオブジェクトが利用できます。  
# それ以前はtzinfoの実装は標準モジュールに存在しませんでした。  
# そのような場合は、pytzモジュールを使います。  
# tzinfoの実装には、dateutilモジュールのサブモジュールtzもあります。
# 
# また、任意のオフセット値（JSTなら+09:00）でタイムゾーンを示す場合にはtimezoneで事足りますが、  
# タイムゾーンの処理を厳密に扱う必要がある場合は、pytzモジュールの使用が推奨されています。  
# pytzモジュールは、IANAタイムゾーンデータベース（オルソンデータベース）を参照します。  
# 詳しくは、公式リファレンス（参考資料#1）を参照して下さい。
# 
# IANAタイムゾーンデータベースについては、以下の記事を参照して下さい。
# 
# tz database - Wikipedia  
# https://ja.wikipedia.org/wiki/Tz_database
# 
# naiveとawareの変換方法については、本編中に説明があります。

# ## 任意の日時のオブジェクトを生成する
# 
# コンストラクターにより生成できます。timeオブジェクトについては省略します。

# In[ ]:

from datetime import datetime, date, timezone, timedelta


# In[ ]:

# NG
datetime(2016, 7)


# In[ ]:

datetime(2016, 7, 5)


# In[ ]:

datetime(2016, 7, 5, 13)


# In[ ]:

datetime(2016, 7, 5, 13, 45)


# In[ ]:

datetime(2016, 7, 5, 13, 45, 27)


# In[ ]:

datetime(2016, 7, 5, 13, 45, 27, 123456)


# In[ ]:

datetime(2016, 7, 5, 13, 45, 27, 123456, timezone(timedelta(hours=+9)))


# In[ ]:

# NG
date(2016, 7)


# In[ ]:

date(2016, 7, 5)


# In[ ]:

# NG
date(2016, 7, 5, 0)


# datetimeの7番目の要素は、マイクロ秒（1/1,000,000秒）です。
# 
# タイムスタンプから生成することもできます。
# 現在日時のタイムスタンプについては、次項を参照してください。

# In[ ]:

datetime.fromtimestamp(12345678901.234567)


# In[ ]:

date.fromtimestamp(12345678901.234567)


# ここで言うタイムスタンプ(float)とは、UNIX時間またはエポック秒と呼ばれる値です。
# 
# UNIX時間 - Wikipedia
# https://ja.wikipedia.org/wiki/UNIX%E6%99%82%E9%96%93
# 
# UNIX時間は整数値ですが、Python上ではfloatで表されます。  
# 小数点以下の値は、Python上ではマイクロ秒として扱われますが、例で分かる通り、floatの小数部で表されるので、精度は落ちます。
# 
# datetime.date()メソッドで、日付部分だけをdateとして取り出すことができます。

# In[ ]:

from datetime import datetime, date


# In[ ]:

dt = datetime.now()
dt


# In[ ]:

dt.date()


# ## 現在日時のオブジェクトを生成する
# 
# datetime.now()は、現在日時を示すdatetimeオブジェクトを返します。  
# date.today()は、当日を示すdateオブジェクトを返します。

# In[ ]:

from datetime import datetime, date


# In[ ]:

datetime.now()


# In[ ]:

date.today()


# timeモジュールのtime()関数で現在日時のタイムスタンプを取得できます。  
# タイムスタンプについては、前項を参照してください。

# In[ ]:

import time


# In[ ]:

time.time()


# 他にも、timeモジュールにstruct_timeを返す（古風な）関数がいくつかありますが、今回は扱いません。  
# time.gmtime()関数あたりの説明を読んでみて下さい。

# ## 文字列から日時・日付へ変換
# 
# datetimeモジュールでは、datetime.strptime()関数を使います。  
# ただし、以下の結果を見てもらうと分かる通り、この関数は融通が利きません。

# In[ ]:

from datetime import datetime, timezone, timedelta


# In[ ]:

datetime.strptime("2016/07/05 13:45:06", "%Y/%m/%d %H:%M:%S")


# In[ ]:

# NG
datetime.strptime("2016/07/05 13:45", "%Y/%m/%d %H:%M:%S")


# dateutilモジュールのサブモジュールparserのparse()関数が強力なので、そちらを使いましょう。

# In[ ]:

from dateutil.parser import parse


# In[ ]:

parse("Tue Aug 23 17:00:35 JST 2016")


# In[ ]:

parse("2016-07-03T11:22:33Z")


# In[ ]:

parse("2016/07/03 04:05:06")


# In[ ]:

parse("2016/07/03")


# In[ ]:

parse('16/07/03', yearfirst=True)


# なお、Python2の場合、下記のようなエラーが出ました。
# 
# .../dateutil/parser.py:598: UnicodeWarning: Unicode equal comparison failed to convert both arguments to Unicode - interpreting them as being unequal
#   elif res.tzname and res.tzname in time.tzname:
# 
# この↓記事に書いてあるissueに関係ありそうなんですが、dateutilのバージョンが2.5.1なのに発生しているんですよね。  
# 同類だけど別の（fixされていない）バグなのでしょうか。
# 
# twitter - python dateutil unicode warning - Stack Overflow
# http://stackoverflow.com/questions/21296475/python-dateutil-unicode-warning

# ## 日時・日付から文字列へ変換
# 
# ISO8601形式に変換する場合は、date.isoformat()メソッドを使います。  
# datetimeオブジェクトやdateオブジェクトを単純に文字列に変換する場合は、これが使われます。
# 
# 任意のフォーマットで文字列に変換したい場合は、strftime()メソッドを使用します。

# In[ ]:

from datetime import datetime, timezone, timedelta


# In[ ]:

dt1 = datetime(2016, 7, 5, 13, 45) # naive
dt2 = datetime(2016, 7, 5, 13, 45, tzinfo=timezone(timedelta(hours=+9))) # aware
d = dt1.date()


# In[ ]:

dt1, dt2, d


# In[ ]:

dt1.isoformat()


# In[ ]:

str(dt1)


# In[ ]:

dt2.isoformat()


# In[ ]:

str(dt2)


# In[ ]:

d.isoformat()


# In[ ]:

str(d)


# In[ ]:

dt1.strftime("%Y/%m/%d %H:%M:%S")


# In[ ]:

dt1.strftime("%Y/%m/%d %H:%M:%S %Z %z")


# In[ ]:

dt2.strftime("%Y/%m/%d %H:%M:%S %Z %z")


# フォーマットの書式については、下記リファレンスを参照してください。
# 
# 8.1.8. strftime() と strptime() の振る舞い - 8.1. datetime — 基本的な日付型および時間型 — Python 3.5.1 ドキュメント
# http://docs.python.jp/3.5/library/datetime.html#strftime-strptime-behavior

# ## 2つの日時を比較する
# 
# 比較演算子==,!=,<,<=,>,>=による比較が可能です。
# 
# datetimeとdateでは比較できません。  
# （TypeError: can't compare datetime.datetime to datetime.date）
# また、datetime同士でもawareとnaive間では比較できません。  
# （TypeError: can't compare offset-naive and offset-aware datetimes）
# どちらかの型にそろえてから比較する必要があります。

# In[ ]:

from datetime import datetime, date


# In[ ]:

dt1 = datetime(2016, 7, 1, 22, 30)
dt2 = datetime(2016, 7, 5, 13, 45)


# In[ ]:

dt1 == datetime(2016, 7, 1, 22, 30, 0)


# In[ ]:

dt1 == dt1, dt1 == dt2, dt1 != dt1, dt1 != dt2, dt1 < dt2, dt1 > dt2, dt1 <= dt1, dt2 <= dt1, dt2 >= dt2, dt1 >= dt2


# In[ ]:

d1 = dt1.date()
d2 = dt2.date()


# In[ ]:

d1 == date(2016, 7, 1)


# In[ ]:

d1 == d1, d1 == d2, d1 != d1, d1 != d2, d1 < d2, d1 > d2, d1 <= d1, d2 <= d1, d2 >= d2, d1 >= d2


# ## 2つの日時の間隔を計算する
# 
# Pythonでは、日時の間隔、差分をtimedeltaオブジェクトで表現します。  
# （deltaは差分(difference⇒Δ)の意。）
# 
# timedeltaはdatetime同士またはdate同士の引き算（-演算）で得られます。
# 
# datetimeとdateでは計算できません。  
# （TypeError: unsupported operand type(s) for -）  
# また、datetime同士でもawareとnaive間では計算できません。  
# （TypeError: can't subtract offset-naive and offset-aware datetimes）  
# どちらかの型にそろえてから計算する必要があります。

# In[ ]:

from datetime import datetime, date


# In[ ]:

d1, d2 = date(2016, 7, 1), date(2016, 7, 3)


# In[ ]:

type(d2 - d1)


# In[ ]:

d2 - d1


# In[ ]:

str(d2 - d1)


# In[ ]:

str(d1 - d2)


# In[ ]:

dt1, dt2 = datetime(2016, 7, 1, 10, 30), datetime(2016, 7, 3, 9, 10)


# In[ ]:

dt2 - dt1


# In[ ]:

str(dt2 - dt1)


# ## 2つの日時の月数を計算する
# 
# 前項で書いた通り、2つの日付の差分で返されるのは日数です。  
# 2つの日付の間隔が「何か月間なのか」を知りたい場合はどうしたら良いでしょうか？
# 
# そういう場合は、dateutilモジュールのサブモジュールrelativedeltaを使ってみましょう。  
# 基本的にはこれで解決できるはずです。

# In[ ]:

from datetime import date
from dateutil.relativedelta import relativedelta


# In[ ]:

d1, d2 = date(2014, 11, 22), date(2016, 3, 1)


# In[ ]:

str(d2 - d1)


# In[ ]:

delta = relativedelta(d2, d1)
delta


# In[ ]:

month_count = delta.years * 12 + delta.months
month_count


# 12か月以上はyearsになってしまいますが、years * 12 + monthsで月数を算出することができました。
# 
# ここで少し気になるのが、月末がからんでくる計算、特にうるう年の2月前後の場合です。  
# そこで、2つの日付をそれぞれ1日ずつずらしていって、その場合のdeltaがどうなるかを確認してみました。

# In[ ]:

# 2つの日付を1日ずつずらしてrelativedeltaの結果を調べるスクリプト

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

d1 = date(2016, 2, 26)
d2 = date(2016, 3, 26)
delta1day = timedelta(days=1)

for x in range(9):
    print(d1, d2, relativedelta(d2, d1))
    d1 += delta1day
    d2 += delta1day


# さらに先の日付まで見てみましたが、大雑把にいうとどうやら2月の月末をまたぐ場合にだけ特別扱いをしているようです。  
# それ以外はずっと同じ日数(+29)になりました。
# 
# 2つの日付の間隔を1日増やしてみると、今度は30日の月の月末をまたいでいる場合とそうでない場合で日数が変わりました。  
# きっと、月末日で補正をかけているのでしょう。
# 
# この辺の性質を理解しておけば大丈夫そうですね。

# ## 日付・日時の増減
# 
# datetime - datetimeやdate - dateがtimedeltaになることは既に書きましたが、  
# 日付・日時の増減については、timedeltaを足し引きすることで、増減されたdatetimeやdateを得ることができます。

# In[ ]:

from datetime import datetime, date, timedelta


# In[ ]:

dt = datetime(2016, 7, 5, 13, 45)
d = dt.date()


# In[ ]:

dt + timedelta(days=5, minutes=168)


# In[ ]:

d - timedelta(days=13)


# 月単位での加減は、月数計算のときと同じく、dateutilモジュールのサブモジュールrelativedeltaを使用します。

# In[ ]:

from datetime import date
from dateutil.relativedelta import relativedelta


# In[ ]:

date(2016, 7, 5) - relativedelta(months=32)


# ## naiveとawareの相互変換
# 
# 以下の例は、Python3において、標準モジュールだけでnaiveとawareの相互変換を確認したものです。  
# Python2（正確にはPython3.2未満）にはtimezone型が存在しないので、代わりにpytzモジュールを使ってください。
# 
# naiveなdatetimeをawareなdatetimeにするには、datetime.replace()メソッドを使います。

# In[ ]:

from datetime import datetime, timezone, timedelta


# In[ ]:

jst = timezone(timedelta(hours=+9)) # pytzを使う場合は pytz.timezone('Asia/Tokyo')
jst


# In[ ]:

dt = datetime(2016, 7, 5, 13, 45) # naive
dt


# In[ ]:

dt2 = dt.replace(tzinfo=jst) # aware
dt2


# In[ ]:

str(dt2)


# 参考資料#4-2によると、datetime.replace()メソッドでタイムゾーンを付加した場合に、少しおかしな挙動をするという報告がありました。  
# 代替案として、jst.localize(datetime(...))を使う方法が示されています。  
# 私の環境では再現しませんでしたので、pytzの古いバージョンでのバグなのかも知れません。
# 
# 逆に、awareなdatetimeをnaiveなdatetimeにするには、同じくdatetime.replace()メソッドを使います。  
# タイムゾーンには、「無し」を意味するNoneを指定します。

# In[ ]:

dt3 = dt2.replace(tzinfo=None)
dt3


# In[ ]:

str(dt3)


# ## struct_timeをdatetimeまたはdateに変換
# 
# タイムスタンプ型をstruct_timeで表すことがありますが、これをdatetime型に変換したい場合。
# 
# time.mktime()関数とdatetime.fromtimestamp()関数を使って、  
# struct_time→timestamp(float)→datetime  
# の順に変換します。

# In[ ]:

from datetime import datetime, date
from time import localtime, mktime


# In[ ]:

tm = localtime() # struct_time
tm


# In[ ]:

t = mktime(tm) # タイムスタンプ(float)に変換
t


# In[ ]:

datetime.fromtimestamp(t)


# In[ ]:

date.fromtimestamp(t)


# struct_timeがタプルであることを利用して、次のように書くこともできます。

# In[ ]:

datetime(*tm[:6])


# tm[:6]は年月日時分秒のタプルが得られますので、それをdatetimeのコンストラクターに6つの引数として渡しています。  
# これは簡潔に書けますが、可読性が低下するので避けたほうが良いかも知れません。

# ## datetimeまたはdateをstruct_timeに変換
# 
# 前項とは逆に、datetimeオブジェクトやdateオブジェクトをstruct_timeオブジェクトに変換したい場合は、
# date.timetuple()メソッドを使うとstruct_timeが得られます。

# In[ ]:

from datetime import datetime


# In[ ]:

dt = datetime(2016, 7, 3, 12, 25)
d = dt.date()


# In[ ]:

dt.timetuple()


# In[ ]:

d.timetuple()


# ## 日付・日時のリストを生成する
# 
# ここでは日付の例だけ書きます。  
# 日時の場合でも、型と単位を変えればいずれの方法も同様に適用できるはずです。
# 
# 有限の日付リストを生成するには、timedeltaとrangeを組み合わせて、内包表記でリストを作る方法がすぐに思いつきました。

# In[ ]:

from datetime import date, timedelta


# In[ ]:

start_date = date(2016, 7, 5)
a = [start_date + timedelta(days=x) for x in range(5)]
a


# ...が、これだと1行で書けないのが少し残念です。  
# （start_dateの代わりに直接date(2016, 7, 5)を書けばできますが、もちろんそれは無し。）
# 
# dateutilモジュールのrruleサブモジュールを使うと、1行で書けます。

# In[ ]:

from datetime import date
from dateutil.rrule import rrule, DAILY


# In[ ]:

a = [x.date() for x in rrule(DAILY, count=5, dtstart=date(2016, 7, 5))]
a


# そうそう、こういう風に書きたかったんです。
# 
# 場合によっては、Pandasモジュール（pandas.date_range）を使った方が便利かも知れません。  
# 詳しくは参考資料#5を参照して下さい。

# ## 閏年（うるうどし）の判定
# 
# calendarモジュールのisleap関数を使います。

# In[ ]:

import calendar


# In[ ]:

calendar.isleap(2016)


# In[ ]:

calendar.isleap(2015)


# In[ ]:

from datetime import date


# In[ ]:

d = date(2016, 7, 5)
calendar.isleap(d.year)


# ## datetimeモジュールに属するオブジェクトのbool評価
# 
# datetimeモジュール内の日時関連オブジェクトは、Python3.5の時点では、timedelta(0)と等価のものを除き、すべてTrueと評価されます。  
# （timezoneについては、ドキュメントに記載がありませんでしたので、「すべて」には含みません。）

# In[ ]:

from datetime import *


# In[ ]:

bool(datetime.fromtimestamp(0))


# In[ ]:

bool(date.fromtimestamp(0))


# In[ ]:

bool(time(0, 0, 0))


# In[ ]:

bool(timedelta(1))


# In[ ]:

bool(timedelta(0))


# In[ ]:

bool(timezone.utc)


# ところで、Python2では、午前零時ちょうどのtimeオブジェクトはFalseと評価されます。
# ```
# >>> from datetime import *
# 
# >>> bool(time(0, 0, 0))
# False
# ```
# これは、Python2というよりも、Python3.5未満で存在したバグ（Issue13936）で、Python3.5で修正された挙動です。  
# Python3.4.3で確認したところ、やはりFalseとなりました。

# ## 処理時間計算
# 
# 他の言語でも使われる方法ですが、(処理後のタイムスタンプ － 処理前のタイムスタンプ)で算出します。

# In[ ]:

import time


# In[ ]:

t = time.time()
time.sleep(3.1)
print(time.time() - t)


# 今回は扱いませんが、コード断片の実行時間を計測するには、下記のモジュールを利用する方法もあります。
# 
# 27.5. timeit — 小さなコード断片の実行時間計測 — Python 3.5.1 ドキュメント  
# http://docs.python.jp/3.5/library/timeit.html

# ## 付録
# ### 直列化サンプル
# 
# 各オブジェクトをpickleにより直列化したしたときのイメージです。
# 
# ### Python2での実行結果
# ```
# >>> from datetime import datetime
# >>> import pickle
# >>>
# >>> dt = datetime(2016, 7, 5, 13, 45)
# >>>
# >>> pickle.dumps(dt) # datetime
# "cdatetime\ndatetime\np0\n(S'\\x07\\xe0\\x07\\x05\\r-\\x00\\x00\\x00\\x00'\np1\ntp2\nRp3\n."
# >>> pickle.dumps(dt.date()) # date
# "cdatetime\ndate\np0\n(S'\\x07\\xe0\\x07\\x05'\np1\ntp2\nRp3\n."
# >>> pickle.dumps(dt.timetuple()) # struct_time
# 'ctime\nstruct_time\np0\n((I2016\nI7\nI5\nI13\nI45\nI0\nI1\nI187\nI-1\ntp1\n(dp2\ntp3\nRp4\n.'
# >>> pickle.dumps(dt - dt) # timedelta
# 'cdatetime\ntimedelta\np0\n(I0\nI0\nI0\ntp1\nRp2\n.'
# ```
# ### Python3での実行結果

# In[ ]:

from datetime import datetime
import pickle


# In[ ]:

dt = datetime(2016, 7, 5, 13, 45)


# In[ ]:

pickle.dumps(dt) # datetime


# In[ ]:

pickle.dumps(dt.date()) # date


# In[ ]:

pickle.dumps(dt.timetuple()) # struct_time


# In[ ]:

pickle.dumps(dt - dt) # timedelta


# ## 参考資料
# 
# #1.  
# （公式ドキュメント・標準ライブラリーリファレンス）
# 
# 8.1. datetime — 基本的な日付型および時間型 — Python 3.5.1 ドキュメント  
# http://docs.python.jp/3.5/library/datetime.html#module-datetime  
# 8.1. datetime — 基本的な日付型および時間型 — Python 2.7.x ドキュメント  
# http://docs.python.jp/2/library/datetime.html#module-datetime
# 
# （datetime以外のモジュールのリンクは冗長なので省きます。上記リンクのページのどこかにリンクがあるのでそこから飛んでください。）
# 
# #2.  
# dateutil - powerful extensions to datetime — dateutil 2.5.3 documentation  
# https://dateutil.readthedocs.io/en/stable/
# 
# #3.  
# pytz 2016.4 : Python Package Index  
# https://pypi.python.org/pypi/pytz/
# 
# #4-1.  
# Pythonの日付処理とTimeZone | Nekoya Press  
# http://nekoya.github.io/blog/2013/06/21/python-datetime/
# 
# #4-2.  
# Pythonでdatetimeにtzinfoを付与するのにreplaceを使ってはいけない | Nekoya Press  
# http://nekoya.github.io/blog/2013/07/05/python-datetime-with-jst/
# 
# #5.  
# Python pandas で日時関連のデータ操作をカンタンに - StatsFragments  
# http://sinhrks.hatenablog.com/entry/2014/11/09/183603

# In[ ]:



