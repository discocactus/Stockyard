
# coding: utf-8
from __future__ import unicode_literals
import numpy as np
import pandas as pd
#import pandas.io.data as web
import pandas.tools.plotting as plotting
import datetime as dt


def get_quote_yahoojp(code, start=None, end=None, interval='d'):
    base = 'http://info.finance.yahoo.co.jp/history/?code={0}.T&{1}&{2}&tm={3}&p={4}'
    #start, end = web._sanitize_dates(start, end)
    start = pd.to_datetime(start)
    if end == None:
        end = pd.to_datetime(pd.datetime.now())
    else :
        end = pd.to_datetime(end)
    start = 'sy={0}&sm={1}&sd={2}'.format(start.year, start.month, start.day)
    end = 'ey={0}&em={1}&ed={2}'.format(end.year, end.month, end.day)
    p = 1
    results = []

    if interval not in ['d', 'w', 'm', 'v']:
        raise ValueError("Invalid interval: valid values are 'd', 'w', 'm' and 'v'")

    while True:
        url = base.format(code, start, end, interval, p)
        tables = pd.read_html(url, header=0)
        if len(tables) < 2 or len(tables[1]) == 0:
            break
        results.append(tables[1])
        p += 1
    result = pd.concat(results, ignore_index=True)

    result.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
    if interval == 'm':
        result['Date'] = pd.to_datetime(result['Date'], format='%Y年%m月')
    else:
        result['Date'] = pd.to_datetime(result['Date'], format='%Y年%m月%d日')
    result = result.set_index('Date')
    result = result.sort_index()
    return result


class OhlcPlot(plotting.LinePlot):
    ohlc_cols = pd.Index(['open', 'high', 'low', 'close'])
    reader_cols = pd.Index(['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'])

    def __init__(self, data, **kwargs):
        data = data.copy()
        self.freq = kwargs.pop('freq', 'B')

        if isinstance(data, pd.Series):
            data = data.resample(self.freq, how='ohlc')
        assert isinstance(data, pd.DataFrame)
        assert isinstance(data.index, pd.DatetimeIndex)
        if data.columns.equals(self.ohlc_cols):
            data.columns = [c.title() for c in data.columns]
        elif data.columns.equals(self.reader_cols):
            pass
        else:
            raise ValueError('data is not ohlc-like')
        data = data[['Open', 'Close', 'High', 'Low']]
        plotting.LinePlot.__init__(self, data, **kwargs)

    def _get_plot_function(self):
        from matplotlib.finance import candlestick
        def _plot(data, ax, **kwds):
            candles = candlestick(ax, data.values, **kwds)
            return candles
        return _plot

    def _make_plot(self):
        from pandas.tseries.plotting import _decorate_axes, format_dateaxis
        plotf = self._get_plot_function()
        ax = self._get_ax(0)

        data = self.data
        data.index.name = 'Date'
        data = data.to_period(freq=self.freq)
        data = data.reset_index(level=0)

        if self._is_ts_plot():
            data['Date'] = data['Date'].apply(lambda x: x.ordinal)
            _decorate_axes(ax, self.freq, self.kwds)
            candles = plotf(data, ax, **self.kwds)
            format_dateaxis(ax, self.freq)
        else:
            from matplotlib.dates import date2num, AutoDateFormatter, AutoDateLocator
            data['Date'] = data['Date'].apply(lambda x: date2num(x.to_timestamp()))
            candles = plotf(data, ax, **self.kwds)

            locator = AutoDateLocator()
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(AutoDateFormatter(locator))


plotting._all_kinds.append('ohlc')
plotting._common_kinds.append('ohlc')
plotting._plot_klass['ohlc'] = OhlcPlot


if __name__ == '__main__':
    import matplotlib.pyplot as plt
    start = '2014-10-01'

    toyota_tse = get_quote_yahoojp(7203, start=start)
    toyota_tse = toyota_tse[-30:]

    toyota_tse.plot(kind='ohlc')
    plt.show()

    toyota_tse.asfreq('B').plot(kind='ohlc')
    plt.subplots_adjust(bottom=0.25)
    plt.show()