from typing import Union

from backtester.history.history import History
import pandas as pd
import yfinance as yf
from datetime import datetime
import MetaTrader5 as mt5


class CandleHistory(History):
    cols = ['Open', 'High', 'Low', 'Close']
    yahoo_intervals = {"M1": "1m",
                       "M2": "2m",
                       "M5": "5m",
                       "M15": "15m",
                       "M30": "30m",
                       "M60": "60m",
                       "M90": "90m",
                       "H1": "1h",
                       "D1": "1d",
                       "D5": "5d",
                       "W1": "1wk",
                       "MN1": "1mo",
                       "MN3": "3mo"}
    mt5_intervals = {'M1': mt5.TIMEFRAME_M1,
                     'M2': mt5.TIMEFRAME_M2,
                     'M3': mt5.TIMEFRAME_M3,
                     'M4': mt5.TIMEFRAME_M4,
                     'M5': mt5.TIMEFRAME_M5,
                     'M6': mt5.TIMEFRAME_M6,
                     'M10': mt5.TIMEFRAME_M10,
                     'M12': mt5.TIMEFRAME_M12,
                     'M15': mt5.TIMEFRAME_M15,
                     'M20': mt5.TIMEFRAME_M20,
                     'M30': mt5.TIMEFRAME_M30,
                     'H1': mt5.TIMEFRAME_H1,
                     'H2': mt5.TIMEFRAME_H2,
                     'H3': mt5.TIMEFRAME_H3,
                     'H4': mt5.TIMEFRAME_H4,
                     'H6': mt5.TIMEFRAME_H6,
                     'H8': mt5.TIMEFRAME_H8,
                     'H12': mt5.TIMEFRAME_H12,
                     'D1': mt5.TIMEFRAME_D1,
                     'W1': mt5.TIMEFRAME_W1,
                     'MN1': mt5.TIMEFRAME_MN1}

    def __init__(self, data: pd.DataFrame):
        if not all([col in data.columns for col in CandleHistory.cols]):
            raise ValueError(f"Invalid columns: {data.columns}")
        super().__init__(data)
        self.current_col = 0

    def __next__(self):
        if self.current_col == 0:
            if self.current_idx >= self.length:
                raise StopIteration("No more data available.")
        row = self.data.iloc[self.current_idx]
        col = CandleHistory.cols[self.current_col]
        value = row[col]
        self.current_col += 1
        if self.current_col == 4:
            self.current_col = 0
            self.current_idx += 1
        return value, value

    def reset(self):
        super().reset()
        self.current_col = 0

    @classmethod
    def from_yahoo(cls, symbol: str, start_date: Union[str, datetime], end_date: Union[str, datetime],
                   interval: str = 'D1'):
        if parsed_interval := CandleHistory.yahoo_intervals.get(interval.upper()):
            data = yf.download(symbol, start=start_date, end=end_date, interval=parsed_interval)
            return cls(data)
        else:
            raise ValueError(
                f"Not valid interval: {interval} . valid intervals: {CandleHistory.yahoo_intervals.keys()}")

    @classmethod
    def from_mt5(cls, symbol: str, interval: str, start_date: Union[str, datetime],
                 end_date: Union[str, datetime, None] = None):

        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)

        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        try:
            assert mt5.initialize()
            assert mt5.symbol_select(symbol, True)
            if parsed_interval := CandleHistory.mt5_intervals.get(interval.upper()):
                if end_date is None:
                    rates = mt5.copy_rates_range(symbol, parsed_interval, start_date, datetime.now())
                else:
                    rates = mt5.copy_rates_range(symbol, parsed_interval, start_date, end_date)
            else:
                raise ValueError(
                    f"Not valid interval: {interval} valid intervals: {CandleHistory.mt5_intervals.keys()}")
            assert rates is not None
            data = pd.DataFrame(rates)
            data['time'] = pd.to_datetime(data['time'], unit='s')
            data.set_index('time', inplace=True)
            data.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
            return cls(data)
        except AssertionError:
            raise ConnectionError("Failed to retrieve information from MT5")
        finally:
            mt5.shutdown()


if __name__ == '__main__':
    h = CandleHistory.from_mt5("AAPL",'m5', '2024-05-10')

    print(h)
    print(all([e == j for e, j in zip([i for i in h], [i for i in h])]))
