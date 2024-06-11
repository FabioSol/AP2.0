from datetime import datetime
from typing import Union

import pandas as pd

from backtester.history.history import History
import MetaTrader5 as mt5


class TickHistory(History):
    cols = ['Ask', 'Bid']

    def __init__(self, data: pd.DataFrame):
        super().__init__(data)
        if not all([col in data.columns for col in TickHistory.cols]):
            raise ValueError(f"Invalid columns: {data.columns}")
        self.ask_array = self.data['Ask'].to_numpy()
        self.bid_array = self.data['Bid'].to_numpy()

    def __next__(self):
        if self.current_idx >= self.length:
            raise StopIteration("No more data available.")
        ask = self.ask_array[self.current_idx]
        bid = self.bid_array[self.current_idx]
        self.current_idx += 1
        return ask, bid

    @classmethod
    def from_mt5(cls, symbol: str, start_date: Union[str, datetime], end_date: Union[str, datetime, None] = None):

        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)

        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        try:
            assert mt5.initialize()
            assert mt5.symbol_select(symbol, True)
            if end_date:
                ticks = mt5.copy_ticks_range(symbol, start_date, end_date, mt5.COPY_TICKS_ALL)
            else:
                ticks = mt5.copy_ticks_from(symbol, start_date, 2 ** 26, mt5.COPY_TICKS_ALL)
            assert ticks is not None
            data = pd.DataFrame(ticks)
            data['time'] = pd.to_datetime(data['time'], unit='s')
            data.set_index('time', inplace=True)
            data.rename(columns={'ask': 'Ask', 'bid': 'Bid'}, inplace=True)
            return cls(data)
        except AssertionError:
            raise ConnectionError("Failed to retrieve information from MT5")
        finally:
            mt5.shutdown()


if __name__ == '__main__':
    h = TickHistory.from_mt5("EURUSD", datetime(2024, 5, 15))
    print(h)
