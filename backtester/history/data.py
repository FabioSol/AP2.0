from datetime import datetime, timedelta

import pandas as pd


class Data:
    timeframes = {
        'M1': timedelta(minutes=1),
        'M5': timedelta(minutes=5),
        'M10': timedelta(minutes=10),
        'M30': timedelta(minutes=30),
        'H1': timedelta(hours=1),
        'H4': timedelta(hours=4),
        'H12': timedelta(hours=12),
        'D1': timedelta(days=1),
        'W1': timedelta(weeks=1)
    }

    resample_timeframes = {
        'M1': '1min',
        'M5': '5min',
        'M10': '10min',
        'M30': '30min',
        'H1': '1h',
        'H4': '4h',
        'H12': '12h',
        'D1': '1D',
        'W1': '1W',
        'MN1':'ME'
    }

    def __init__(self, data: pd.DataFrame):
        self.data = data
        self.m1 = None
        self.m5 = None
        self.m10 = None
        self.m30 = None
        self.h1 = None
        self.h4 = None
        self.h12 = None
        self.d1 = None
        self.w1 = None

        if all([col in data.columns for col in ['Ask', 'Bid']]):
            grouped_data = data.resample('1T').agg({
                'Ask': ['max', 'first', 'last'],
                'Bid': ['min', 'first', 'last']
            })

            self.m1 = pd.DataFrame({
                'Open': (grouped_data[('Ask', 'first')] + grouped_data[('Bid', 'first')]) / 2,
                'High': grouped_data[('Ask', 'max')],
                'Low': grouped_data[('Bid', 'min')],
                'Close': (grouped_data[('Ask', 'last')] + grouped_data[('Bid', 'last')]) / 2
            })
        elif all([col in data.columns for col in ['Open', 'High', 'Low', 'Close']]):
            time_deltas = data.index.diff().dropna().value_counts()
            time_delta = time_deltas.index[time_deltas.argmax()]

            for key, value in Data.timeframes.items():
                if time_delta == value:
                    setattr(self, key.lower(), data)
                    break
                elif (time_delta < value) and (value % time_delta == timedelta(days=0)):
                    df = Data.resample(data, Data.resample_timeframes.get(key))
                    setattr(self, key.lower(), df)
                    break
            else:
                raise NotImplementedError(f"Not implemented timeframe")
        else:
            raise ValueError("Not supported format")

    def __repr__(self):
        return self.data.__repr__()

    @staticmethod
    def resample(data: pd.DataFrame, tf: str):
        return data.resample(tf).agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last'
        })

    @property
    def M1(self):
        if self.m1 is None:
            raise ValueError("Cannot infer data on that timeframe")
        return self.m1

    @property
    def M5(self):
        if self.m5 is None:
            self.m5 = self.resample(self.M1, Data.resample_timeframes.get("M5"))
        return self.m5

    @property
    def M10(self):
        if self.m10 is None:
            self.m10 = self.resample(self.M5, Data.resample_timeframes.get("M10"))
        return self.m10

    @property
    def M30(self):
        if self.m30 is None:
            self.m30 = self.resample(self.M10, Data.resample_timeframes.get("M30"))
        return self.m30

    @property
    def H1(self):
        if self.h1 is None:
            self.h1 = self.resample(self.M30, Data.resample_timeframes.get("H1"))
        return self.h1

    @property
    def H4(self):
        if self.h4 is None:
            self.h4 = self.resample(self.H1, Data.resample_timeframes.get("H4"))
        return self.h4

    @property
    def H12(self):
        if self.h12 is None:
            self.h12 = self.resample(self.H4, Data.resample_timeframes.get("H12"))
        return self.h12

    @property
    def D1(self):
        if self.d1 is None:
            self.d1 = self.resample(self.H12, Data.resample_timeframes.get("D1"))
        return self.d1

    @property
    def W1(self):
        if self.w1 is None:
            self.w1 = self.resample(self.D1, Data.resample_timeframes.get("W1"))
        return self.w1

    @property
    def MN1(self):
        return self.resample(self.D1, Data.resample_timeframes.get("MN1"))




if __name__ == '__main__':
    from backtester.history.candle_history import CandleHistory
    from backtester.history.tick_history import TickHistory

    # h = TickHistory.from_mt5("EURUSD", datetime(2024, 5, 15), datetime(2024, 5, 15, 2))

    h = CandleHistory.from_mt5("AAPL", datetime(2024, 5, 1), '2024-05-16', 'm6')
    d = Data(h.data)
    print(d.D1)
