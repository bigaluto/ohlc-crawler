from math import atan, pi
import pandas as pd
import numpy as np


def generate_ma(df, ma, ma2):
    df['ma{}'.format(ma)] = df['Close'].rolling(window=ma, min_periods=ma).mean()
    df['ma{}'.format(ma2)] = df['Close'].rolling(window=ma2, min_periods=ma2).mean()

    a = df['ma{}'.format(ma)].tail(2).values.tolist()
    b = df['ma{}'.format(ma2)].tail(2).values.tolist()

    diff_past = a[0] - b[0]
    diff_present = a[1] - b[1]

    time = (a[1] + b[1]) / 2 * 0.01

    opposite1 = diff_past
    adjacent1 = time
    opposite2 = diff_present
    adjacent2 = time

    angle1 = atan(opposite1 / adjacent1) * 180 / pi
    angle2 = atan(opposite2 / adjacent2) * 180 / pi

    current_angle = abs(angle1 - angle2)

    df['ma_angle'] = np.NaN
    # df['ma_angle'].iloc[-1] = current_angle   # Obsolete way of assigning values
    df.iloc[-1, df.columns.get_loc('ma_angle')] = current_angle


def generate_ema(df, ema_val):
    df['ema{}'.format(ema_val)] = df['Close'].ewm(span=ema_val, min_periods=ema_val).mean()


def generate_macd(df, ema1, ema2, signal):

    df['EMA{}'.format(ema1)] = df['Close'].ewm(span=ema1, min_periods=ema1).mean()
    df['EMA{}'.format(ema2)] = df['Close'].ewm(span=ema2, min_periods=ema2).mean()

    macd_symbol = 'MACD[{},{}]'.format(ema1, ema2)
    signal_symbol = 'MACD[{},{}]_Signal{}'.format(ema1, ema2, signal)
    difference = 'MACD[{},{}]_Difference'.format(ema1, ema2)

    df[macd_symbol] = df['EMA{}'.format(ema1)] - df['EMA{}'.format(ema2)]
    df[signal_symbol] = df[macd_symbol].ewm(span=signal, min_periods=signal).mean()

    df[difference] = df[macd_symbol] - df[signal_symbol]
    # df.drop(columns=['Difference'], inplace=True)
    # df.drop(columns=['Signal_{}'.format(signal)], inplace=True)
    df.drop(columns=['EMA{}'.format(ema1), 'EMA{}'.format(ema2)], inplace=True)


def generate_stochastic_oscillator(df, window_size):

    low = high = window_size

    df['L{}'.format(low)] = df['Low'].rolling(window=low, min_periods=low).min()
    df['H{}'.format(high)] = df['High'].rolling(window=high, min_periods=high).max()

    so_k = 'so_K{}'.format(window_size)
    so_d = 'so_D{}'.format(window_size)

    df[so_k] = 100 * (
        (df['Close'] - df['L{}'.format(low)]) / (df['H{}'.format(high)] - df['L{}'.format(low)])
    )
    df[so_d] = df[so_k].rolling(window=3, min_periods=3).mean()

    df.drop(columns=['L{}'.format(low), 'H{}'.format(high)], inplace=True)


def generate_modified_stochastic_oscillator(df, window_size):
    low = high = window_size

    df['Open_blocks'] = df['Open'].shift(low - 1)
    df['Comparator'] = df['Close'] / df['Open_blocks']
    df['Result'] = df['Close'] - df['Open_blocks']
    df['L{}'.format(low)] = df['Low'].rolling(window=low, min_periods=low).min()
    df['H{}'.format(high)] = df['High'].rolling(window=high, min_periods=high).max()

    df['%mK_tmp'] = 100 * (
        (df['Close'] - df['H{}'.format(high)]) / (df['H{}'.format(high)] - df['L{}'.format(low)])
    )
    df['%K_tmp'] = 100 * (
        (df['Close'] - df['L{}'.format(low)]) / (df['H{}'.format(high)] - df['L{}'.format(low)])
    )

    pd.DataFrame({'%m513_K_tmp': list(df['%mK_tmp']), '%K_tmp': list(df['%K_tmp'])})

    modified_k = 'mso_K{}'.format(window_size)
    modified_d = 'mso_D{}'.format(window_size)

    df[modified_k] = np.where(df['Comparator'] >= 1.0, df['%K_tmp'], -df['%mK_tmp'])
    df[modified_k] = df[modified_k] * df['Result'] * 100
    df[modified_d] = df[modified_k].rolling(window=3, min_periods=3).mean()

    df.drop(columns=['L{}'.format(low), 'H{}'.format(high)], inplace=True)
    df.drop(columns=['Open_blocks', 'Result', 'Comparator'], inplace=True)
    df.drop(columns=['%K_tmp', '%mK_tmp'], inplace=True)


def generate_bollinger_band(df, mb, std, colour):

    df['lb_{}'.format(colour)] = df['Close'].rolling(window=mb, min_periods=mb).std()
    df['mb_{}'.format(colour)] = df['Close'].rolling(window=mb, min_periods=mb).mean()
    df['ub_{}'.format(colour)] = df['Close'].rolling(window=mb, min_periods=mb).std()

    df['lb_{}'.format(colour)] = df['mb_{}'.format(colour)] - (df['lb_{}'.format(colour)] * std)
    df['ub_{}'.format(colour)] = df['mb_{}'.format(colour)] + (df['ub_{}'.format(colour)] * std)

    df.drop(columns=['mb_{}'.format(colour)], inplace=True)
