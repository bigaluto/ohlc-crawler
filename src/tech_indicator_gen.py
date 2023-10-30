import pandas as pd

from src.core.tech_indicator import (
    generate_bollinger_band,
    generate_ema,
    generate_ma,
    generate_macd,
    generate_modified_stochastic_oscillator,
    generate_stochastic_oscillator,
)


def generate_tech_indicator(df: pd.DataFrame) -> pd.DataFrame:
    generate_ma(df, 5, 13)
    generate_ema(df, 20)
    generate_macd(df, 12, 26, 9)
    generate_macd(df, 21, 55, 13)
    generate_stochastic_oscillator(df, 5)
    generate_stochastic_oscillator(df, 14)
    generate_stochastic_oscillator(df, 60)
    generate_stochastic_oscillator(df, 233)
    generate_modified_stochastic_oscillator(df, 5)
    generate_modified_stochastic_oscillator(df, 14)
    generate_modified_stochastic_oscillator(df, 60)
    generate_modified_stochastic_oscillator(df, 233)
    generate_bollinger_band(df, 21, 2, "blue")
    generate_bollinger_band(df, 34, 2, "red")
    generate_bollinger_band(df, 55, 2, "green")

    return df
