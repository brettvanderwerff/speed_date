# speed_date
A simple wrapper for Pandas to_datetime method that speeds up datetime conversions.


### Usage

```python
import pandas as pd
import speed_date as sd

df = pd.read_csv("file.csv")

df['date'] = sd.to_datetime(df['date'])

```
