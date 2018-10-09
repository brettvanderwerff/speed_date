# speed_date
A simple wrapper for Pandas to_datetime method that speeds up datetime conversions. 

There is not much to this, but if you have a lot of datetime conversions to do it will be faster than the vanilla method and takes all the same args. 

### Installation

```
$pip install speed_date
```

### Usage

```python
import pandas as pd
import speed_date as sd

df = pd.read_csv("file.csv")

df['date'] = sd.to_datetime(df['date'])

```
