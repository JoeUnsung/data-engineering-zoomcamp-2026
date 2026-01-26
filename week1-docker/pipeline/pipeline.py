import sys

import pandas as pd

print('arguments', sys.argv)

month = sys.argv[1]

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
pd.DataFrame()


print(f'hello pipeline month={month}')