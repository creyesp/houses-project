
| | type | size | ratio |
| --- | --- | --- | --- | 
|1	| A | 3 | 0.2 |
|2	| B | 5 | 0.4 |
|3	| C | NaN | 0.8 |

Pandas Data Structures

# Create a DataFrame
```python
>>> import pandas as pd

>>> df = pd.DataFrame({'type': ['A', 'B', 'C'], 'size': [3, 5, None], 'ratio': [0.2, 0.4, 0.8]})
>>> df
  type  size  ratio
0    A   3.0    0.2
1    B   5.0    0.4
2    C   NaN    0.8

```

# I/O
## Read and Write to CSV
```python
>>> df = pd.read_csv('file.csv', header=None, nrows=5)
>>> df.to_csv('myDataFrame.csv')
```

# Selection
## Getting
```python
df['type']  # Select all values from type column
df[['type', 'size']]  # Select all values from type and size columns
```
## Selecting', Boolean Indexing and Setting
### By Position
```python
df.iloc[:2, 0]
```
### By label
```python
df.loc[:2, 'type']
```	
### Boolean Indexing
```python
df[df['ratio'] > 0.5]
df[(df['ratio'] > 0.5) & (df['type'] == 'A')]  # or |, and &, not ~
```
### Setting, create a new column
```python
df['ratio_x_100'] = df['ratio'] * 100
```
# Dropping
## columns
```python
df.drop(columns=['type'])
df.drop('type', axis=1)
```
## missing values
```python
df.dropna()
```
# Sort
## indexs
```python
df.sort_index()
```
## values
```python
df.sort_values(by='Country') 
df['size']
```
# Retrieving Series/DataFrame Information
## basic info
```python
df.shape
df.index
df.columns
df.dtypes
```
## Summary statistics
```python
df.describe()
```
## number of valid values
```python
df.count()
```
# Summary
```python
df.sum()
df.cumsum()
df.min()
df.max()
df.mean()
df.median()
df.std()
```
# Applying functions
```python
df.apply(lambda x: x + 2, axis=0)
```
# Arithmetic Operations
```python
ss = df['size'] * df['ratio']
```
# Fill Methods
```python
df['size'].fillna(0)
df['size'].fillna()
```

# Plotting
```python
df['size'].plot()
df.plot.hist(bins=10)
```