# %%
from glob import glob
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

PATH = '/mnt/e/dataset/reuters+21578+text+categorization+collection/reuters21578'

# %%
df = pd.DataFrame()
for filepath in tqdm(glob(PATH + '/*.sgm')):
    with open(filepath, 'br') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
        bodies = soup.find_all('body')
        dates = soup.find_all('date')
        for body, date in zip(bodies, dates):
            row = pd.Series()
            row['date'] = date.string
            row['body'] = body.string.replace('\n', ' ')
            df = pd.concat([df, row.to_frame().T], axis=0, ignore_index=True)

df.to_csv('./body.csv')
# %%
