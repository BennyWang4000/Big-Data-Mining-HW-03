# Big-Data-Mining-HW-03

|||
|---|---|
|112598041 王衍斌|shingle, minhash, pre-processing|
|112598044 劉彥鴻|minhash, lsh, pre-processing|

## Usage
```bash
python3 lsh.py -k 2 -H 1
```
## Sample Result
https://ntutcc-my.sharepoint.com/:f:/g/personal/112598041_cc_ntut_edu_tw/EoMc9IVuhB1IhR_0g5B6rKoBuFEp58caZLyieJYPs99BRg?e=G7hAaI
\
parameters
- k-single= 2
- hash-function= 2
- band= 20
- bucket= 4 
### Shingles 
```
+--------------------+--------------------+
|                  _1|                  _2|
+--------------------+--------------------+
|      ('week', 'in')|[1, 0, 0, 0, 0, 0...|
|    ('the', 'Bahia')|[1, 0, 0, 0, 0, 0...|
|  ('Bahia', 'cocoa')|[1, 0, 0, 0, 0, 0...|
|  ('cocoa', 'zone,')|[1, 0, 0, 0, 0, 0...|
|('and', 'improving')|[1, 0, 0, 0, 0, 0...|
|   ('the', 'coming')|[1, 0, 0, 0, 0, 0...|
|('humidity', 'lev...|[1, 0, 0, 0, 0, 0...|
|     ('not', 'been')|[1, 0, 0, 0, 0, 0...|
|('restored,', 'Co...|[1, 0, 0, 0, 0, 0...|
|   ('Smith', 'said')|[1, 0, 0, 0, 0, 0...|
+--------------------+--------------------+
```

## Data Preprocessing
extract the body tag by 'html.parser' of BeautifulSoup4
