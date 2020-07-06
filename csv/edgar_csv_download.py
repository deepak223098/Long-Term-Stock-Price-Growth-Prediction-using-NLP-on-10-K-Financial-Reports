# Set the Start and End Date
import datetime

# Please download index files chunk by chunk. For example, please first download index files during 2015–2020, then
# download index files during 2001–2005 by changing the following two lines repeatedly, and so on. If you need index
# files up to the most recent year and quarter, comment out the following three lines, remove the comment sign at
# the starting of the next three lines, and define the start_year that immediately follows the ending year of the
# previous chunk.

start_year = 2015       # change start_year and end_year to re-define the chunk
current_year = 2020     # change start_year and end_year to re-define the chunk
current_quarter = 4     # do not change this line

years = list(range(start_year, current_year))
quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
history = [(y, q) for y in years for q in quarters]
for i in range(1, current_quarter + 1):
    history.append((current_year, 'QTR%d' % i))
urls = ['https://www.sec.gov/Archives/edgar/full-index/%d/%s/master.idx' % (x[0], x[1]) for x in history]
urls.sort()

# Download index files and write content into SQLite
import sqlite3
import requests

con = sqlite3.connect('edgar_kevin_idx.db')
cur = con.cursor()
cur.execute('DROP TABLE IF EXISTS idx')
cur.execute('CREATE TABLE idx (cik TEXT, ticker TEXT, type TEXT, date TEXT, path TEXT)')

for url in urls:
    lines = requests.get(url).content.decode("utf-8", "ignore").splitlines()
    records = [tuple(line.split('|')) for line in lines[11:]]
    cur.executemany('INSERT INTO idx VALUES (?, ?, ?, ?, ?)', records) #splits the data into columns
    print(url, 'downloaded and wrote to SQLite')

con.commit()
con.close()

# Write SQLite database to csv
import pandas
from sqlalchemy import create_engine

engine = create_engine('sqlite:///edgar_idx.db')
with engine.connect() as conn, conn.begin():
    data = pandas.read_sql_table('idx', conn)
    data.to_stata('edgar_csv_idx.dta', version=117)
    data.to_csv('edgar_csv_idx.csv')
