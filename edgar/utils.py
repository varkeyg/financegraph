import logging
import requests
from tqdm import tqdm
import zipfile
import os
import sqlite3
import csv
import codecs

logger = logging.getLogger("Utils")
header = {
    "User-Agent": "GTV NoOne@domain.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}
header2 = {
    "User-Agent": "GTV NoOne@domain.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov",
}
def download_url(url,header) -> str:
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)  
    response = requests.get(url, headers=header)
    if response.ok:
        return response.text
    else:
        response.raise_for_status()


def download_file(url, filename) -> None:
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    response = requests.get(url, stream=True, headers=header)
    if response.ok:
        total = int(response.headers.get("content-length", 0))
        bar = tqdm(
            desc=(filename.split("/")[-1]),
            total=total,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        )
        with open(filename, "wb") as fd:
            for chunk in response.iter_content(chunk_size=1024):
                size = fd.write(chunk)
                bar.update(size)
    else:
        response.raise_for_status()

def clean_file(filename) -> None:
    tmp = filename + ".tmp"
    BLOCKSIZE = 1048576 # or some other, desired size in bytes
    with codecs.open(filename, "r") as sourceFile:
        with codecs.open(tmp, "w", "utf-8") as targetFile:
            while True:
                contents = sourceFile.read(BLOCKSIZE)
                if not contents:
                    break
                targetFile.write(contents)
    os.remove(filename)
    os.rename(tmp, filename)


def unzip_file(filename, dest_dir) -> None:
    os.makedirs(dest_dir, exist_ok=True)
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(dest_dir)


class DB:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def execute_script(self, ddl):
        for sql in ddl.split(";"):
            self.cursor.execute(sql)
        self.cursor.executescript(ddl)
        self.conn.commit()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params)
        self.conn.commit()

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        self.conn.close()

    def load_file(self, filename, table_name, delimiter="\t"):
        rowcount = 0
        with open(filename, "r") as tsvfile:
            tsvreader = csv.reader(tsvfile, delimiter=delimiter)
            next(tsvreader)
            col_count = len(next(tsvreader))
            snippet = ("?," * col_count)[:-1]
            ins_sql = f"insert into {table_name} values ({snippet})"
            data = []
            for idx, row in enumerate(tsvreader):
                if len(tuple(row)) != col_count:
                    continue
                else:
                    data.append(tuple(row))
                if idx % 10000 == 0:
                    self.cursor.executemany(ins_sql, data)
                    self.conn.commit()
                    data = []
                rowcount = idx
            self.cursor.executemany(ins_sql, data)
            self.conn.commit()
            data = []
            logger.info(f"Loaded {rowcount} rows into {table_name} from {filename}")

    def export_data(self, sql, outfile):
        self.cursor.execute(sql)
        header = []
        for col in self.cursor.description:
            header.append(col[0])
        with open(outfile, "w") as csvfile:
            writer = csv.writer(csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            for row in self.cursor:
                writer.writerow(row)