import os
import logging
from edgar import utils as ut
import json


class ThirteenF:
    def __init__(self) -> None:
        self.logger = logging.getLogger("ThirteenF")
        self.data_dir = os.environ["HOME"] + "/Downloads/financegraph/"
        self.thirteenf_dir = self.data_dir + "thirteenf/"
        os.makedirs(self.data_dir, exist_ok=True)
        self.base_url = "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/"  # 2023q4_form13f.zip
        self.years = [2023]
        self.urls = []
        self.load_urls()
        self.db = ut.DB(self.data_dir + "finance.sqlite")

    def load_urls(self):
        for year in self.years:
            for q in range(1, 5):
                self.urls.append(f"{self.base_url}{year}q{q}_form13f.zip")

    def download13fs(self):
        for url in self.urls:
            dest_dir = self.thirteenf_dir + url.split("/")[-1][:-4] + "/"
            os.makedirs(dest_dir, exist_ok=True)
            filename = dest_dir + url.split("/")[-1]
            ut.download_file(url, filename)
            ut.unzip_file(filename, dest_dir)

    def create_db(self):
        with open("resources/FORM13F_metadata.json","r") as f:
            metadata = json.load(f)
            tables = metadata["tables"]
            for table in tables:
                table_name = table["url"][:-4].lower()
                ddl = f"\ndrop table if exists {table_name};\n"
                ddl += f"create table {table_name} \n(\n"
                columns = table["tableSchema"]["columns"]
                column_defs = []
                for column in columns:
                    dt = column["datatype"]["base"]
                    if dt == "string" or dt == "date":
                        dt = "text"
                    elif dt == "NUMBER":
                        dt = "real"
                    column_name = column['name'].lower()
                    column_defs.append(f"   {column_name:<30} {dt}")
                ddl += ",\n".join(column_defs)
                ddl += "\n);\n"
                self.db.execute_script(ddl)
                # self.logger.info(ddl)

    def load_data(self):
        for url in self.urls:
            for filename in os.listdir(self.thirteenf_dir + url.split("/")[-1][:-4]):
                if filename.endswith(".tsv"):
                    table_name = filename[:-4].lower()
                    #self.logger.info(filename)
                    self.db.load_file(self.thirteenf_dir + url.split("/")[-1][:-4] + "/" + filename,table_name)
