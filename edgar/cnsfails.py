import os
import logging
from edgar import utils as ut
import json
from datetime import datetime, timedelta
from edgar import sql

class CNSFails:
    def __init__(self):
        self.logger = logging.getLogger("CNSFails")
        self.data_dir = os.environ["HOME"] + "/Downloads/financegraph/"
        self.cnsfails_dir = self.data_dir + "cnsfails/"
        os.makedirs(self.cnsfails_dir, exist_ok=True)
        self.cns_url = "https://www.sec.gov/files/data/fails-deliver-data/cnsfails"
        self.urls = []
        self.load_urls()
        self.download_cnsfails()
        self.db = ut.DB(self.data_dir + "finance.sqlite")
        self.db.execute_script(sql.cns_fails_sql)
        self.load_data()
        self.db.execute_script(sql.cusip_ticker_sql)

    ## hack.. need smarter way to not skip a month
    def load_urls(self):
        cur_month = datetime.today().strftime("%Y%m") + "01"
        last_month = (datetime.today() - timedelta(days=30)) #.strftime("%Y%m") + "01"
        for x in range(1,5):
            for y in ["a","b"]:
                last_month = (datetime.today() - timedelta(days=30 * x)).strftime("%Y%m") + y
                self.urls.append(f"{self.cns_url}{last_month}.zip")

    def download_cnsfails(self):
        for url in self.urls:
            dest_dir = self.cnsfails_dir + url.split("/")[-1][:-4] + "/"
            dest_dir = self.cnsfails_dir
            os.makedirs(dest_dir, exist_ok=True)
            filename = dest_dir + url.split("/")[-1]
            try:
                ut.download_file(url, filename)
                ut.unzip_file(filename, dest_dir)
                os.remove(filename)
            except Exception as e:
                pass
    
    def load_data(self):
        table_name = "cns_fails"
        for filename in os.listdir(self.cnsfails_dir):
            if filename != ".DS_Store":        
                # ut.clean_file(self.cnsfails_dir + filename)
                try:
                    self.db.load_file(self.cnsfails_dir + filename,table_name, "|")
                except Exception as e:
                    pass


    
