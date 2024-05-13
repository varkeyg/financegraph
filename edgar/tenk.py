from edgar import utils as ut
from edgar import sql
import json
import logging
import os
import csv


class TenK:
    def __init__(self) -> None:
        self.logger = logging.getLogger("TenK")
        self.data_dir = os.environ["HOME"] + "/Downloads/financegraph/"
        self.tenk_dir = self.data_dir + "tenk/"
        os.makedirs(self.tenk_dir, exist_ok=True)
        self.syms = ["AAPL", "DE", "NFLX", "CAT"]
        # self.syms = [ "NFLX"]
        self.cik_ticker_url = "https://www.sec.gov/include/ticker.txt"
        self.sub_url = "https://data.sec.gov/submissions/"
        self.facts_url = "https://data.sec.gov/api/xbrl/companyfacts/"
        self.tenk_url = "https://www.sec.gov/Archives/edgar/data/"
        self.ticker_cik = dict()
        self.load_cik_ticker_dict()
        self.db = ut.DB(self.data_dir + "finance.sqlite")

    def load_cik_ticker_dict(self):
        res = ut.download_url(self.cik_ticker_url, header=ut.header)
        for line in res.split("\n"):
            if line:
                ticker, cik = line.split("\t")
                self.ticker_cik[ticker.upper()] = cik

    def get_full_cik(self, cik) -> str:
        cik = cik.rjust(10, "0")
        return "CIK" + cik

    def get_facts_and_docs(self):
        structured_facts = []
        for sym in self.syms:
            self.logger.info(f"Getting facts for {sym}")
            cik = self.ticker_cik[sym]
            cik_full = self.get_full_cik(cik)
            facts = self.get_facts(self.sub_url + cik_full + ".json")
            facts.append(sym)
            tenk_facts = self.get_10k_facts(facts, self.facts_url + cik_full + ".json")
            for rec in tenk_facts:
                structured_facts.append(rec)
            tenk_url = self.tenk_url + self.ticker_cik[sym] + "/"
            self.download_10k_filings(self.sub_url + cik_full + ".json", cik, sym)
        self.save_facts_data(structured_facts, self.tenk_dir + "structured_facts.tsv")
        self.load_10_data()
        
    
    def export_graph_data(self):
        data_dir = self.data_dir + "graph_data/"
        os.makedirs(data_dir, exist_ok=True)
        self.db.export_data(sql.companies_sql,data_dir + "company_nodes.csv")

    def load_10_data(self):
        self.db.execute_script(sql.tenk_facts_table)
        self.db.load_file(self.tenk_dir + "structured_facts.tsv", "tenk_facts")


    def save_facts_data(self, facts, filename):
        header = [
            "cik",
            "name",
            "sic",
            "sicDescription",
            "ein",
            "sym",
            "gaap_metric",
            "gaap_metric_value",
            "gaap_metric_year",
            "gaap_metric_form",
            "gaap_metric_label",
        ]
        with open(filename, "w" ,newline='\n') as csvfile:
            fieldnames = header
            writer = csv.writer(csvfile, delimiter="\t")
            #writer.writeheader()
            writer.writerow(header)
            for row in facts:
                filename = f"{self.tenk_dir}{row[5]}_{row[0]}_10-K_{row[8]}.txt"
                filename = f"{row[5]}_{row[0]}_10-K_{row[8]}.txt"
                row.append(filename)
                writer.writerow(row)

    def download_10k_filings(self, submission_url, cik, sym):
        res = ut.download_url(submission_url, ut.header2)
        sub = json.loads(res)
        recent_filings = sub["filings"]["recent"]
        # find the 10K filing details
        filings = recent_filings["form"]
        tenk_indices = []
        for row_id, row in enumerate(filings):
            if row == "10-K":
                tenk_indices.append(row_id)
        for index in tenk_indices:
            url = self.tenk_url + cik + "/"
            an = recent_filings["accessionNumber"][index]
            an_short = an.replace("-", "")
            url += an_short + "/"
            url += an + ".txt"
            report_date = recent_filings["reportDate"][index][:4]
            fname = f"{self.tenk_dir}{sym}_{cik}_10-K_{report_date}.txt"
            ut.download_file(url, fname)
            # print(fname)

    def get_facts(self, submission_url):
        res = ut.download_url(submission_url, ut.header2)
        sub = json.loads(res)
        return [sub["cik"], sub["name"], sub["sic"], sub["sicDescription"], sub["ein"]]

    def get_10k_facts(self, basic_facts, facts_url):
        recordset = []
        res = ut.download_url(facts_url, ut.header2)
        try:
            facts = json.loads(res)["facts"]["us-gaap"]
            for key, fact_data in facts.items():
                fact_history = fact_data["units"]["USD"]
                for rec in fact_history:
                    if rec["form"] == "10-K":
                        fact_record = []
                        fact_record.extend(basic_facts)
                        fact_record.append(key)
                        fact_record.append(rec["val"])
                        fact_record.append(rec["fy"])
                        fact_record.append(rec["form"])
                        fact_record.append(fact_data["label"])
                        recordset.append(fact_record)
            return recordset
        except Exception as e:
            return recordset
        return recordset
