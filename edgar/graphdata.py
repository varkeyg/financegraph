from edgar import utils as ut
import os
from edgar import graphsql as gs
import logging


class GraphData:
    def __init__(self):
        self.logger = logging.getLogger("ThirteenF")
        self.data_dir = os.environ["HOME"] + "/Downloads/financegraph/"
        self.graph_data = self.data_dir + "graphdata/"
        os.makedirs(self.graph_data, exist_ok=True)
        self.db = ut.DB(self.data_dir + "finance.sqlite")
        self.logger.info(f"Exporting to dir: {self.graph_data}")

    def export(self):
        self.db.export_data(gs.company_nodes_sql, self.graph_data + "company_nodes.csv")
        self.logger.info("Exported company nodes to company_nodes.csv")
        self.db.export_data(gs.sic_nodex_sql, self.graph_data + "sic_nodes.csv")
        self.logger.info("Exported SIC nodes to sic_nodes.csv")
        self.db.export_data(gs.cik_sic_edges_sql, self.graph_data + "cik_sic_edges.csv")
        self.logger.info("Exported cik sic edges to cik_sic_edges.csv")
        self.db.export_data(gs.gaap_metric_nodes_sql, self.graph_data + "gaap_metric_nodes.csv")
        self.logger.info("Exported gaap_metric_nodes to gaap_metric_nodes.csv")
        self.db.export_data(gs.cik_gaap_metric_edges_sql, self.graph_data + "cik_gaap_metric_edges.csv")
        self.logger.info("Exported cik_gaap_metric_edges to cik_gaap_metric_edges.csv")


