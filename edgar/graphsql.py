

company_nodes_sql = """
select
	distinct cik as ":ID",
	cik as "cik:String",
	name as "name:String",
	sic as "sic:String",
	ein as "ein:String",
	sym as "sym:String",
	ct.cusip "cusip:String",
	'Company' as ":LABEL"
from
	tenk_facts tf left outer join 
	cusip_ticker ct on tf.sym  = ct.symbol
"""

sic_nodex_sql = """
select
	distinct sic as ":ID",
	sic as "sic:String",
	sicDescription as "sic:String",
	'SIC' as ":LABEL"
from
	tenk_facts tf 
"""

cik_sic_edges_sql = """
select
	distinct concat(sic,'-',cik) as ":ID",
	sic as ":START_ID",
	cik as ":END_ID",
	'cik-sic' as ":TYPE"
from
	tenk_facts tf 
"""

gaap_metric_nodes_sql = """
select concat(tf.cik,'-',tf.gaap_metric_year,'-',tf.gaap_metric) as ":ID",
	   max(tf.gaap_metric) as "gaap_metric:String",
	   max(tf.gaap_metric_year) as "gaap_metric_year:String",
	   max(tf.gaap_metric_value) as "gaap_metric_value:float",
	   max(tf.gaap_metric_label) as "gaap_metric_label:String",
	   max(tf.tenk_filename) as "tenk_filename:String",
	   max('GAAP-Metric') as ":LABEL"
  from tenk_facts tf 
  group by concat(tf.cik,'-',tf.gaap_metric_year,'-',tf.gaap_metric)
  """

cik_gaap_metric_edges_sql = """
select concat(tf.cik, '-', tf.gaap_metric_year, '-', tf.gaap_metric) as ":ID", max(tf.cik) as ":START_ID", 
	   concat(tf.cik, '-', tf.gaap_metric_year, '-', tf.gaap_metric) as ":END_ID", max('CIK-GAAP-METRIC-BY-YEAR') as ":TYPE"
from tenk_facts tf
group by concat(tf.cik, '-', tf.gaap_metric_year, '-', tf.gaap_metric)
"""