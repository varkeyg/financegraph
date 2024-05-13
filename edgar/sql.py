
tenk_facts_table = """
drop table if exists tenk_facts;
create table tenk_facts (
    cik text,
    name text,
    sic text,
    sicDescription text,
    ein text,
    sym text,
    gaap_metric text,
    gaap_metric_value real,
    gaap_metric_year integer,
    gaap_metric_form text,
    gaap_metric_label text,
    tenk_filename text
)
"""


companies_sql = """
select distinct cik, name, sic, ein, sym from tenk_facts
"""


cns_fails_sql = """
drop table if exists cns_fails;
create table cns_fails (
SETTLEMENT_DATE text,
CUSIP text,
SYMBOL text,
QUANTITY text,
DESCRIP text,
PRICE text )
"""

cusip_ticker_sql = """
drop table if exists cusip_ticker;
create table cusip_ticker as
select cusip, symbol
from cns_fails cf
group by
	cusip, symbol
having
	cf.settlement_date = max(settlement_date)
"""