import logging
import logging.config
from edgar import thirteenf, tenk, cnsfails, graphdata

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(module)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
        'file': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': 'financegraph.log',
            'mode': 'a',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default', 'file'],
            'level': 'DEBUG',
            'propagate': True,
        },
    },
}


def download13f():
    logging.config.dictConfig(LOGGING_CONFIG)
    tf = thirteenf.ThirteenF()
    tf.download13fs()

def create_db():
    logging.config.dictConfig(LOGGING_CONFIG)
    tf = thirteenf.ThirteenF()
    tf.create_db()
    tf.load_data()

def get_10k_facts():
    logging.config.dictConfig(LOGGING_CONFIG)
    tk = tenk.TenK()
    tk.get_facts_and_docs()

def get_graph_data():
    logging.config.dictConfig(LOGGING_CONFIG)
    tk = tenk.TenK()
    tk.export_graph_data()

def load_cns():
    logging.config.dictConfig(LOGGING_CONFIG)
    cf = cnsfails.CNSFails()
    
def gen_graph_data():
    logging.config.dictConfig(LOGGING_CONFIG)
    tk = tenk.TenK()
    tk.get_facts_and_docs()
    cf = cnsfails.CNSFails()
    gd = graphdata.GraphData()
    gd.export()