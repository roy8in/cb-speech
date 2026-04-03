from .frb import FRBScraper
from .ecb import ECBScraper
from .boe import BOEScraper
from .boj import BOJScraper
from .rba import RBAScraper
from .boc import BOCScraper

ALL_SCRAPERS = {
    'FRB': FRBScraper,
    'ECB': ECBScraper,
    'BOE': BOEScraper,
    'BOJ': BOJScraper,
    'RBA': RBAScraper,
    'BOC': BOCScraper,
}
