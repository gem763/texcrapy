import os
import pandas as pd
import twitterscraper as tws
from IPython.core.debugger import set_trace
from tqdm import tqdm_notebook, tqdm
#from multiprocessing import Process, Pool, Queue
from dask import compute, delayed
import dask.threaded
import dask.multiprocessing
import json

def _pretty(queried, what):
    return [{k:str(v) for k,v in qrd.__dict__.items() if k in what} for qrd in queried]
    
    
def _period(start=None, end=None):
    if start is None: start = '2010-01-01'
    if end is None: end = pd.Timestamp.today()

    return pd.Timestamp(start).date(), pd.Timestamp(end).date()



def scrap(qry_dict, lang=None, start=None, end=None, what=None, download_to=None):
    if what is None:
        what = ['id', 'fullname', 'likes', 'replies', 'retweets', 'text', 'timestamp', 'user']

    if not os.path.exists(download_to):
        os.makedirs(download_to)

    start, end = _period(start=start, end=end)
    fname = download_to + '/{item} (' + str(start).replace('-','.') + '-' + str(end).replace('-','.') + ').json'

    def _scrap(item, q):
        try:
            queried = tws.query_tweets(q, lang=lang, begindate=start, enddate=end)
            res = _pretty(queried, what)

            with open(fname.format(item=item), 'w', encoding='UTF-8-sig') as f:
                json.dump({item:res}, f, ensure_ascii=False)

        except:
            return item

    #values = [delayed(_scrap)(item, q) for item, q in qry_dict.items()]
    #err_items = compute(*values, scheduler='processes', num_workers=4)
            
    err_items = [_scrap(item, q) for item, q in tqdm_notebook(qry_dict.items())]
        
    return [er for er in err_items if er is not None]

 
def scrap2(bname, query, lang=None, start=None, end=None, what=None, download_to=None):
    if what is None:
        what = ['id', 'fullname', 'likes', 'replies', 'retweets', 'text', 'timestamp', 'user']

    if not os.path.exists(download_to):
        os.makedirs(download_to)

    start, end = _period(start=start, end=end)
    fname = download_to + '/{item} (' + str(start).replace('-','.') + '-' + str(end).replace('-','.') + ').json'
    err_item = []

    def _scrap(item, q):
        try:
            queried = tws.query_tweets(q, lang=lang, begindate=start, enddate=end)
            res = _pretty(queried, what)

            #df = pd.DataFrame(res)
            #df.columns = pd.MultiIndex.from_arrays([[item]*len(df.columns), df.columns])
            #df.to_pickle(fname.format(item=item))
            
            with open(fname.format(item=item), 'w', encoding='UTF-8-sig') as f:
                json.dump({item:res}, f, ensure_ascii=False)

        except:
            err_item.append(item)

    
    #for item, q in tqdm_notebook(qry_dict.items()):
    _scrap(bname, query)
        
    return err_item