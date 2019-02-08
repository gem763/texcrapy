import os
import pandas as pd
import twitterscraper as tws
from IPython.core.debugger import set_trace
from tqdm import tqdm_notebook, tqdm
from multiprocessing import Process, Pool, Queue


def _pretty(queried, what):
    return [{k:v for k,v in qrd.__dict__.items() if k in what} for qrd in queried]
    
    
def _period(start=None, end=None):
    if start is None: start = '2010-01-01'
    if end is None: end = pd.Timestamp.today()

    return pd.Timestamp(start).date(), pd.Timestamp(end).date()
    
    
def _scrap(item, q, lang, start, end, what, fname):
    try:
        queried = tws.query_tweets(q, lang=lang, begindate=start, enddate=end)
        res = _pretty(queried, what)

        df = pd.DataFrame(res)
        df.columns = pd.MultiIndex.from_arrays([[item]*len(df.columns), df.columns])
        df.to_pickle(fname.format(item=item))

    except:
        pass
        #rr.append(item)
        
    #finally:
    #    pbar.update(1)
        
        

        
def scrap2(qry_dict, lang=None, start=None, end=None, what=None, download_to=None):
    if what is None:
        what = ['id', 'fullname', 'likes', 'replies', 'retweets', 'text', 'timestamp', 'user']

    if not os.path.exists(download_to):
        os.makedirs(download_to)

    start, end = _period(start=start, end=end)
    fname = download_to + '/{item} (' + str(start).replace('-','.') + '-' + str(end).replace('-','.') + ').pkl'
    
    pool = Pool()
    for item, q in qry_dict.items():
        pool.apply_async(_scrap, args=(item, q, lang, start, end, what, fname))
        
    pool.close()
    pool.join()
        
        
        
def scrap(qry_dict, lang=None, start=None, end=None, what=None, download_to=None):
       
    if what is None:
        what = ['id', 'fullname', 'likes', 'replies', 'retweets', 'text', 'timestamp', 'user']

    if not os.path.exists(download_to):
        os.makedirs(download_to)

    start, end = _period(start=start, end=end)
    fname = download_to + '/{item} (' + str(start).replace('-','.') + '-' + str(end).replace('-','.') + ').pkl'
    err_item = []

    procs = []
    for item, q in qry_dict.items():
        proc = Process(target=_scrap, args=(item, q, lang, start, end, what, fname))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()
          
        
        
    return err_item



def scrap(qry_dict, lang=None, start=None, end=None, what=None, download_to=None):
    if what is None:
        what = ['id', 'fullname', 'likes', 'replies', 'retweets', 'text', 'timestamp', 'user']

    if not os.path.exists(download_to):
        os.makedirs(download_to)

    start, end = _period(start=start, end=end)
    fname = download_to + '/{item} (' + str(start).replace('-','.') + '-' + str(end).replace('-','.') + ').pkl'
    err_item = []

    def _scrap(item, q):
        try:
            queried = tws.query_tweets(q, lang=lang, begindate=start, enddate=end)
            res = _pretty(queried, what)

            df = pd.DataFrame(res)
            df.columns = pd.MultiIndex.from_arrays([[item]*len(df.columns), df.columns])
            df.to_pickle(fname.format(item=item))

        except:
            err_item.append(item)

    
    for item, q in tqdm_notebook(qry_dict.items()):
        _scrap(item, q)
        
    return err_item    