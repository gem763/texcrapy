import pandas as pd
import re

from tqdm import tqdm_notebook
from nltk.tag import untag
# from dask import compute, delayed
# import dask.threaded
# import dask.multiprocessing
# from dask.diagnostics import ProgressBar

# pbar = ProgressBar()
# pbar.register()


def preproc(text, remove_url=True, remove_mention=True, remove_hashtag=False):
    LINEBREAK = r'\n' # str.replace에서는 r'\n'으로 검색이 안된다
    RT = '((?: rt)|(?:^rt))[^ @]?'
    EMOJI = r'[\U00010000-\U0010ffff]'
    DOTS = '…'
    LONG_BLANK = r'[ ]+'
    
    # \u3131-\u3163\uac00-\ud7a3 는 한글을 의미함
    # URL = r'(?P<url>(https?://)?(www[.])?[^ \u3131-\u3163\uac00-\ud7a3]+[.][a-z]{2,6}\b([^ \u3131-\u3163\uac00-\ud7a3]*))'
    URL1 = r'(?:https?:\/\/)?(?:www[.])?[^ :\u3131-\u3163\uac00-\ud7a3]+[.][a-z]{2,6}\b(?:[^ \u3131-\u3163\uac00-\ud7a3]*)'
    URL2 = r'pic.twitter.com/[a-zA-Z0-9_]+'
    URL = '|'.join((URL1, URL2))
    
    HASHTAG = r'#(?P<inner_hashtag>[^ #@]+)'
    MENTION = r'@(?P<inner_mention>[^ #@]+)' 
    
    #PTNS = '|'.join((LINEBREAK, RT, URL, HASHTAG, MENTION, EMOJI))
    
    #out = {}
    text = re.sub('|'.join((LINEBREAK, RT, EMOJI, DOTS)), '', text.lower())
    
    if remove_url:
        text = re.sub(URL, ' ', text)

    if remove_mention:
        text = re.sub(MENTION, ' ', text)        
    else:
        text = re.sub(MENTION, ' \g<inner_mention>', text)
        
    if remove_hashtag:
        text = re.sub(HASHTAG, ' ', text)
    else:
        text = re.sub(HASHTAG, ' \g<inner_hashtag>', text)
        
    return re.sub(LONG_BLANK, ' ', text).strip()


def tokenize_doc(doc, textkey=None, stopdoc=None, with_pos=False, tagger=None):
    if textkey is None:
        textkey = 'text'
       
    _text = doc[textkey]
    if any(d in _text for d in stopdoc):
        return None
    
    else:
        text = preproc(_text)
        toks = [(tok[0].replace(' - ', ''),tok[1]) for tok in tagger.pos(text)]

        return (toks if with_pos else untag(toks)), text
            
    
def tokenize_docs(*docs, textkey=None, stopdoc=None, with_pos=False, tagger=None):
    #values = [delayed(tokenize_doc)(doc, textkey=textkey, stop_pos=stop_pos, with_pos=with_pos, tagger=tagger) for doc in docs]
    #return compute(*values, scheduler='processes')
        
    #return [tokenize_doc(doc, textkey=textkey, stopdoc=stopdoc, with_pos=with_pos, tagger=tagger) for doc in tqdm_notebook(docs)]

    tok_docs = []
    for doc in tqdm_notebook(docs):
        tok_doc = tokenize_doc(doc, textkey=textkey, stopdoc=stopdoc, with_pos=with_pos, tagger=tagger)
        if tok_doc is not None:
            tok_docs.append(tok_doc)
            
    return tok_docs