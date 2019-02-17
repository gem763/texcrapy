import pandas as pd
import re

from tqdm import tqdm_notebook
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
        text = re.sub(URL, '', text)

    if remove_mention:
        text = re.sub(MENTION, '', text)        
    else:
        text = re.sub(MENTION, ' \g<inner_mention>', text)
        
    if remove_hashtag:
        text = re.sub(HASHTAG, '', text)
    else:
        text = re.sub(HASHTAG, ' \g<inner_hashtag>', text)
        
    return re.sub(LONG_BLANK, ' ', text).strip()


def tokenize_doc(doc, textkey=None, stop_pos=None, with_pos=False, tagger=None):
    if textkey is None:
        textkey = 'text'
        
    if stop_pos is None:
        stop_pos = ['Punctuation', 'Josa', 'KoreanParticle', 'Foreign', 'Exclamation']
        
    text = preproc(doc[textkey])
    
    if with_pos:
        return [t for t in tagger.pos(text, norm=True, stem=True) if t[1] not in stop_pos]
    else:
        return [t[0] for t in tagger.pos(text, norm=True, stem=True) if t[1] not in stop_pos]
        
    
def tokenize_docs(*docs, textkey=None, stop_pos=None, with_pos=False, tagger=None):
    #values = [delayed(tokenize_doc)(doc, textkey=textkey, stop_pos=stop_pos, with_pos=with_pos, tagger=tagger) for doc in docs]
    #return compute(*values, scheduler='processes')
       
    return [tokenize_doc(doc, textkey=textkey, stop_pos=stop_pos, with_pos=with_pos, tagger=tagger) for doc in tqdm_notebook(docs)]