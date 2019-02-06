import os
import re
import json
import pandas as pd
import numpy as np
import asyncio
from functools import partial
import twitterscraper as tws
from IPython.core.debugger import set_trace
from tqdm import tqdm_notebook
from konlpy.tag import Okt
from multiprocessing import Pool, Process



def preproc(text, url=True, mention=True, hashtag=True, remove=True):
    LINEBREAK = r'\n' # str.replace에서는 r'\n'으로 검색이 안된다
    RT = ' rt '
    EMOJI = r'[\U00010000-\U0010ffff]'
    
    #URL = r'(?P<url>(https?://)?(www[.])?[^ \u3131-\u3163\uac00-\ud7a3]+[.][a-z]{2,6}\b([^ \u3131-\u3163\uac00-\ud7a3]*))'
    URL = r'(?:https?:\/\/)?(?:www[.])?[^ :\u3131-\u3163\uac00-\ud7a3]+[.][a-z]{2,6}\b(?:[^ \u3131-\u3163\uac00-\ud7a3]*)'
    # \u3131-\u3163\uac00-\ud7a3 는 한글을 의미함
    HASHTAG = r'(#[^ #@]+)'
    MENTION = r'(@[^ #@]+)' 
    
    PTNS = '|'.join((LINEBREAK, RT, URL, HASHTAG, MENTION, EMOJI))
    
    out = {}
    text = re.sub('|'.join((LINEBREAK,RT)), '', text.lower())
    
    if url:
        urls = re.findall(URL, text) #[m.groupdict()['url'] for m in re.finditer(URL, text)]
        urls = [url.replace('\xa0', '') for url in urls]
        out['urls'] = urls
        
    if hashtag:
        hashtags = re.findall(HASHTAG, text)
        out['hashtags'] = [h[1:] for h in hashtags]
        
    if mention:
        mentions = re.findall(MENTION, text)
        out['mention'] = [m[1:] for m in mentions]
        
    if remove:    
        text = re.sub(PTNS, '', text)
        
    out['text'] = text.strip()
    return out

tagger = Okt()
def tokenize(file):
    df = pd.read_pickle(file).droplevel(0, axis=1)
    tokenized = []
    
    for doc in tqdm_notebook(df.text.iloc[:]):
        preprocessed = preproc(doc)
        text, hashtags = preprocessed['text'], preprocessed['hashtags']
        text += ' ' + ' '.join(hashtags)
        _tokenized = [t[0] for t in tagger.pos(text, norm=True, stem=True) if t[1] not in ['Punctuation', 'Josa']]
        tokenized.append(_tokenized)
    
    out = df[['id']].copy()
    
    #df['brand'] = file.split('.')[0]
    out['tokenized'] = tokenized
    return out