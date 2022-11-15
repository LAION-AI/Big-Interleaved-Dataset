from fastwarc.warc import ArchiveIterator
from pathlib import Path
import gzip
import time
import pandas as pd
import logging
from extraction_utils import parser_bytes

def pipeline(warcf,wurl,config):
    warc_stats ={'warc_img_count':0,'warc_vid_count':0,'warc_aud_count':0,'warc_iframe_count':0,'warc_html_hits':0,'warc_exception_counts':0}
    st=time.time()
    data=list()
    logging.basicConfig(
        filename=f"{config['log_store']}/{wurl.split('.')[1].split('/')[-1][-5:]}.log",
        level=logging.DEBUG,
        filemode="w",
        format="%(process)d:%(asctime)s:%(levelname)s:%(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
    )
    for index_r,record in enumerate(ArchiveIterator(gzip.open(warcf), max_content_length=4*1024**2)):

        try:
            if record.headers is None: continue
            if record.http_headers is None: continue
            if record.headers["WARC-Type"]=="response" and record.content_length>=128:
                content_type=str(record.http_content_type).lower()
                
                if content_type.startswith("text/html"):
                    warc_stats['warc_html_hits']+=1
                    url = str(record.headers['WARC-Target-URI'])
                    html_bytes = record.reader.read()
                    page_config,text,imgs,vids,auds,iframes = parser_bytes(url,html_bytes)
                    warc_stats['warc_img_count']+=page_config['img_count']
                    warc_stats['warc_vid_count']+=page_config['vid_count']
                    warc_stats['warc_aud_count']+=page_config['aud_count']
                    warc_stats['warc_iframe_count']+=page_config['iframe_count']
                    
                    logging.debug(f'Sucessflly parsed record index:{warc_stats["warc_html_hits"]}')

                    data.append([warc_stats['warc_html_hits'],url,page_config,text,imgs,vids,auds,iframes])
        
        except Exception as er:
                warc_stats['warc_exception_counts']+=1
                logging.debug(f"An exception occured at index {warc_stats['warc_html_hits']}: Total exceptions: {warc_stats['warc_exception_counts']}")
                
   
    ster=time.time()
    df4 = pd.DataFrame(data, columns=["Warc_Html", "URL","Page_config","Text","Imgs","Vids","Auds","Iframes"])
    df4.to_parquet(f"{config['Extraction_store']}/{wurl.split('.')[1].split('/')[-1][-5:]}.parquet")
    #df_stats=pd.DataFrame.from_dict(warc_stats).to_json(f"{config['Stats_store']}/{wurl.split('.')[1].split('/')[-1][-5:]}_stats.json")
    logging.info(f"This took this much time:{time.time()-st}s")