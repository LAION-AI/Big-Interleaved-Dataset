from resiliparse.parse import detect_encoding
from resiliparse.parse.html import HTMLTree
from resiliparse.extract.html2text import extract_plain_text
from urllib.parse import urljoin
import json
import os
from fastwarc.warc import ArchiveIterator
from pathlib import Path
from multiprocessing import Pool
import logging
import multiprocessing
import time

#setup global config manually
global_config=dict()
global_config['cwd'] = Path().absolute()
global_config['warc_house']=global_config['cwd'] / 'warchouse/'
global_config['log_store']=global_config['cwd'] / 'logstore/'
global_config['warc_store']=global_config['cwd'] / 'warc_store/'

for y in global_config.values():
    y.mkdir(parents=True, exist_ok=True)

def parser_bytes(url,html_byte):

    encoding = detect_encoding(html_byte)
    tree = HTMLTree.parse_from_bytes(html_byte,encoding)
    main,iframe_links,vids,imgs,auds=dict(),dict(),dict(),dict(),dict()
    page_config = {'img_count':0,'vid_count':0,'aud_count':0,'iframe_count':0}

    for ele in tree.body.get_elements_by_tag_name("img"):
        csrc=ele.getattr("src")
        imgs[f"###img###{page_config['img_count']}###"]=urljoin(url, csrc)
        ele.setattr('alt', f"###img###{page_config['img_count']}###")
        page_config['img_count']+=1

    for ele in tree.body.get_elements_by_tag_name("iframe"):
        csrc=ele.getattr("src")
        iframe_links[f"###iframe###{page_config['iframe_count']}###"]= urljoin(url, csrc)
        nele=tree.create_element('img')
        nele['src']=csrc
        nele.setattr('alt', f"###iframe###{page_config['iframe_count']}###")
        page_config['iframe_count']+=1
        ele.parent.append_child(nele)
        ele.parent.replace_child(nele,ele)
        
    for ele in tree.body.get_elements_by_tag_name("video"):

      if len(ele.get_elements_by_tag_name("source"))>0:
        mele=ele.get_elements_by_tag_name("source")
        csrc=mele[0].getattr('src') 
        vids[f"###video###{page_config['vid_count']}###"]= urljoin(url, csrc)
        nele=tree.create_element('img')
        nele['src']=csrc
        nele.setattr('alt', f"###video###{page_config['vid_count']}###")
        page_config['vid_count']+=1            
        ele.parent.insert_before(nele,ele)
        ele.parent.remove_child(ele)

      if ele.getattr("src"):
        csrc=ele.getattr("src")
        vids[f"###video###{page_config['vid_count']}###"]= urljoin(url, csrc)
        nele=tree.create_element('img')
        nele.setattr('src',csrc)         
        nele.setattr('alt', f"###video###{page_config['vid_count']}###")
        page_config['vid_count']+=1
        ele.parent.append_child(nele)
        ele.parent.replace_child(nele,ele)

    for ele in tree.body.get_elements_by_tag_name("audio"):
        
      if len(ele.get_elements_by_tag_name("source"))>0:
        mele=ele.get_elements_by_tag_name("source")
        csrc=mele[0].getattr('src') 
        auds[f"###audio###{page_config['aud_count']}###"]= urljoin(url, csrc)
        nele=tree.create_element('img')
        nele.setattr('src',csrc)
        nele.setattr('alt', f"###audio###{page_config['aud_count']}###")
        page_config['aud_count']+=1            
        ele.parent.insert_before(nele,ele)
        ele.parent.remove_child(ele)

      if ele.getattr("src"):
        csrc=ele.getattr("src")
        auds[f"###audio###{page_config['aud_count']}###"]= urljoin(url, csrc)
        nele=tree.create_element('img')
        nele['src']=csrc
        nele.setattr('alt', f"###audio###{page_config['aud_count']}###")
        ele.parent.append_child(nele)
        ele.parent.replace_child(nele,ele)
        page_config['aud_count']+=1
        

    main['text'] = extract_plain_text(tree, preserve_formatting=False,
                                                  main_content=False, list_bullets=False,
                                                  alt_texts=True, links=False,
                                                  form_fields=False, noscript=False)
    main['imgs'] = json.dumps(imgs)
    main['vids'] = json.dumps(vids),
    main['auds'] = json.dumps(auds)
    main['iframes']= json.dumps(iframe_links)

    return json.dumps(main),page_config


def pipeline(warcpath):
    logging.basicConfig(filename=f"{global_config['log_store']}/{os.path.splitext(warcpath)[0][-12:]}.log",level=logging.DEBUG,filemode='w',format='%(process)d:%(asctime)s:%(levelname)s:%(message)s',datefmt='%d-%b-%y %H:%M:%S')
    st=time.time()
    with open(warcpath,"rb") as f:
        warc_stats ={'warc_img_count':0,'warc_vid_count':0,'warc_aud_count':0,'warc_iframe_count':0,'warc_html_hits':0,'warc_exception_counts':0}
        store_path=global_config['warc_house'] / Path(os.path.splitext(warcpath)[0][-12:])
        store_path.mkdir(parents=True, exist_ok=True)
        for index_r,record in enumerate(ArchiveIterator(f, max_content_length=4*1024**2)):
  
            try:
                if record.headers is None: continue
                if record.http_headers is None: continue
                if record.headers["WARC-Type"]=="response" and record.content_length>=128:
                    content_type=str(record.http_content_type).lower()
                    
                    if content_type.startswith("text/html"):
                        warc_stats['warc_html_hits']+=1
                        url = str(record.headers['WARC-Target-URI'])
                        html_bytes = record.reader.read()
                        parsed_page,html_stats = parser_bytes(url,html_bytes)
                        warc_stats['warc_img_count']+=html_stats['img_count']
                        warc_stats['warc_vid_count']+=html_stats['vid_count']
                        warc_stats['warc_aud_count']+=html_stats['aud_count']
                        warc_stats['warc_iframe_count']+=html_stats['iframe_count']
                        
                        html_pack = {'url':url,'parsed_page':parsed_page,'html_stats':html_stats}

                        logging.debug(f'Sucessflly parsed record index:{warc_stats["warc_html_hits"]}')
                        
                        with open(f'{store_path}/record{warc_stats["warc_html_hits"]}.json', 'w') as fre:
                                json.dump(json.dumps(html_pack), fre ,ensure_ascii=False)
                                fre.close()
                      
                 
            except Exception as er:
                    warc_stats['warc_exception_counts']+=1
                    logging.debug(f"An exception occured at index {warc_stats['warc_html_hits']}: Total exceptions: {warc_stats['warc_exception_counts']}")

        logging.debug(f"This took this much time:{time.time()-st}s")
        return warc_stats        


file_list=[os.path.join(global_config['warc_store'], file) for file in os.listdir(global_config['warc_store'])]
 


if __name__=='__main__':
      pool = multiprocessing.Pool()
      outputs = pool.map(pipeline,file_list)
      print(outputs)