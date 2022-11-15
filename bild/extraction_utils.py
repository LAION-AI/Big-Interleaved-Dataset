from typing import Any
from resiliparse.parse import detect_encoding
from resiliparse.parse.html import HTMLTree
from resiliparse.extract.html2text import extract_plain_text
from urllib.parse import urljoin
import json
import os


def parser_bytes(url,html_byte):

    encoding = detect_encoding(html_byte)
    tree = HTMLTree.parse_from_bytes(html_byte,encoding)
    main,iframe_links,vids,imgs,auds=dict(),dict(),dict(),dict(),dict()
    page_config = {'img_count':0,'vid_count':0,'aud_count':0,'iframe_count':0}

    for ele in tree.body.get_elements_by_tag_name("nav"):
        ele.parent.remove_child(ele)

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
        

    text = extract_plain_text(tree, preserve_formatting=False,
                                                  main_content=False, list_bullets=False,
                                                  alt_texts=True, links=False,
                                                  form_fields=False, noscript=False)
    imgs= json.dumps(imgs)
    vids = json.dumps(vids),
    auds = json.dumps(auds)
    iframes= json.dumps(iframe_links)

    return page_config,text,imgs,vids,auds,iframes






           
