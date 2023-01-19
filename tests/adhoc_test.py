import json
import os
import re
import hashlib
from collections import namedtuple
from typing import List, Dict, Tuple, NamedTuple, Any

import boto3
import pandas as pd
from resiliparse.parse import detect_encoding
from resiliparse.parse.html import HTMLTree
from resiliparse.extract.html2text import extract_plain_text
from urllib.parse import urljoin, urlparse

from bild.helpers_fn import get_extension, text2chunks, chunks2darray

# Create the named tuple with the specified fields
bildtuple = namedtuple("bildtuple", ["hashm", "file_extension", "src"])

# Description: Tests for the BILD project
def read_html_bytes(filepath: str) -> bytes:
    # Open the file in binary mode
    with open(filepath, "rb") as f:
        # Read the contents of the file
        data = f.read()
    return data


def dictnt_to_list(data: Dict[str, bildtuple]) -> List[Tuple[str, str, str, str]]:
    # Create a list from the key-value pairs in the dictionary using a list comprehension
    lst = [(k, v) for k, v in data.items()]
    # Unpack the values from the named tuples and add them to the list
    lst = [[k, *v] for k, v in lst]
    return lst  # type: ignore


def downls_s3_test(wurl, filename):
    s3client = boto3.client("s3", use_ssl=False)

    # Open the specified file in binary write mode
    with open(filename, "wb") as file:
        # Download the file from S3 and write it to the local file
        s3client.download_fileobj("commoncrawl", wurl, file)


# def parser_bytes2(url,html_byte):
#   """
#   Some notes: csrc,chash are the current source and hash of the image/video/iframe
#   """

#   encoding = detect_encoding(html_byte)
#   tree = HTMLTree.parse_from_bytes(html_byte,encoding)
#   iframedict,vids,imgs,auds=dict(),dict(),dict(),dict()
#   page_config = {'img_count':0,'vid_count':0,'aud_count':0,'iframe_count':0,'youtube_count':0}

#   for ele in tree.body.get_elements_by_tag_name("nav"):
#       ele.parent.remove_child(ele)

#   for ele in tree.body.get_elements_by_tag_name("img"):
#       csrc=urljoin(url, ele.getattr("src"))
#       chash=str(hashlib.md5((csrc).encode()).hexdigest())

#       imgs[f"###img###{page_config['img_count']}###"]=bildtuple(chash,get_extension(csrc),csrc)
#       ele.setattr('alt', f"###img#{page_config['img_count']}###")
#       page_config['img_count']+=1

#   for ele in tree.body.get_elements_by_tag_name("iframe"):
#       csrc=urljoin(url, ele.getattr("src"))
#       chash=str(hashlib.md5((csrc).encode()).hexdigest())

#       iframedict[f"###iframe#{page_config['iframe_count']}###"]= bildtuple(chash,get_extension(csrc),csrc)
#       nele=tree.create_element('img')
#       nele['src']=csrc
#       nele.setattr('alt', f"###iframe#{page_config['iframe_count']}###")
#       page_config['iframe_count']+=1
#       ele.parent.append_child(nele)
#       ele.parent.replace_child(nele,ele)

#   for ele in tree.body.get_elements_by_tag_name("video"):

#     if len(ele.get_elements_by_tag_name("source"))>0:
#       mele=ele.get_elements_by_tag_name("source")
#       csrc=mele[0].getattr('src')
#       csrc=urljoin(url,csrc)
#       chash=str(hashlib.md5((csrc).encode()).hexdigest())

#       vids[f"###video#{page_config['vid_count']}###"]= bildtuple(chash,get_extension(csrc),csrc)
#       nele=tree.create_element('img')
#       nele['src']=csrc
#       nele.setattr('alt', f"###video#{page_config['vid_count']}###")
#       page_config['vid_count']+=1
#       ele.parent.insert_before(nele,ele)
#       ele.parent.remove_child(ele)

#     if ele.getattr("src"):
#       csrc=ele.getattr("src")
#       csrc=urljoin(url,csrc)
#       chash=str(hashlib.md5((csrc).encode()).hexdigest())
#       vids[f"###video#{page_config['vid_count']}###"]= bildtuple(chash,get_extension(csrc),csrc)
#       nele=tree.create_element('img')
#       nele.setattr('src',csrc)
#       nele.setattr('alt', f"###video#{page_config['vid_count']}###")
#       page_config['vid_count']+=1
#       ele.parent.append_child(nele)
#       ele.parent.replace_child(nele,ele)

#   for ele in tree.body.get_elements_by_tag_name("audio"):

#     if len(ele.get_elements_by_tag_name("source"))>0:
#       mele=ele.get_elements_by_tag_name("source")

#       csrc=mele[0].getattr('src')
#       csrc=urljoin(url,csrc)
#       chash=str(hashlib.md5((csrc).encode()).hexdigest())

#       auds[f"###audio#{page_config['aud_count']}###"]=bildtuple(chash,get_extension(csrc),csrc)
#       nele=tree.create_element('img')
#       nele.setattr('src',csrc)
#       nele.setattr('alt', f"###audio#{page_config['aud_count']}###")
#       page_config['aud_count']+=1
#       ele.parent.insert_before(nele,ele)
#       ele.parent.remove_child(ele)

#     if ele.getattr("src"):

#       csrc=ele.getattr("src")
#       csrc=urljoin(url,csrc)
#       chash=str(hashlib.md5((csrc).encode()).hexdigest())

#       auds[f"###audio#{page_config['aud_count']}###"] = bildtuple(chash,get_extension(csrc),csrc)
#       nele=tree.create_element('img')
#       nele['src']=csrc
#       nele.setattr('alt', f"###audio#{page_config['aud_count']}###")
#       ele.parent.append_child(nele)
#       ele.parent.replace_child(nele,ele)
#       page_config['aud_count']+=1


#   text = extract_plain_text(tree, preserve_formatting=False,
#                                                 main_content=False, list_bullets=False,
#                                                 alt_texts=True, links=False,
#                                                 form_fields=False, noscript=False)

#   fmttext = text2chunks(text)

#   bildrecord = chunks2darray(chunks=fmttext,vids=vids,imgs=imgs,auds=auds,iframedict=iframedict)

#   imgs = dictnt_to_list(imgs)
#   vids = dictnt_to_list(vids)
#   auds = dictnt_to_list(auds)
#   iframedict = dictnt_to_list(iframedict)

#   # pandas concat design
#   #types pd.DataFrame.from_dict(imgs, orient='index')

#   # print(fmttext)
#   # print(vids)
#   # print(bildrecord)
#   assert len(fmttext)==len(bildrecord)


# p=parser_bytes2(url="www.google.com",html_byte=read_html_bytes("../assets/video.html"))
