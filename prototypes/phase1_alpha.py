
from resiliparse.parse import detect_encoding
from resiliparse.parse.html import HTMLTree
from resiliparse.extract.html2text import extract_plain_text
from urllib.parse import urljoin
import os
from fastwarc.warc import ArchiveIterator
import gdown
from pathlib import Path
import json
dir_path=Path("/WARC/").parent.mkdir(parents=True, exist_ok=True)





"""Downloads WARC from a drive"""

id = "1InCW8W7ZlgomABD31-6KA0vJg4Yi3wAZ"
gdown.download(id=id, quiet=False)

os.system("gunzip "+"/CC-MAIN-20210916094919-20210916124919-00000.warc.gz")



"""Parser for HTML tree"""

def parser_bytes_main(url,html_byte):


    encoding = detect_encoding(html_byte)
    tree = HTMLTree.parse_from_bytes(html_byte,encoding)
    main,iframe_links,vids,imgs,auds=dict(),dict(),dict(),dict(),dict()



    for z,ele in enumerate(tree.body.get_elements_by_tag_name("img")):
        csrc=ele.getattr("src")
        #imgs.append(urljoin(url, csrc))
        imgs[f'###img###{z}###']=urljoin(url, csrc)
        ele.setattr('alt', f"###img###{z}###")

    for z,ifl in enumerate(tree.body.get_elements_by_tag_name("iframe")):
        csrc=ifl.getattr("src")
        #iframe_links.append(urljoin(url, csrc))
        iframe_links[f"###iframe###{z}###"]= urljoin(url, csrc)
        nele=tree.create_element('img')
        nele['src']=csrc
        nele.setattr('alt', f"###iframe###{z}###")
        ifl.parent.append_child(nele)
        ifl.parent.replace_child(nele,ifl)

    for z,ele in enumerate(tree.body.get_elements_by_tag_name("video")):
        #THIS IS not the way
        csrc=ele.get_elements_by_tag_name("source").getattr('src') #Videos don't have a src attribute
        #vids.append(urljoin(url, csrc))
        vids[f"###video###{z}###"]= urljoin(url, csrc)
        nele=tree.create_element('img')
        nele['src']=csrc
        nele.setattr('alt', f"###video###{z}###")
        ele.parent.append_child(nele)
        ele.parent.replace_child(nele,ele)

    for z,ele in enumerate(tree.body.get_elements_by_tag_name("audio")):
        csrc=ele.getattr("src")
        #auds.append(urljoin(url, csrc))
        auds[f"###audio###{z}###"]= urljoin(url, csrc)
        nele=tree.create_element('img')
        nele['src']=csrc
        nele.setattr('alt', f"###audio###{z}###")
        ele.parent.append_child(nele)
        ele.parent.replace_child(nele,ele)


            
    main['text'] = extract_plain_text(tree, preserve_formatting=False,
                                                  main_content=False, list_bullets=False,
                                                  alt_texts=True, links=False,
                                                  form_fields=False, noscript=False)
    main['imgs'] = json.dumps(imgs)
    main['vids'] = json.dumps(vids),
    main['auds'] = json.dumps(auds)
    main['iframes']= json.dumps(iframe_links)


    return json.dumps(main)

dfr=dict() #just a try to create a dataframe for exceptions

"""

    Main Driver Function

"""

def pipeline(warcpath):
  with open(warcpath,"rb") as f:
    #index_name=int(0)
    exception_count=0
    no_headers,no_records=0,0
    last=None
  
    for index_r,record in enumerate(ArchiveIterator(f, max_content_length=4*1024**2)):

      try:
          if record.headers is None: 
            no_headers+=1
            continue
          if record.http_headers is None: 
            no_records+=1
            continue
          if record.headers["WARC-Type"]=="response" and record.content_length>=128:
            content_type=str(record.http_content_type).lower()
            
            if content_type.startswith("text/html"):
              url = str(record.headers['WARC-Target-URI'])
              html_bytes = record.reader.read()

              print(f'Prcoessing Record: {index_r}')

              json_pack = parser_bytes_main(url,html_bytes)

              print(f'Sucessflly parsed record name:{index_r}')
            
              with open(f'{dir_path}/record{index_r}.json', 'w') as f:
                    json.dump(json_pack, f ,ensure_ascii=False)
                    f.close()

              last=index_r
      except Exception as e:
            exception_count+=1
            if e not in dfr:
              dfr[e]=1
            else:
              dfr[e]+=1
            print(f"An exception occured: count no{exception_count}")
            continue

    print("Done!")
    
    print("TOTAL elements were: ",last)
    
    print("Total no of Misses with no headers: ",no_headers)
    print("Total no of Misses with no records: ",no_records)
    print("Total no of exceptions raised: ",exception_count)


# %timeit -r1 -n2 pipeline("/content/CC-MAIN-20210916094919-20210916124919-00000.warc")

if __name__=="__main__":
  pipeline("/CC-MAIN-20210916094919-20210916124919-00000.warc")