import urllib.parse
import re
from typing import List, Dict, Tuple, NamedTuple, Any
from .bildtypes import bildimage,bildvideo,bildaudio,bildany
from docarray import DocumentArray, Document


def get_extension(url: str) -> str:
    """Parse the URL using the urlparse method
    Get the file name and extension from the parsed URL
    Return the file extension"""
    parsed_url = urllib.parse.urlparse(url)
    try:
        filename, file_ext = parsed_url.path.rsplit('.', maxsplit=1)
    except ValueError:
        file_ext = ""
    return file_ext



def text2chunks(string):
    # Create an empty list to store the resulting substrings.
    result = []

    # Keep track of the start and end indices of the previous match.
    prev_end = 0

    # Iterate over the input string, looking for instances of the ###<text>#<digits> pattern.
    for match in re.finditer(r"###[^#]+#\d+###", string):
        # Get the start and end indices of the current match.
        start = match.start()
        end = match.end()

        # Extract the text between the previous match and the current match.
        substring = string[prev_end:start]

        # Add the substring and the current match to the list of substrings.
        result.append(substring)
        result.append(match.group(0))

        # Update the previous end index.
        prev_end = end

    # Extract the text after the last match.
    substring = string[prev_end:]

    # Add the final substring to the list of substrings.
    result.append(substring)

    result = list(filter(lambda x: x != '' , result))
    result = list(filter(lambda x: x != ' ' , result))

    return result


def chunks2darray(chunks:List[str], vids:Dict[str, NamedTuple],auds:Dict[str, NamedTuple],imgs:Dict[str,NamedTuple],iframedict,pg_stats:Dict) -> List[Document]:
        # Initialize the output list
    output_list = []
    bild_stats = {'img_count':0,'vid_count':0,'aud_count':0,'iframe_count':0}
    for x in chunks:
        if x.startswith("video#",3):
            # create a bildvideo doc wtih the hash
            bild_stats['vid_count']+=1
            output_list.append(Document(bildvideo(vid=None,bildtype="video",md5hash=vids[x].hashm)))

        elif x.startswith("img#",3):
            # create a bildimage doc wtih the hash
            bild_stats['img_count']+=1
            output_list.append(Document(bildimage(img=None,bildtype="image",md5hash=imgs[x].hashm)))

        elif x.startswith("audio#",3):
            # create a bildaudio doc wtih the hash
            bild_stats['aud_count']+=1
            output_list.append(Document(bildaudio(aud=None,bildtype="audio",md5hash=auds[x].hashm)))

        elif x.startswith("iframe#",3):
            # create a doc with the bildany format
            bild_stats['iframe_count']+=1
            output_list.append(Document(bildany(bildtype="iframe",md5hash=iframedict[x].hashm)))
        else:
            # If the element does not starts with ###
            output_list.append(Document(text=x))
        # assert bild_stats==pg_stats, "Bild stats and page stats do not match"
        #assert bild_stats == pg_stats, f"Bild stats and page stats do not match: {bild_stats} vs {pg_stats}"
        
    return output_list
