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
    filename, file_ext = parsed_url.path.rsplit('.', maxsplit=1)
    return file_ext

import re

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


def chunks2darray(chunks:List[str], vids:Dict[str, NamedTuple],auds:Dict[str, NamedTuple],imgs:Dict[str,NamedTuple],iframedict) -> List[Document]:
        # Initialize the output list
    output_list = []
    for x in chunks:
        if not x.startswith("###"):
            output_list.append(Document(text=x))
        # If the element starts with ###
        else:
            if x.startswith("video#", 3):
                # create a bildvideo doc wtih the hash
                output_list.append(Document(bildvideo(vid=None,bildtype="video",md5hash=vids[x].hashm)))

            elif x.startswith("image#", 3):
                # create a bildimage doc wtih the hash
                output_list.append(Document(bildimage(img=None,bildtype="image",md5hash=imgs[x].hashm)))

            elif x.startswith("audio#", 3):
                # create a bildaudio doc wtih the hash
                output_list.append(Document(bildaudio(aud=None,bildtype="audio",md5hash=auds[x].hashm)))

            elif x.startswith("iframe#", 3):
                # create a doc with the bildany format
                output_list.append(Document(bildany(bildtype="iframe",md5hash=iframedict[x].hashm)))
                
    return output_list
