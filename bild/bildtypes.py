from docarray import dataclass, Document
from docarray.typing import Image,Text,Video,Audio


@dataclass
class bildimage:
    img: Image
    bildtype: Text
    md5hash: Text

@dataclass
class bildvideo:
    vid: Video
    bildtype: Text
    md5hash: Text

@dataclass
class bildaudio:
    aud: Audio
    bildtype: Text
    md5hash: Text

@dataclass
class bildany:
    bildtype: Text
    md5hash: Text
