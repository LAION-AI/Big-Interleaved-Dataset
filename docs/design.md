# Big-Interleaved-Dataset
Big-Interleaved-Dataset is a LAION project to create an open source multimodal dataset to the likes of Deepmind M3W (MultiModal MassiveWeb dataset) .

## Communications:
 Real time convos #big-interleaved-dataset channel in LAION [discord](https://discord.gg/kAyhUK3jyW).

Meeting's: Weekly either on Tuesday or Thursday at 8pm Cet. Link is provided in the channel.

 Meeting notes at this [doc](https://docs.google.com/document/d/1R8WYJ1YcEZ5fAYJH91FCglhzAWS92R3zeRy-bppP1ho/edit?usp=sharing).

## Current Progress:
 Current progress is being [tracked here](https://github.com/LAION-AI/Big-Interleaved-Dataset/issues/1)

## Structure of the project:
 Presently BILD is divided into three phases.
 - Phase 1: Data extraction from [common crawl](https://commoncrawl.org/), maybe licensed part of the internet archive from [Webis group](https://webis.de/research/web-archive.html). Being tracked [here](https://github.com/LAION-AI/Big-Interleaved-Dataset/issues/2)
 - Phase 2: Data filtering for NSFW components, data quality, duplicated data, and other broad things.Being tracked [here](https://github.com/LAION-AI/Big-Interleaved-Dataset/issues/3).
 - Phase 3: Filtered data can be used for creating datasets of various modalities. However, this project would like to tackle the interleaved format. Being tracked [here](https://github.com/LAION-AI/Big-Interleaved-Dataset/issues/4)

 # Phase 1

 Data extraction pipeline from data sources.

## Data Sources:

- Common Crawl
- Maybe, licensed part of Internet archive from Webis.de group.
- Other sources that the community can recommend.

## Pipeline:
Common crawl provides most of its dataset in form of WARC files consisting of HTTPS responses. Thus pipeline will have to parse the WARC file and then the underneath HTML response to extract the required data mainly text, different media links etc, disregarding the script, CSS, and other components.

Naturally, it'll be divided into two parts.

1. WARC file parser.
2. HTML parser.

### WARC parser

There are many open-source WARC parsers available in the wild. **_WARCIO_** is most commonly used, but there is an improved version known as [**_FastWARC_**
](https://resiliparse.chatnoir.eu/en/latest/man/fastwarc.html).

### HTML parsers

- There are various HTML parsers available, we may need to select the best suited for our requirements which basically corresponds to text and media-link attributes preservation.
- Our ideal parser should retain the text as well as multimodal attributes in the corresponding HTML along with their locality.

# Phase 2
 To add


 # Phase 3
To add

# More about project here:
[https://docs.google.com/document/d/1R8WYJ1YcEZ5fAYJH91FCglhzAWS92R3zeRy-bppP1ho/edit?usp=sharing](https://docs.google.com/document/d/1R8WYJ1YcEZ5fAYJH91FCglhzAWS92R3zeRy-bppP1ho/edit?usp=sharing)
