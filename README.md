# Big-Interleaved-Dataset
Big-Interleaved-Dataset is a LAION project to create an open source multimodal dataset to the likes of Deepmind M3W (MultiModal MassiveWeb dataset).

## Communications:
 Real time convos #big-interleaved-dataset channel in LAION [discord](https://discord.gg/kAyhUK3jyW).

Meeting's: Weekly either on Tuesday or Thursday at 8pm Cet. Link is provided in the channel.

## Usage
Note that:
- Currently not acting as pkg
- Early design expect breaking changes

Create a venv and

``` pip install -r requirements.txt ```

Currently ``` execute.py ``` act as a entry point with no cli, cause the only variable to change is amount of files.
Will integrate cctableindex for accessing all common crawl with a better CLI. Feature name P2

So run this for now, and please take a look at ```examples/1warc.py``` for explaination 

``` python3 execute.py ```

## Design:

- BILD is being designed for keeping locality of different modalities together
- We keep this through crating/converting alt text of elements with a positional flag in the parsed text.
- parsed text is a list of strings such like :

    ["Watch the following video to see how solar system moves in space", ###video###{0}### ,"we can see it in this picture the neptune has a elliptical orbit", ###image###{0}###, ". To see more elliptical orbits See this video", ###youtube_links###{0}### ]
- We use ```chunks2darray``` define in ```bild.hepler_fn.py``` to convert it a list of JINA Documents
- Current design is to create separate tables for modalities (images,audios,videos,iframes) with their md5hash as unique key.
-  We keep track of these modalities while extraction, and create a bildrecord element which is a jina DocumentArray which has md5hash as modality key to be substituted back to record 

