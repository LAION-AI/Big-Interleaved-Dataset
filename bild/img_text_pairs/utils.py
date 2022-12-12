import json
import re
import nltk
import pandas as pd
from tqdm import tqdm
from nltk import ngrams 
from nltk.tokenize import word_tokenize
from nltk.corpus import wordnet as wn
from collections import OrderedDict

SEPERATOR = "###img###sep###"
nltk_download = False

# TODO - clean this up
if nltk_download:
    nltk.download('averaged_perceptron_tagger')
    nltk.download('punkt')
    nltk.download('wordnet')
    nltk.download('omw-1.4')

def convert_to_image_url_text_parquet(filename, debug):
    df = pd.read_parquet(filename)

    img_url_to_caption = {'URL' : [], 'TEXT' : []}

    end_index = 20 if debug else df.shape[0]
    for row_idx in tqdm(range(end_index)):
        row = df.iloc[row_idx]

        text = row['Text']

        img_to_url = json.loads(row['Imgs'])

        # Get start and end indices of every image tag in text
        # Hack think more about this
        img_to_idxs = OrderedDict()
        for img_name in img_to_url.keys():
            if img_name in text:
                img_to_idxs[img_name] = re.search(img_name, text).span()
        
        last_end = 0
        # For every image 
        for idx, (img_name, img_span) in enumerate(img_to_idxs.items()):

            # get text before and text after image
            start, end = img_span

            before_text = text[last_end:start]
            last_end = end

            if idx == (len(img_to_idxs) - 1):
                after_text = text[end:]
            else:
                next_img_name = list(img_to_idxs.keys())[idx + 1]
                after_text = text[end:img_to_idxs[next_img_name][0]]

            url = img_to_url[img_name]
            surrounding_text = before_text + SEPERATOR + after_text

            img_url_to_caption['URL'].append(url)
            img_url_to_caption['TEXT'].append(surrounding_text)

    new_df = pd.DataFrame(img_url_to_caption)

    new_filename = "{}_url_to_text.parquet".format(filename.split('.')[-2])

    print (f"{new_filename} created")

    new_df.to_parquet(new_filename, compression=None)

    return new_filename

def get_filtered_ngrams(text, ngram_range, lang):
    sent_tokenizer = nltk.data.load('tokenizers/punkt/PY3/english.pickle')

    if lang == 'en':
        candidates = sent_tokenizer.tokenize(text)
    else:
        candidates = [[text]]

    filtered_candidates = []
    for i in range(len(candidates)):
        for n in range(*ngram_range):
            for item in ngrams(candidates[i].split(), n):
                item = " ".join(item)

                if lang == 'en':
                    word_tokens = word_tokenize(item)	
                    adj_present = False
                    verb_or_noun_present = False

                    for word in word_tokens:
                        wordtype = set()
                        for tmp in wn.synsets(word):
                            if tmp.name().split('.')[0] == word:
                                wordtype.add(tmp.pos())

                        if ('a' in wordtype or 's' in wordtype):
                            adj_present = True

                        if ('n' in wordtype or 'v' in wordtype):
                            verb_or_noun_present = True

                        if adj_present and verb_or_noun_present:
                            filtered_candidates.append(item)
                            break
                else:
                    filtered_candidates.append(item)
    return filtered_candidates

def get_before_after_text(text):
    sep_span = re.search(SEPERATOR, text).span()

    # Remove urls and email ids - see pycld3 README
    url_re = r"\b(?:https?://|www\.)[a-z0-9-]+(\.[a-z0-9-]+)+(?:[/?].*)?"
    before_text = re.sub(url_re, "", text[:sep_span[0]].strip())
    after_text = re.sub(url_re, "", text[sep_span[1]:].strip())

    return before_text, after_text