import os
import json
import re
import nltk
import heapq
import wandb
import pandas as pd
from tqdm import tqdm
from nltk import ngrams 
from nltk.tokenize import word_tokenize
from nltk.corpus import wordnet as wn
from collections import OrderedDict
from cc_net.perplexity import MultiSentencePiece, DocLM

SEPERATOR = "###img###sep###"
nltk_download = False

# TODO - clean this up
if nltk_download:
    nltk.download('averaged_perceptron_tagger')
    nltk.download('punkt')
    nltk.download('wordnet')
    nltk.download('omw-1.4')

path_to_perplexity_models = {
    'oscar' : "/admin/home-siddhesh1793/data/big_sci_lm/oscar",
    'laion2B-en' : "/admin/home-siddhesh1793/data_tooling/kenlm_training/data/laion/lm_sp_1M"
}

def get_stats_table(raw_counts : dict, stats_table_cols : list):
    # Logging for stats 
    stats_table_data = []
    for key, val in raw_counts.items():
        if ("before" in key) or ("after" in key):
            stats_table_data.append([key, val / raw_counts['total_imgs'], val])

    if raw_counts["num_candidates_scored"] > 0:
        stats_table_data.append(["inference_time", raw_counts["inference_time"] / raw_counts["num_candidates_scored"], raw_counts["inference_time"]])
        stats_table_data.append(["matches", raw_counts["matches"] / raw_counts["num_candidates_scored"], raw_counts["matches"]])
        stats_table_data.append(["total_imgs", 1, raw_counts["total_imgs"]])
        stats_table_data.append(["num_candidates_scored", 1, raw_counts["num_candidates_scored"]])

    stats_table = wandb.Table(columns=stats_table_cols, data=stats_table_data)

    return stats_table_data

class LM:
    def __init__(self, tokenizer, lm):
        self.tokenizer = tokenizer
        self.lm = lm

    def get_perplexity(self, text : str, language : str):
        data = {"raw_content" : text, "language" : language}
        data = self.tokenizer.do(data)
        return self.lm.do(data)['perplexity']

def load_perplexity_language_model(model_name):
    path_to_lm = path_to_perplexity_models.get(model_name)

    if path_to_lm is not None:
        sp_path = os.path.join(path_to_lm, "en.sp.model")
        sp = MultiSentencePiece({"en": sp_path}, field="raw_content", output_field="tokenized", normalize=True)

        lm_path = os.path.join(path_to_lm, "en.arpa.bin")
        lm = DocLM({"en": lm_path}, field="tokenized", output_field="perplexity", normalize=False)

        return LM(sp, lm)

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

def get_filtered_ngrams(text, ngram_range, lang, filter_by_lang=False, perplexity_lm=None):
    sent_tokenizer = nltk.data.load('tokenizers/punkt/PY3/english.pickle')

    if lang == 'en':
        candidates = sent_tokenizer.tokenize(text)
    else:
        if not filter_by_lang:
            candidates = [text]
        else:
            return []

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

    if perplexity_lm is not None and lang == "en":
        perplexity_filtered_candidates = []

        for candidate in filtered_candidates:
            perplexity_filtered_candidates.append((candidate, perplexity_lm.get_perplexity(candidate, lang)))

        top_n_candidates = heapq.nlargest(10, perplexity_filtered_candidates, key=lambda x : -x[1])

        return [candidate[0] for candidate in top_n_candidates]

    return filtered_candidates

def get_before_after_text(text):
    sep_span = re.search(SEPERATOR, text).span()

    # Remove urls and email ids - see pycld3 README
    url_re = r"\b(?:https?://|www\.)[a-z0-9-]+(\.[a-z0-9-]+)+(?:[/?].*)?"
    before_text = re.sub(url_re, "", text[:sep_span[0]].strip())
    after_text = re.sub(url_re, "", text[sep_span[1]:].strip())

    return before_text, after_text