import requests
import json
import sys
import torch
import io
import pandas as pd
import re
import nltk
from nltk import ngrams 
from nltk.tokenize import word_tokenize
from nltk.corpus import wordnet as wn
from PIL import Image
import open_clip
import wandb
from tqdm.auto import tqdm
from collections import OrderedDict

def get_filtered_ngrams(before_text, after_text, sent_tokenizer, ngram_range, word_tokenizer):
    candidates = sent_tokenizer.tokenize(before_text) + sent_tokenizer.tokenize(after_text)

    filtered_candidates = []
    for i in range(len(candidates)):
        for n in range(*ngram_range):
            for item in ngrams(candidates[i].split(), n):
                item = " ".join(item)
                word_tokens = word_tokenizer(item)	
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

    return filtered_candidates

def main():
    sent_tokenizer = nltk.data.load('tokenizers/punkt/PY3/english.pickle')
    ngram_range = (3, 20)
    model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-32-quickgelu', pretrained='laion400m_e32')
    model = model.to('cuda')
    clip_tokenizer = open_clip.get_tokenizer('ViT-B-32-quickgelu')

    wandb.init(project="img_text_pairs", entity="sid1793", mode="online")
    wandb_table = None
    table_data = []

    df = pd.read_parquet("00000_subset.parquet")

    start_idx = 0

    num_images_logged = 0
    # Loop through all html
    for idx in tqdm(range(start_idx, df.shape[0])):

        row = df.iloc[idx]

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

            # Check if image is jpeg, png
            img_url = img_to_url[img_name]
            if ("jpeg" not in img_url) and ("png" not in img_url):
                continue
            
            # Download image and ignore if size is <5KB
            try:
                img_data = requests.get(img_url).content
            except Exception as e:
                continue

            if sys.getsizeof(img_data) * 1e-3 < 5:
                continue

            # Get filtered ngrams for image before and after 
            candidates = get_filtered_ngrams(before_text, after_text, sent_tokenizer, ngram_range, word_tokenize)
            
            if len(candidates) == 0:
                continue

            try:
                image = Image.open(io.BytesIO(img_data))
            except Exception as e:
                continue
            
            # Read in image
            with torch.no_grad(), torch.cuda.amp.autocast():
                inp_image = preprocess(image).unsqueeze(0).to('cuda')
                tokenized_text = clip_tokenizer(candidates).to('cuda')

                if tokenized_text.shape[0] > 1024:
                    num_candidates = tokenized_text.shape[0]
                    text_features = torch.zeros([num_candidates, 512]).to('cuda')
                    for i in range(0, num_candidates, 1024):
                        tokenized_text_sub = tokenized_text[i:i+1024]
                        text_features[i:i+1024] = model.encode_text(tokenized_text_sub)

                else:
                    text_features = model.encode_text(tokenized_text)

                image_features = model.encode_image(inp_image)

                image_features /= image_features.norm(dim=-1, keepdim=True)
                text_features /= text_features.norm(dim=-1, keepdim=True)

                dot_prod = image_features @ text_features.T
            
                maximum, argmax = dot_prod.max(dim=-1)

            prediction = candidates[argmax.cpu().item()]

            table_data.append([wandb.Image(image), prediction, maximum])

            num_images_logged += 1

            if num_images_logged % 10 == 0:
                # print (f"Num images logged {num_images_logged}")
                wandb_table = wandb.Table(columns=["Image", "Predicted text", "Score"], data=table_data)
                wandb.log({"predictions_table" : wandb_table})

            if num_images_logged > 1000:
                break

        if num_images_logged > 1000:
            break

    wandb_table = wandb.Table(columns=["Image", "Predicted text", "Score"], data=table_data)

    wandb.log({"predictions_table" : wandb_table})

if __name__ == "__main__":
    main()