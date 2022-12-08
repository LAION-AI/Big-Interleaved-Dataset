import json
import re
import pandas as pd
from tqdm import tqdm
from collections import OrderedDict

SEPERATOR = "###img###sep###"

def convert_to_image_url_text_parquet(filename):
    df = pd.read_parquet(filename)

    img_url_to_caption = {'URL' : [], 'TEXT' : []}

    end_index = df.shape[0]
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

    new_filename = "{}_url_to_text.parquet".format(filename.split('.')[-2][1:])

    new_df.to_parquet(new_filename, compression=None)

    return new_filename