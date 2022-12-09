import os
import wandb
import torch
import shutil
import cld3
import webdataset as wds
from img2dataset import download
import open_clip
from utils import convert_to_image_url_text_parquet, get_filtered_ngrams, SEPERATOR, get_before_after_text

def main():
    filename = "~/data/bild/00000.parquet"
    convert = True
    download_imgs = False
    compute_clip_similarity = False
    output_dir = os.path.abspath("output")
    ngram_range = (3, 20)
    enable_wandb = False

    if convert:
        converted_filename = convert_to_image_url_text_parquet(filename)
    else:
        converted_filename = filename

    if download_imgs:
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)

        download(
            processes_count=36,
            thread_count=32,
            url_list=converted_filename,
            image_size=256,
            output_folder=output_dir,
            output_format="webdataset",
            input_format="parquet",
            url_col="URL",
            caption_col="TEXT",
            enable_wandb=enable_wandb,
            number_sample_per_shard=1000,
            distributor="multiprocessing",
        )

    if compute_clip_similarity:
        filename = "00000.tar"
        path_to_wds = os.path.join(output_dir, filename)

        # Create model
        model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-32-quickgelu', pretrained='laion400m_e32')
        model = model.to('cuda')
        clip_tokenizer = open_clip.get_tokenizer('ViT-B-32-quickgelu')

        dataset = wds.WebDataset(path_to_wds).decode("pil")

        # Wandb stuff
        wandb.init(project="img_text_pairs", entity="sid1793", mode="online")
        wandb_table = None
        wandb_table_data = []
        wandb_table_cols = ["Image", "Predicted text", "Score"]

        log_freq = 10000

        from itertools import islice

        # Loop through the images dir
        for sample in islice(dataset, 0, 20):
            # Read in image and text 
            text = sample['txt']
            image = sample['jpg']

            # Split text into before and after
            before_text, after_text = get_before_after_text(text)

            before_lang = cld3.get_language(before_text)
            after_lang = cld3.get_language(after_text)

            # Check if English
            if before_lang != "en":
                before_text = ""

            if after_lang != "en":
                after_text = ""

            # Compute and filter ngrams
            candidates = get_filtered_ngrams(before_text, after_text, ngram_range)

            if len(candidates) > 0:

                # Compute embeddings
                with torch.no_grad(), torch.cuda.amp.autocast():
                    inp_image = preprocess(image).unsqueeze(0).to('cuda')
                    tokenized_text = clip_tokenizer(candidates).to('cuda')

                    text_features = model.encode_text(tokenized_text)

                    image_features = model.encode_image(inp_image)

                    image_features /= image_features.norm(dim=-1, keepdim=True)
                    text_features /= text_features.norm(dim=-1, keepdim=True)

                    dot_prod = image_features @ text_features.T
                
                    maximum, argmax = dot_prod.max(dim=-1)

                prediction = candidates[argmax.cpu().item()]

                wandb_table_data.append([wandb.Image(image), prediction, maximum])

                if (len(wandb_table_data) % log_freq) == 0:
                    #TODO - Try logging to the same table
                    wandb_table = wandb.Table(columns=wandb_table_cols, data=wandb_table_data)
                    wandb.log({"predictions_table" : wandb_table})

        wandb_table = wandb.Table(columns=wandb_table_cols, data=wandb_table_data)
        wandb.log({"predictions_table" : wandb_table})

        # TODOs -
        # Logging for stats 
        # Profiling of model on A100

if __name__ == "__main__":
    main()