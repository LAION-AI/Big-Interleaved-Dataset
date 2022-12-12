import os
import time
import wandb
import torch
import shutil
import cld3
import fire
import logging
import webdataset as wds
from img2dataset import download
from collections import Counter
from models import get_model
from utils import convert_to_image_url_text_parquet, get_filtered_ngrams, get_before_after_text

def run_pipeline(filename=None, 
                 convert=True, 
                 download_imgs=True, 
                 converted_filename=None,
                 compute_clip_similarity=True, 
                 output_dir=None,
                 ngram_range=(3, 20), 
                 enable_wandb=True, 
                 log_frequency=10,
                 model_type='open_clip',
                 device='cuda',
                 max_batch_size=2e10,
                 debug=False,
                 wandb_log_freq=1000,
                 matching_threshold=0.3):

    output_dir = os.path.abspath("output") if output_dir is None else output_dir
    log_frequency = 1 if debug else log_frequency
    wandb_log_frequency = 1 if debug else wandb_log_frequency

    if convert:
        if filename is None:
            raise ValueError("Specify filename to convert")

        converted_filename = convert_to_image_url_text_parquet(filename, debug)

    if download_imgs:
        if not convert and converted_filename is None:
            raise ValueError("Either set \'convert\' to True or specify converted filename")

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

    config_to_log = locals()

    if compute_clip_similarity:
        filenames = [os.path.join(output_dir, filename) for filename in os.listdir(output_dir) if "tar" in filename]

        # Create model
        model = get_model(model_type, device, max_batch_size)

        dataset = wds.WebDataset(filenames).decode("pil")

        # Wandb stuff
        if enable_wandb:
            wandb.init(project="img_text_pairs", entity="sid1793", mode="online", config=config_to_log)
        predictions_table_data = []
        predictions_table_cols = ["Image", "Predicted text", "Score"]
        stats_table_cols = ["Description", "Fraction", "Counts"]

        # Dict for maintaining counts
        raw_counts = Counter()

        # Loop through the images dir
        for idx, sample in enumerate(iter(dataset)):
            raw_counts['total_imgs'] += 1

            # Read in image and text 
            text = sample['txt']
            image = sample['jpg']

            # Split text into before and after
            before_text, after_text = get_before_after_text(text)

            before_lang = cld3.get_language(before_text)
            after_lang = cld3.get_language(after_text)

            if before_lang is not None:
                lang = before_lang.language
                raw_counts[f"before_{lang}"] += 1

            if after_lang is not None:
                lang = after_lang.language
                raw_counts[f"after_{lang}"] += 1

            # Compute and filter ngrams
            candidates = []

            if (before_lang is not None) and (before_lang.language == "en"):
                candidates.extend(get_filtered_ngrams(before_text, ngram_range))

            if (after_lang is not None) and (after_lang.language == "en"):
                candidates.extend(get_filtered_ngrams(after_text, ngram_range))

            if len(candidates) > 0:
                raw_counts['num_candidates_scored'] += 1

                torch.cuda.synchronize()
                start_time = time.time()
                # Compute embeddings
                with torch.no_grad(), torch.cuda.amp.autocast():

                    image_features = model.encode_image(image)
                    text_features = model.encode_text(candidates)

                    image_features /= image_features.norm(dim=-1, keepdim=True)
                    text_features /= text_features.norm(dim=-1, keepdim=True)

                    dot_prod = image_features @ text_features.T
                
                    maximum, argmax = dot_prod.max(dim=-1)

                torch.cuda.synchronize()
                end_time = time.time()
                raw_counts['inference_time'] += (end_time - start_time)

                prediction = candidates[argmax.cpu().item()]
                score = maximum.cpu().item()

                if score >= matching_threshold:
                    raw_counts['matches'] += 1

                if enable_wandb:
                    predictions_table_data.append([wandb.Image(image), prediction, score])

                    num_pred_rows = len(predictions_table_data)

                    # wandb recommends logging a table of only 200000 rows
                    if num_pred_rows >= 200000:
                        continue

                if (len(predictions_table_data) % wandb_log_frequency) == 0:

                    if enable_wandb:
                        predictions_table = wandb.Table(columns=predictions_table_cols, data=predictions_table_data)
                        wandb.log({"predictions_table" : predictions_table})

                    print (raw_counts)

            if (idx % log_frequency) == 0:
                print (raw_counts)
                logging.info(raw_counts)

        num_pred_rows = len(predictions_table_data)
        if num_pred_rows <= 200000:

            if enable_wandb:
                predictions_table = wandb.Table(columns=predictions_table_cols, data=predictions_table_data)
                wandb.log({"predictions_table" : predictions_table})

        # Logging for stats 
        stats_table_data = []
        for key, val in raw_counts.items():
            if ("before" in key) or ("after" in key):
                stats_table_data.append([key, val / raw_counts['total_imgs'], val])

        stats_table_data.append(["inference_time", raw_counts["inference_time"] / raw_counts["num_candidates_scored"], raw_counts["inference_time"]])
        stats_table_data.append(["matches", raw_counts["matches"] / raw_counts["num_candidates_scored"], raw_counts["matches"]])
        stats_table_data.append(["total_imgs", 1, raw_counts["total_imgs"]])
        stats_table_data.append(["num_candidates_scored", 1, raw_counts["num_candidates_scored"]])

        if enable_wandb:
            stats_table = wandb.Table(columns=stats_table_cols, data=stats_table_data)
            wandb.log({"stats_table" : stats_table})

if __name__ == "__main__":
    fire.Fire(run_pipeline)