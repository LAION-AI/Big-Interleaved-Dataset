import os
import shutil
from img2dataset import download
from utils import convert_to_image_url_text_parquet

def main():
    
    filename = "../../../00000.parquet"
    convert = True
    download_imgs = True

    if convert:
        converted_filename = convert_to_image_url_text_parquet(filename)
    else:
        converted_filename = filename

    if download_imgs:
        output_dir = os.path.abspath("output")
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
            enable_wandb=False,
            number_sample_per_shard=1000,
            distributor="multiprocessing",
        )

if __name__ == "__main__":
    main()