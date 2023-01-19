import logging

import gzip
import time
from fastwarc.warc import ArchiveIterator
from pathlib import Path

import pandas as pd
from docarray import DocumentArray as DA

from .extraction_utils import parser_bytes


def pipeline(warcf, wurl, config):
    warc_stats = {
        "warc_img_count": 0,
        "warc_vid_count": 0,
        "warc_aud_count": 0,
        "warc_iframe_count": 0,
        "warc_html_hits": 0,
        "warc_exception_counts": 0,
    }
    st = time.time()
    warchtml, warcimg, warcaud, warcvid, warciframe = (
        list(),
        list(),
        list(),
        list(),
        list(),
    )

    logging.basicConfig(
        filename=f"{config['log_store']}/{wurl.split('.')[1].split('/')[-1][-5:]}.log",
        level=logging.DEBUG,
        filemode="w",
        format="%(process)d:%(asctime)s:%(levelname)s:%(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
    )
    for index_r, record in enumerate(
        ArchiveIterator(gzip.open(warcf), max_content_length=4 * 1024**2)
    ):

        try:
            if record.headers is None:
                continue
            if record.http_headers is None:
                continue
            if (
                record.headers["WARC-Type"] == "response"
                and record.content_length >= 128
            ):
                content_type = str(record.http_content_type).lower()
                if content_type.startswith("text/html"):
                    warc_stats["warc_html_hits"] += 1
                    url = str(record.headers["WARC-Target-URI"])
                    html_bytes = record.reader.read()

                    # we get a bunch of dataframes and bildrecord and it's config
                    (
                        page_config,
                        bildrecord,
                        imgs,
                        vids,
                        auds,
                        iframedict,
                    ) = parser_bytes(url, html_bytes)

                    # we update the warc stats
                    warc_stats["warc_img_count"] += page_config["img_count"]
                    warc_stats["warc_vid_count"] += page_config["vid_count"]
                    warc_stats["warc_aud_count"] += page_config["aud_count"]
                    warc_stats["warc_iframe_count"] += page_config["iframe_count"]

                    logging.debug(
                        f'Sucessflly parsed record index:{warc_stats["warc_html_hits"]}'
                    )

                    warchtml.append([url, page_config, DA(bildrecord).to_json()])
                    warcimg += imgs
                    warcvid += vids
                    warcaud += auds
                    warciframe += iframedict

        except Exception as er:
            warc_stats["warc_exception_counts"] += 1
            logging.debug(
                f"An exception occured at index {warc_stats['warc_html_hits']}: Total exceptions: {warc_stats['warc_exception_counts']}"
            )
            logging.debug(er)

    ster = time.time()
    warc_htmldf = pd.DataFrame(warchtml, columns=["URL", "Page_config", "bildrecord"])
    warc_htmldf.to_parquet(
        f"{config['Extraction_store']}/{wurl.split('.')[1].split('/')[-1][-5:]}.parquet"
    )

    warc_imgdf = pd.DataFrame(
        warcimg, columns=["id", "md5hash", "file_extension", "url"]
    )
    warc_imgdf.to_parquet(
        f"{config['Imgstore']}/{wurl.split('.')[1].split('/')[-1][-5:]}imgs.parquet"
    )

    warc_viddf = pd.DataFrame(
        warcvid, columns=["id", "md5hash", "file_extension", "url"]
    )
    warc_viddf.to_parquet(
        f"{config['Vidstore']}/{wurl.split('.')[1].split('/')[-1][-5:]}vids.parquet"
    )

    warc_auddf = pd.DataFrame(
        warcaud, columns=["id", "md5hash", "file_extension", "url"]
    )
    warc_auddf.to_parquet(
        f"{config['Audstore']}/{wurl.split('.')[1].split('/')[-1][-5:]}auds.parquet"
    )

    warc_iframedf = pd.DataFrame(
        warciframe, columns=["id", "md5hash", "file_extension", "url"]
    )
    warc_iframedf.to_parquet(
        f"{config['Iframestore']}/{wurl.split('.')[1].split('/')[-1][-5:]}iframes.parquet"
    )

    df_stats = pd.DataFrame.from_dict([warc_stats]).to_json(
        f"{config['Stats_store']}/{wurl.split('.')[1].split('/')[-1][-5:]}_stats.json"
    )

    logging.info(f"Time taken:{time.time()-st}s")
