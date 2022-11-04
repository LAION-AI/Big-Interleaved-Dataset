import boto3
def downls(wurl):
    s3client = boto3.client('s3', use_ssl=False)
    with open("main.gz",'wb') as data:
        s3client.download_fileobj(
        'commoncrawl',
        wurl,
        data
        )

wurl="crawl-data/CC-MAIN-2022-33/segments/1659882570651.49/warc/CC-MAIN-20220807150925-20220807180925-00000.warc.gz"

downls(wurl)