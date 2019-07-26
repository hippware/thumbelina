import boto3
import logging
import os
import psycopg2
import string
import urllib

from subprocess import Popen, PIPE

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
targetBucket = None
body = None
response = None
contentType = None

def lambda_handler(event, context):
    global targetBucket, body, response, contentType

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    try:
        logger.info('Fetching image')
        response = s3.get_object(Bucket=bucket, Key=key)
        targetBucket = string.replace(bucket, "-quarantine", "")
        body = response['Body'].read()
        contentType = response['ContentType']
        cmdHead = [
                    'convert',  # ImageMagick Convert
                    '-',        # Read original picture from StdIn
                    '-strip'    # Remove metadata
                  ]
        cmdTail = [
                    output_format(contentType) + ':-' # Write output with to StdOut
                  ]

        # Make full size clean image
        logger.info('Creating clean image')
        cmd = cmdHead + limit_size() + cmdTail
        p = Popen(cmd, stdout=PIPE, stdin=PIPE)
        cleanImage = p.communicate(input=body)[0]

        logger.info('Uploading clean image')
        upload_to_s3(key, cleanImage)

        # Make thumbnail
        logger.info('Creating thumbnail')
        cmd = cmdHead + thumbnail_params() + cmdTail
        p = Popen(cmd, stdout=PIPE, stdin=PIPE)
        thumbnailImage = p.communicate(input=body)[0]

        logger.info('Uploading thumbnail')
        upload_to_s3(key + "-thumbnail", thumbnailImage)

        # Make aspect-ratio-correct thumbnail
        logger.info('Creating aspect-ratio-correct thumbnail')
        cmd = cmdHead + aspect_thumbnail_params() + cmdTail
        p = Popen(cmd, stdout=PIPE, stdin=PIPE)
        aspectThumbnailImage = p.communicate(input=body)[0]

        logger.info('Uploading aspect-ratio-correct thumbnail')
        upload_to_s3(key + "-aspect_thumbnail", aspectThumbnailImage)

        # Copy over the original image (in case we want to reprocess it in the
        # future)
        logger.info('Uploading original')
        upload_to_s3(key + "-original", body)

        # Update the database to mark the image as processed
        mark_processed(key)

        # Clean up source object
        logger.info('Deleting quarantine object')
        s3.delete_object(Bucket = bucket,
                         Key = key)

        logger.info('Finished')

    except Exception as e:
        logger.error(e)
        logger.error('Error processing object {} from bucket {}.'.format(key, bucket))
        raise e

def output_format(contentType):
    if contentType == "image/png":
        return "png"
    elif contentType == "image/jpeg":
        return "jpeg"
    else:
        raise Exception('Unhandled content type: {}'.contentType)

def limit_size():
    # Max size of an iPhone 7+'s largest axis - do not grow
    return ['-resize', '1920x1920>']

def thumbnail_params():
    # 1/3 the width of iPhone7+, in a square, cropped
    thumb_size = '360x360'
    return ['-thumbnail', thumb_size + "^",
            '-gravity', 'center',
            '-extent', thumb_size]

def aspect_thumbnail_params():
    # As for thumbnails, but maintain aspect ratio and do not crop
    return ['-thumbnail', "360x360"]

def upload_to_s3(key, body):
    s3.put_object(Bucket = targetBucket,
                  Key = key,
                  Body = body,
                  Metadata = response['Metadata'],
                  ContentType = contentType)

def mark_processed(key):
    id = key.split('/')[-1] # ID is the part after the last '/'
    logger.info('Connecting to DB')
    conn = psycopg2.connect(conn_string())
    logger.info('Connected to DB')
    cursor = conn.cursor()
    logger.info('Executing UPDATE')
    cursor.execute(
        "UPDATE tros_metadatas SET ready = true, available_formats = ARRAY['full', 'original', 'thumbnail', 'aspect_thumbnail']::file_type[] WHERE id = %s", [id])
    logger.info('COMMITing')
    conn.commit()
    cursor.close()
    logger.info('Closing connection')
    conn.close()
    logger.info('Finished DB update')

def conn_string():
    return "dbname=" + os.getenv('DB_NAME') + " user=" + os.getenv('DB_USER') + " password=" + os.getenv('DB_PASSWORD') + " host=" + os.getenv('DB_HOST')
