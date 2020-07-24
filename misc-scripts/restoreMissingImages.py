import imutils
import cv2
from pgdb import connect
import boto3
import os
from dotenv import load_dotenv
import datetime
import copy
import time
import uuid
import sys
import math
import pandas as pd
from multiprocessing import Pool

# Load environment variables
load_dotenv(dotenv_path="../../video-annotation-tool/.env")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv('AWS_S3_BUCKET_NAME')
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
S3_ANNOTATION_FOLDER = os.getenv("AWS_S3_BUCKET_ANNOTATIONS_FOLDER")
S3_VIDEO_FOLDER = os.getenv('AWS_S3_BUCKET_VIDEOS_FOLDER')
S3_TRACKING_FOLDER = os.getenv("AWS_S3_BUCKET_TRACKING_FOLDER")

# connect to db
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def getCapture(filename):
    # grab video stream
    url = s3.generate_presigned_url('get_object',
                                    Params={'Bucket': S3_BUCKET,
                                            'Key': S3_VIDEO_FOLDER + filename},
                                    ExpiresIn=86400)
    capture = cv2.VideoCapture(url)
    return capture


def getVideoFrame(capture, frame_num):
    capture.set(1, frame_num)
    check, frame = capture.retrieve()
    if (check is None or not check):
        return None
    return frame


def upload_image(frame, image):
    temp_file = str(uuid.uuid4()) + ".png"
    cv2.imwrite(temp_file, frame)
    s3.upload_file(temp_file, S3_BUCKET, S3_ANNOTATION_FOLDER +
                   image, ExtraArgs={'ContentType': 'image/png'})
    os.system('rm ' + temp_file)
    return


def getAllImages(filename, rows):
    print(f'Working on {filename}')
    capture = getCapture(filename)
    if capture is None:
        print('Capture is broken')
        return
    length = rows.shape[0]
    for index, (_, row) in enumerate(rows.iterrows()):
        print(f'{filename} at {round(100*(index/length), 1)}%')
        frame_num = row.framenum
        if (pd.isna(frame_num)):
            frame_num = row.timeinvideo * 29.97002997003
        frame = getVideoFrame(capture, round(frame_num))
        if (frame is None):
            print(f'Something went wrong with annotation: {row.id}')
            print(row)
            continue
        upload_image(frame, row.image)
    capture.release()
    return


if __name__ == "__main__":
    missingImages = pd.read_csv(sys.argv[1])
    with Pool() as p:
        p.starmap(getAllImages, map(
            lambda x: x, missingImages.groupby('filename')))
