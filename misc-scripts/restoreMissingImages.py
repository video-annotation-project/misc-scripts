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
load_dotenv(dotenv_path="../video-annotation-tool/.env")
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

# Uploads images


def upload_image(frame, image):
    temp_file = str(uuid.uuid4()) + ".png"
    cv2.imwrite(temp_file, frame)
    s3.upload_file(temp_file, S3_BUCKET, S3_ANNOTATION_FOLDER +
                   image, ExtraArgs={'ContentType': 'image/png'})
    os.system('rm ' + temp_file)
    return


def openVideo(filename):
    # grab streaming url
    url = s3.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': S3_BUCKET,
            'Key': S3_VIDEO_FOLDER + filename
        },
        ExpiresIn=86400
    )
    cap = cv2.VideoCapture(url)
    return cap


def getFrame(cap, frame_num, videowidth, videoheight):
    cap.set(1, round(frame_num))
    check, frame = cap.read()
    if (check is None or not check):
        print("Error in cap.read()")
        return
    frame = cv2.resize(frame, (videowidth, videoheight))
    return frame


def getAllImages(filename, rows):
    print(f'Working on {filename}')
    cap = openVideo(filename)
    length = values.shape[0]
    for index, row in rows.iterrows():
        print(f'{filename} is at {round(index/length, 3)}%')
        frame_num = row.frame_num
        if (pd.isna(frame_num)):
            frame_num = row.timeinvideo * 29.97002997002997
        frame = getFrame(cap, round(frame_num), round(
            row.videowidth), round(row.videoheight))
        if (frame is None):
            print(f'Failed on annotation: {row.id}')
            continue
        upload_image(frame, row.image)
    cap.release()
    cv2.destroyAllWindows()
    return


if __name__ == "__main__":
    missingImages = pd.read_csv('missingImages420.csv')
    with Pool() as p:
        p.starmap(getAllImages, map(
            lambda x: x, missingImages.groupby('filename')))
