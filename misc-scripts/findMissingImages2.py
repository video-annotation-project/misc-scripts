
import datetime
import psycopg2  # Postgresql
import pandas as pd  # Ez table management
from dotenv import load_dotenv  # Load .env strings
import os  # console commands
import boto3  # AWS querier

# Note: .env file should be in the top-level directory of the repo
load_dotenv(dotenv_path="../../video-annotation-tool/.env")
S3_BUCKET = os.getenv('AWS_S3_BUCKET_NAME')
SRC_IMG_FOLDER = 'annotation_frames/' 
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
A_KEY = os.getenv("AWS_ACCESS_KEY_ID")
A_SEC = os.getenv("AWS_SECRET_ACCESS_KEY")

# Connect to database using .env variables


def queryDB(query, params=None):
    conn = psycopg2.connect(database=DB_NAME,
                            user=DB_USER,
                            password=DB_PASSWORD,
                            host=DB_HOST,
                            port="5432")
    # Use pandas to read queries into a dataframe
    result = pd.read_sql_query(query, conn, params=params)
    conn.close()  # Close postgresql
    return result


# # Create csv of image names in lubo's s3 bucket
current_time = datetime.datetime.now().strftime("%b %d %H:%M")
# Get all the image filename
'''
with open(f'./csv/s3Images{current_time}.csv', 'a') as fd:
    # Connect to AWS
    session = boto3.Session(aws_access_key_id=A_KEY,
                            aws_secret_access_key=A_SEC)
    # Access simple storage and s3 bucket
    s3 = session.resource('s3')
    client = boto3.client('s3')
    bucket = s3.Bucket("lubomirstanchev")
    # Iterate through bucket folder test
    for index, obj in enumerate(bucket.objects.filter(Delimiter='/', Prefix='annotation_frames/')):
        if index >= 1:
            response = client.head_object(Bucket='lubomirstanchev', Key=obj.key)
            size = response['ContentLength']
            if size == 0:
                # Make csv of images in s3 bucket
                fd.write(obj.key + '\n')
'''

annotation_rows = queryDB('''
    Select
        DISTINCT ON (annotations.image)
        annotations.*, videos.filename
    FROM
        annotations
    LEFT JOIN
        videos
    ON
        videos.id=videoid
    WHERE
        videoid = ANY(ARRAY[11, 12, 14, 18, 19, 20, 21, 24, 25, 27, 28, 30, 31, 32, 37, 39, 49, 50, 53, 54, 55, 63, 64, 65, 66, 67, 70, 76, 77, 78, 86, 87, 88, 89, 91, 96, 97, 98, 99, 102, 106, 107, 109, 110, 129, 130, 131, 133, 134, 150, 163])
''')


# # Read in s3 image names into memory
'''
s3Images = pd.read_csv(f"./csv/s3Images{current_time}.csv", header=None)
# Remove extra path info in s3 images names
s3Images[0] = s3Images[0].str.split('/', expand=True)[1]
s3Images = s3Images[0]
'''

# # Remove images that exist in the s3 bucket
'''
annotation_rows_no_s3img = annotation_rows[(
    ~annotation_rows.image.isin(s3Images))]


# # Number of images that are missing from the s3 bucket
print(f"There are {annotation_rows_no_s3img.shape[0]} missing images")
'''

annotation_rows.to_csv(
    f"./csv/annotations_no_s3img {current_time}.csv", index=False)
