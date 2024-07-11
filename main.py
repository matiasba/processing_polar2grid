import os
from datetime import datetime  # basic date and time types
import pandas as pd  # python data analysis library
import boto3
from botocore.handlers import disable_signing
import re
from pathlib import Path
import urllib.request
import zipfile

s3 = boto3.resource('s3')
s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

samples_path = './jpss_samples'
shapefiles_path = './shapefiles'
output_file = 'result.tif'
shapefiles_url = 'https://www.soest.hawaii.edu/pwessel/gshhg/gshhg-shp-2.3.7.zip'
bucket_name = "noaa-nesdis-n20-pds"
target_data = "VIIRS-I1-SDR"
target_data_geo = "VIIRS-IMG-GEO-TC"

year = '2024'
month = '02'
day = '20'
start_hour = '17'
start_minute = '05'
end_hour = '17'
end_minute = '10'

start_limiter = datetime(int(year), int(month), int(day), int(start_hour), int(start_minute), 0)
end_limiter = datetime(int(year), int(month), int(day), int(end_hour), int(end_minute), 0)


def list_blobs(bucket, prefix):
    """Lists all the blobs in the bucket."""
    storage = s3.Bucket(bucket)
    blobs = storage.objects.filter(Prefix=prefix)
    results = []
    for blob in blobs:
        results.append(blob.key)
    return results


def parse_dates(key):
    s_date = re.search('_(d.*)_e', key).group(1)
    year = int(s_date[1:5])
    month = int(s_date[5:7])
    day = int(s_date[7:9])
    hour = int(s_date[11:13])
    minute = int(s_date[13:15])
    seconds = int(s_date[15:17])
    string_dt = datetime(year, month, day, hour, minute, seconds)
    return string_dt


def download_blob(bucket, source_blob_name, destination_file_name):
    storage = s3.Bucket(bucket)
    try:
        local_size = Path(f'{destination_file_name}').stat().st_size
    except FileNotFoundError:
        local_size = None
    s3_object = s3.Object(bucket, source_blob_name)
    if local_size != s3_object.content_length:
        storage.download_file(source_blob_name, destination_file_name)
        print(f'Blob {source_blob_name} downloaded to {destination_file_name}')
    else:
        print(f'File {source_blob_name} size matches local copy, skipping...')


def download_data():
    results_data = list_blobs(bucket_name, f"{target_data}/{year}/{month}/{day}")

    dfr = pd.DataFrame(results_data, columns=['Files'])
    dfr['Date'] = dfr.Files.apply(parse_dates)

    lets_get = dfr[(dfr.Date >= start_limiter) & (dfr.Date < end_limiter)].Files.to_list()
    print('Filtered to:', len(lets_get))
    print(lets_get)

    for file in lets_get:
        file_name = file.rsplit('/', 1)[-1]
        download_blob(bucket_name, file, f'{samples_path}/{file_name}')


def download_data_geo():
    results_geo = list_blobs(bucket_name, f"{target_data_geo}/{year}/{month}/{day}/")
    dfr_geo = pd.DataFrame(results_geo, columns=['Files'])
    dfr_geo['Date'] = dfr_geo.Files.apply(parse_dates)

    lets_get_geo = dfr_geo[(dfr_geo.Date >= start_limiter) & (dfr_geo.Date < end_limiter)].Files.to_list()
    print('Filtered to:', len(lets_get_geo))
    print(lets_get_geo)

    for file in lets_get_geo:
        file_name = file.rsplit('/', 1)[-1]
        download_blob(bucket_name, file, f'{samples_path}/{file_name}')


def update_shapefiles():
    try:
        shapefile_info = urllib.request.urlopen(shapefiles_url).info().get('Content-Length', 0)
    except:
        shapefile_info = None
    try:
        local_size = Path(f"{shapefiles_path}/{os.path.basename(shapefiles_url)}").stat().st_size
    except FileNotFoundError:
        local_size = None
    if local_size is None or (int(shapefile_info) != local_size):
        print(f"Going to download {shapefiles_url}")
        urllib.request.urlretrieve(shapefiles_url, f"{shapefiles_path}/{os.path.basename(shapefiles_url)}")
        print("Shapefiles downloaded, unziping...")
        with zipfile.ZipFile(f"{shapefiles_path}/{os.path.basename(shapefiles_url)}", 'r') as zip_ref:
            zip_ref.extractall(f"{shapefiles_path}/")
        print("Shapefiles unzip completed")
    else:
        print("Shapefiles up to date")


download_data()
download_data_geo()
update_shapefiles()

os.environ["USE_POLAR2GRID_DEFAULTS"] = "1"

from polar2grid.glue import main as polar2grid

polar2grid_args = ["-r", "viirs_sdr", "-w", "geotiff", "--output-filename", str(output_file), "-vvv", "-p", "i01", "-f", str(samples_path)]
polar2grid(argv=polar2grid_args)

#from polar2grid.add_coastlines import main as add_costlines
#add_costlines_args = ["--shapes-dir", str(shapefiles_path), "--add-coastlines", "--add-grid", "--grid-D", "10.0", "10.0", "--grid-d", "10.0", "10.0", "--grid-text-size", "20", str(output_file)]
#add_costlines(argv=add_costlines_args)

