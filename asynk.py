import aiofiles
import aiohttp
import asyncio
import bz2
import datetime
import requests
import struct
import xarray as xr

from pathlib import Path
from time import sleep

BASE_URL = "https://opendata.dwd.de/weather/nwp/icon-d2/grib/12/tot_prec/"
CACHE_DIR = ".cache"

EMPY_VALUE = -100500.0
MODEL = "icon_d2"
MULT = 1000000


def get_queue_of_sorces():
    result = []

    # We assume that links in page order ascending by time
    content = get_content(BASE_URL)
    
    for line in content.split(b"\n"):
        items = line.split(b'\">')[0].split(b'href=\"')
        
        if len(items) != 2:
            continue

        if b"lat-lon" in items[1]:
            result.append(items[1].decode())

    return result


def get_content(path):
    print ("Start getting content from {}".format(path))
    
    content = None
    attempts = 0
    
    while content is None and attempts < 10:
        attempts += 1
        try:
            responce = requests.get(path)
            assert responce.status_code == 200
            content = responce.content
        except Exception as e:
            print(e)
            print("{} try".format(attempts))
            sleep(2)

    print ("Finish getting content from {}".format(path))
    return content


def get_filename(bz2filename):
    return "{}/{}".format(CACHE_DIR, bz2filename[:-4])


async def get_content_async(path):
    print ("Start getting content from {}".format(path))
    content = None
    attempts = 0

    while content is None and attempts < 10:
        attempts += 1
        try:
            async with aiohttp.request('GET', path) as response:
                assert response.status == 200
                content = await response.read()
                response.close()
        except Exception as e:
            print("Get {} try {}".format(path, attempts))
            print(e)
            sleep(2)

    print ("Finish getting content from {}".format(path))
    return content


async def download_dataset(bz2filename):
    filename = get_filename(bz2filename)

    # Try to use file in cache if we download it in the past
    # Or download it right now and save to cache
    if not Path(filename).exists():
        content = await get_content_async(BASE_URL + bz2filename)
        data = bz2.decompress(content)
        f = open(filename, "wb")
        f.write(data)
        f.close()
        print ("Save file {}".format(filename))


def get_dataset(bz2filename):
    result = None
    filename = get_filename(bz2filename)

    dataset = xr.load_dataset(filename, engine="cfgrib")

    if dataset.dims.get('step') is not None:
        # Get first message as value for start hour
        result = dataset.tp[0]
    else:
        # In last file we have only one message
        result = dataset.tp

    return result


def normalize_tuple(*args):
    return [int(item * MULT) for item in args]


def get_time(dataset):
    t = datetime.datetime.utcfromtimestamp(
        (dataset.time.item() + dataset.step.item())/1000000000
    )
    return "{}_{}".format(t.strftime("%d:%m:%Y_%H:%M"), int(t.timestamp()))


def get_header(dataset):
    return struct.pack(
        'i' * 7,
        *normalize_tuple(
            dataset.GRIB_latitudeOfFirstGridPointInDegrees,
            dataset.GRIB_latitudeOfLastGridPointInDegrees,
            dataset.GRIB_longitudeOfFirstGridPointInDegrees,
            dataset.GRIB_longitudeOfLastGridPointInDegrees,
            dataset.GRIB_iDirectionIncrementInDegrees,
            dataset.GRIB_jDirectionIncrementInDegrees,
            1
        )
    )


def write_data(path, header, values):
    print("Start write {}".format(path))
    
    value = header + struct.pack('f', EMPY_VALUE)
    for line in values:
        value += struct.pack('f'*len(line), *line.as_numpy())

    output = open("{}/PRATE.wgf4".format(path), mode="wb")
    output.write(value)
    output.close()

    print("Finish write {}".format(path))


def process(filenames, index):
    print("Start processing {}".format(filenames[index]))

    prev_dataset = get_dataset(filenames[index-1])
    dataset = get_dataset(filenames[index])
    
    header = get_header(dataset)
    path = "./{}/{}".format(MODEL, get_time(dataset))
    Path(path).mkdir(parents=True, exist_ok=True)
    
    write_data(path, header, (dataset - prev_dataset).fillna(EMPY_VALUE))

    print("Finish processing {}".format(filenames[index]))
    print("=" * 100)


async def download_datasets(filenames):
    tasks = []

    for filename in filenames:
        tasks.append(asyncio.create_task(download_dataset(filename)))

    for task in tasks:
        await task


def main():
    Path("./{}".format(MODEL)).mkdir(parents=True, exist_ok=True)
    Path("./{}".format(CACHE_DIR)).mkdir(parents=True, exist_ok=True)
    filenames = get_queue_of_sorces()

    # Download all source files in async-mode and save they into CACHE_DIR
    print("Start downloading")
    asyncio.run(download_datasets(filenames))
    print("Finish downloading")
    print("=" * 100)


    # We assume that filenames in page order ascending by time
    print("Start processing")
    for i in range(1, len(filenames)):
        process(filenames, i)
    print("Finish processing")

if __name__ == '__main__':
    main()
