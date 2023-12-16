import bz2
import datetime
import requests
import struct
import xarray as xr

from collections import deque
from pathlib import Path
from time import sleep

BASE_URL = "https://opendata.dwd.de/weather/nwp/icon-d2/grib/12/tot_prec/"
CACHE_DIR = ".cache"

EMPY_VALUE = -100500.0
MODEL = "icon_d2"
MULT = 1000000


def get_queue_of_sorces():
    result = deque()

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


def get_dataset(bz2filename):
    filename = "{}/{}".format(CACHE_DIR, bz2filename[:-4])
    # Try to use file in cache if we download it in the past
    # Or download it right now and save to cache
    if not Path(filename).exists():
        content = get_content(BASE_URL + bz2filename)
        data = bz2.decompress(content)
        f = open(filename, "wb")
        f.write(data)
        f.close()
        print ("Save file {}".format(filename))

    # Get first message as value for start hour
    dataset = xr.load_dataset(filename, engine="cfgrib")

    result = dataset.tp[0]
    if dataset.dims.get('step') is None:
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


def get_values(dataset, prev_values=None):
    result = dataset
    if prev_values is not None:
        result -= prev_values

    return result.fillna(EMPY_VALUE), dataset


def write_data(path, header, values):
    print("Start write {}".format(path))

    output = open("{}/PRATE.wgf4".format(path), "wb")
    output.write(header)
    output.write(struct.pack('f', EMPY_VALUE))
    for line in values:
        output.write(struct.pack('f'*len(line), *line.as_numpy()))
    output.close()

    print("Finish write {}".format(path))


def main():
    Path("./{}".format(MODEL)).mkdir(parents=True, exist_ok=True)
    Path("./{}".format(CACHE_DIR)).mkdir(parents=True, exist_ok=True)
    filenames = get_queue_of_sorces()

    # Just get value at 0 time as previus values
    prev_values, _ = get_values(get_dataset(filenames[0]))
    filenames.popleft()

    # We assume that filenames in page order ascending by time
    while filenames:
        current = filenames[0]
        print("Start processing {}".format(current))
    
        try:
            dataset = get_dataset(current)
            header = get_header(dataset)
            path = "./{}/{}".format(MODEL, get_time(dataset))
            Path(path).mkdir(parents=True, exist_ok=True)
            result, prev_values = get_values(dataset, prev_values)
            write_data(path, header, result)
            filenames.popleft()
        # DoTo: specify exceptions
        except Exception as e:
            print(e)
    
        print("Finish processing {}".format(current))
        print("=" * 100)

if __name__ == '__main__':
    main()
