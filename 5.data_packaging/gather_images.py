"""
Python module for gathering images from Image Data Resource (IDR).
"""

import pathlib
from functools import partial

import pyarrow.compute as pc
from joblib import Parallel, delayed
from packager.images import export_frame_data_to_parquet, get_image_union_table

# specify an image download dir and create it
image_download_dir = "./5.data_packaging/images/extracted_frame"
pathlib.Path(image_download_dir).mkdir(parents=True, exist_ok=True)

# specify an export dir and create it
export_dir = "./5.data_packaging/location_and_ch5_frame_image_data"
pathlib.Path(export_dir).mkdir(parents=True, exist_ok=True)

# get a table of image-relevant data
image_union_table = get_image_union_table()

export_frame_data_to_parquet_with_defaults = partial(
    export_frame_data_to_parquet,
    image_union_table=image_union_table,
    export_dir=export_dir,
    image_download_dir=image_download_dir,
)

# iterate through location union data in parallel
results = Parallel(n_jobs=5)(
    delayed(export_frame_data_to_parquet_with_defaults)(unique_file)
    for unique_file in pc.unique(image_union_table["IDR_FTP_ch5_location"]).to_pylist()
)

print(f"Created {len(results)} parquet files with image data.")
