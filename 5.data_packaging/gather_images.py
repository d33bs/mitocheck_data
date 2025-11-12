"""
Python module for gathering images from Image Data Resource (IDR).
"""

import os
import pathlib
import warnings
from ftplib import FTP
from typing import List
import time
import imageio.v3 as iio
from datetime import datetime, timezone
from serpula_rasa.image import make_ome_arrow_row
from serpula_rasa.meta import OME_ARROW_SCHEMA
import subprocess
import docker
import duckdb
import h5py
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pybasic
import skimage
from constants import (
    DOCKER_PLATFORM,
    FTP_IDR_MITOCHECK_CH5_DIR,
    FTP_IDR_URL,
    FTP_IDR_USER,
)
from pyarrow import parquet

OME_STRUCT_TYPE = OME_ARROW_SCHEMA.field(0).type

def to_float01(img: np.ndarray) -> np.ndarray:
    if img.dtype in (np.float32, np.float64):
        return np.clip(img.astype(np.float32, copy=False), 0.0, 1.0)
    img = img.astype(np.float32, copy=False)
    maxv = float(np.iinfo(img.dtype).max) if np.issubdtype(img.dtype, np.integer) else float(img.max() or 1.0)
    if maxv != 0:
        img /= maxv
    return np.clip(img, 0.0, 1.0)

def retrieve_ftp_file(
    ftp_file: str,
    download_dir: str,
    ftp_url: str = FTP_IDR_URL,
    ftp_user: str = FTP_IDR_USER,
    ftp_pass: str = "",
    *,
    retries: int = 2,
    timeout: int = 60,
) -> str:
    """
    Retrieve a file using FTP.

    Args:
        ftp_file (str):
            The name of the file to retrieve.
        download_dir (str):
            The directory where the file will be downloaded.
        ftp_url (str, optional):
            The URL of the FTP server. Defaults to FTP_IDR_URL.
        ftp_user (str, optional):
            The username for accessing the FTP server. Defaults to FTP_IDR_USER.
        ftp_pass (str, optional):
            The password for accessing the FTP server. Defaults to "".

    Returns:
        str:
            A string indicating the path to the downloaded file.
    """
    download_dir_path = pathlib.Path(download_dir)
    download_dir_path.mkdir(parents=True, exist_ok=True)
    download_filepath = download_dir_path / pathlib.Path(ftp_file).name

    if download_filepath.is_file() and download_filepath.stat().st_size > 0:
        return str(download_filepath)

    last_err = None
    for attempt in range(retries + 1):
        try:
            with FTP(ftp_url, timeout=timeout) as ftp:
                ftp.login(user=ftp_user, passwd=ftp_pass)

                # Some servers prefer RETR with cwd; try full path first, then cwd+RETR basename
                try:
                    with open(download_filepath, "wb") as local_file:
                        ftp.retrbinary(f"RETR {ftp_file}", local_file.write)
                except Exception:
                    # fallback: cwd then retr basename
                    parent = str(pathlib.Path(ftp_file).parent).lstrip("/")
                    basename = pathlib.Path(ftp_file).name
                    if parent and parent != ".":
                        ftp.cwd(parent)
                    with open(download_filepath, "wb") as local_file:
                        ftp.retrbinary(f"RETR {basename}", local_file.write)

            # verify we really got it
            if download_filepath.is_file() and download_filepath.stat().st_size > 0:
                return str(download_filepath)

            last_err = RuntimeError("Downloaded file is empty")
        except Exception as e:
            last_err = e

    # clean up a zero-byte stub if created
    try:
        if download_filepath.exists() and download_filepath.stat().st_size == 0:
            download_filepath.unlink()
    except Exception:
        pass

    raise FileNotFoundError(
        f"Failed to download {ftp_file} from {ftp_url} → {download_filepath} "
        f"after {retries+1} attempt(s): {last_err}"
    )


def get_image_union_table() -> pa.Table:
    """
    Build a table with relevant information to extract images.

    Returns:
        pa.Table:
            A PyArrow table containing relevant information for image extraction.
    """

    with duckdb.connect() as ddb:
        return ddb.execute(
            f"""
            /* concat all locations data together as a single table */
            WITH locations_union AS (
                SELECT *
                FROM read_csv('0.locate_data/locations/negative_control_locations.tsv')
                UNION ALL BY NAME
                SELECT *
                FROM read_csv('0.locate_data/locations/positive_control_locations.tsv')
                UNION ALL BY NAME
                SELECT *
                FROM read_csv('0.locate_data/locations/training_locations.tsv')
            )
            /* join locations with additional plate location data */
            SELECT
                locations_union.*,
                /* create a dotted notation filename for extracting image frames from ch5 files */
                replace(locations_union.DNA, '/', '.') AS DNA_dotted_notation,
                /* clean up screen location string for IDR FTP location work below */
                replace(
                    replace(plates.Screen, '../screens/', ''),
                    '.screen',
                    ''
                    ) AS Screen_cleaned,
                /* build an IDR path from other data based on IDR_Stream:
                https://github.com/WayScience/IDR_stream/blob/main/idrstream/download.py#L95 */
                concat(
                    '{FTP_IDR_MITOCHECK_CH5_DIR}',
                    '/',
                    Screen_cleaned,
                    '/hdf5/00',
                    format('{{:03d}}', locations_union."Well Number"),
                    '_01.ch5'
                ) AS IDR_FTP_ch5_location,
                'TARGET_FRAME' as Frame_type
            FROM locations_union
            LEFT JOIN read_csv('1.idr_streams/stream_files/idr0013-screenA-plates-w-colnames.tsv') as plates ON
                    plates.Plate = locations_union.Plate
            """
        ).arrow()


def run_dockerfile_container(
    dockerfile: str,
    image_name: str,
    volumes: List[str],
    command: str,
) -> None:
    """
    Build, run, and stream logs from a Dockerfile-based container.

    Args:
        dockerfile (str):
            The path to the Dockerfile.
        image_name (str):
            The name of the Docker image to build.
        volumes (List[str]):
            List of volume mounts for the container.
        command (str):
            The command to execute inside the container.

    Returns:
        None
    """

    print(
        f"Running Docker container {image_name} based on {dockerfile} with command '{command}'."
    )

    # Initialize the Docker client
    client = docker.from_env()

    # Build the Docker image using the Dockerfile
    # Check if the image already exists
    check = subprocess.run(
        ["docker", "image", "inspect", image_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    if check.returncode == 0:
        print(f"✅ Image '{image_name}' already exists — skipping build.")
    else:
        print(f"🛠️  Image '{image_name}' not found — building now...")
        subprocess.run(
            [
                "docker", "buildx", "build",
                "--platform", DOCKER_PLATFORM,
                "--load",
                "--pull",
                "--no-cache",
                "-t", image_name,
                "-f", pathlib.Path(dockerfile).name,
                ".",
            ],
            check=True,
            cwd=str(pathlib.Path(dockerfile).parent),
        )

    # Run a container based on the built image, mounting a local directory
    container = client.containers.run(
        image=image_name,
        volumes=volumes,
        command=command,
        remove=True,
        detach=True,
    )

    # capture log messages from detached container
    process = container.logs(stream=True, follow=True)

    # print the lines of output from the container as it runs
    for line in process:
        print(line.decode("utf-8").strip())


def find_frame_len(ch5_file: str):
    """
    Find the length of the 'time_lapse' dataset within an HDF5 file.

    This function recursively searches for a dataset named 'time_lapse' within
    the provided HDF5 (or HDF5-like) file. Once found, it returns the size of
    the dataset, which represents the number of frames.

    Args:
        ch5_file (str): The path to the HDF5 file to be searched.

    Returns:
        int: The length of the 'time_lapse' dataset (number of frames).

    Example:
        >>> find_frame_len('example.h5')
        100

    Note:
        This function returns None if the 'time_lapse' dataset is not found
        within the HDF5 file.
    """

    def find_time_lapse_len(name, obj):
        """
        Recursively search for a dataset named "time_lapse"
        within an HDF5 (or HDF5-like) file.

        Return the dataset object when found.
        """

        if isinstance(obj, h5py.Dataset):
            if "sample" in name and "time_lapse" in name:
                print("Found 'time_lapse' dataset at:", obj.name)
                # Return the dataset object
                return obj.size

        elif isinstance(obj, h5py.Group):
            # Traverse the group's children recursively
            for key, value in obj.items():
                # Recursively search within the group
                result = find_time_lapse_len(name, value)
                if result is not None:
                    # If "time_lapse" is found in a subgroup, return the result
                    return result

    # Open the HDF5 file in read-only mode
    with h5py.File(ch5_file, "r") as f:
        # Start the search from the root group
        return f.visititems(find_time_lapse_len)


def get_frame_tiff_from_idr_ch5(
    frame: str, local_ch5_file: str, local_frame_tif: str
) -> str:
    """
    Gather IDR ch5 file and extract a frame as a TIFF, returning the local filepath
    and cleaning up the ch5 afterwards.

    Args:
        frame (str):
            The frame number to extract from the IDR ch5 file.
        local_ch5_file (str):
            The local filepath where the ch5 file may be referenced.
        local_frame_tif (str):
            The local filepath where the extracted TIFF will be saved.

    Returns:
        str:
            The local filepath of the extracted TIFF image.
    """

    print(
        "Working on",
        f"frame: {frame}",
        f"tiff: {local_frame_tif}",
    )

    # if we don't already have a file, create it
    if not pathlib.Path(local_frame_tif).is_file():
        # extract a frame from the ch5 file using bfconvert through a docker container
        run_dockerfile_container(
            dockerfile="./5.data_packaging/Dockerfile.bfconvert",
            image_name="ome_bfconvert",
            volumes=[f"{os.getcwd()}:/app"],
            command=(
                "-z 0 "
                f"-timepoint {frame} {local_ch5_file} {str(local_frame_tif)}"
                " -overwrite"
            ),
        )

    return local_frame_tif


# modified from:
# https://github.com/WayScience/IDR_stream/blob/main/idrstream/preprocess.py#L194C1-L227C76
def get_ic_context_frames(target_frame: int, movie_len: int) -> List[int]:
    """
    Gather additional non-target frames for use with PyBasic IC.

    This function returns a list of three frames: one frame before the target frame,
    the target frame itself, and one frame after the target frame. The frames are
    0-indexed, while the movie length is not. If the target frame is the first frame
    (0), it returns the target frame and the next two frames. If the target frame is
    the last frame, it returns the target frame and the two preceding frames.

    Args:
        target_frame (int):
            The index of the target frame (0-indexed).
        movie_len (int):
            The length of the movie (1-indexed).

    Returns:
        List[int]:
            A list of three frame indices for context.

    Example:
        >>> get_ic_context_frames(2, 5)
        [1, 2, 3]

        >>> get_ic_context_frames(0, 5)
        [0, 1, 2]

        >>> get_ic_context_frames(4, 5)
        [2, 3, 4]
    """

    # "sandwich" the frames using one frame before and one frame after
    # the target frame provided from frame_num.
    # note: we zero index the movie length here for comparisons.
    if target_frame + 1 <= movie_len - 1:
        return [target_frame - 1, target_frame, target_frame + 1]

    # else if we have the first frame, use two frames after
    elif target_frame == 0:
        return [target_frame, target_frame + 1, target_frame + 2]

    # otherwise we have the last frame, so use two frames prior
    else:
        return [target_frame - 2, target_frame - 1, target_frame]


# referenced with modifications
# from: https://github.com/WayScience/IDR_stream/blob/main/idrstream/preprocess.py#L114
def pybasic_IC_target_frame_to_tiff(
    frames_as_arrays: List[np.ndarray], target_frame: int, destination_filename: str
) -> str:
    """
    PyBaSiC Illumination correction as described in:
    http://www.nature.com/articles/ncomms14836

    Parameters
    ----------
    frames_as_arrays : List[np.ndarray]
        array of frames to perform illumination correction on
    target_frame : int
        target frame within the context of the frames_as_arrays
        to return.
    destination_filename : str
        export the target_frame to a destination filepath specified
        through this parameter.

    Returns
    -------
    str
        filepath with the IC image as tiff
    """
    print(f"[INFO] Starting PyBaSiC illumination correction")
    print(f"       Number of frames provided: {len(frames_as_arrays)}")
    print(f"       Target frame index: {target_frame}")
    print(f"       Destination file: {destination_filename}")
    start_time = time.time()

    # capture pybasic warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        print("[STEP] Estimating flatfield and darkfield components...")
        flatfield, darkfield = pybasic.basic(
            frames_as_arrays, darkfield=True, verbosity=False
        )
        print(f"       Flatfield shape: {np.shape(flatfield)}, Darkfield shape: {np.shape(darkfield)}")

        print("[STEP] Estimating background timelapse...")
        baseflour = pybasic.background_timelapse(
            images_list=frames_as_arrays,
            flatfield=flatfield,
            darkfield=darkfield,
            verbosity=False,
        )

        print("[STEP] Correcting illumination across frames...")
        brightfield_images_corrected_original = pybasic.correct_illumination(
            images_list=frames_as_arrays,
            flatfield=flatfield,
            darkfield=darkfield,
            background_timelapse=baseflour,
        )

        print("[STEP] Normalizing corrected images...")
        brightfield_images_corrected = np.array(brightfield_images_corrected_original)
        print(f"       Corrected movie shape: {brightfield_images_corrected.shape}")

        # Ensure no negative values
        negatives = np.sum(brightfield_images_corrected < 0)
        if negatives > 0:
            print(f"       Found {negatives} negative pixels — setting them to zero.")
        brightfield_images_corrected[brightfield_images_corrected < 0] = 0

        # Normalize and scale to uint8
        max_val = np.max(brightfield_images_corrected)
        print(f"       Maximum pixel value before normalization: {max_val}")
        brightfield_images_corrected = brightfield_images_corrected / max_val
        brightfield_images_corrected *= 255
        corrected_movie = brightfield_images_corrected.astype(np.uint8)

        print(f"[STEP] Saving corrected frame {target_frame} to {destination_filename}")
        skimage.io.imsave(fname=destination_filename, arr=corrected_movie[target_frame])

    elapsed = time.time() - start_time
    print(f"[DONE] Illumination correction completed in {elapsed:.2f} seconds.")
    print(f"       Saved corrected frame to {destination_filename}\n")

    return destination_filename


def read_image_as_binary(image_path: str) -> bytes:
    """
    Reads an image file and returns its content as binary data.
    We read image data files as bytearrays in order to retain their
    full representation within the data package without data loss or
    transformation.

    Args:
        image_path (str): The path to the image file to be read.

    Returns:
        bytes: The binary content of the image file.

    Example:
        binary_data = read_image_as_binary("path/to/image.jpg")
        print(binary_data)
    """
    with open(image_path, "rb") as f:
        return f.read()


# specify an image download dir and create it
image_download_dir = "./5.data_packaging/images/extracted_frame"
pathlib.Path(image_download_dir).mkdir(parents=True, exist_ok=True)

# specify an export dir and create it
export_dir = "./5.data_packaging/location_and_ch5_frame_image_data"
pathlib.Path(export_dir).mkdir(parents=True, exist_ok=True)

# get a table of image-relevant data
table = get_image_union_table()

# iterate through location union data
for unique_file in pc.unique(table["IDR_FTP_ch5_location"]).to_pylist():

    # download the ch5 file
    if not pathlib.Path((filename := f"{image_download_dir}/{pathlib.Path(unique_file).name}")).is_file():
        local_ch5_file = retrieve_ftp_file(
            ftp_file=unique_file, download_dir=image_download_dir
        )
    else:
        local_ch5_file = filename

    # find the movie length
    movie_length = find_frame_len(ch5_file=local_ch5_file)

    # reference rows with the same ch5 file
    for batch in table.filter(
        pc.equal(table["IDR_FTP_ch5_location"], unique_file)
    ).to_batches(max_chunksize=1):

        # convert to a dictionary for the row, where the dictionary
        # elements include the column name as key and
        # value as the value from a single row in the table.
        row = batch.to_pydict()

        # reference a target frame as an integer
        target_frame = int(row["Frames"][0])

        # loop through frames to extract them
        frames_to_tiffs = {}
        for frame in get_ic_context_frames(target_frame=target_frame, movie_len=movie_length):
            # construct the target TIFF path
            local_frame_tif = pathlib.Path(image_download_dir) / row["DNA_dotted_notation"][0].replace(
                f"_{target_frame}.tif", f"_{frame}.tif"
            )

            # skip extraction if TIFF already exists
            if local_frame_tif.exists():
                frames_to_tiffs[str(frame)] = local_frame_tif
                continue

            # otherwise, extract and save
            frames_to_tiffs[str(frame)] = get_frame_tiff_from_idr_ch5(
                frame=frame - 1,
                local_ch5_file=local_ch5_file,
                local_frame_tif=str(local_frame_tif),
            )

        # read the tiffs as arrays for use with pybasic
        # and then add the IC image filepath as a new
        # element along with the others

        destination_filename = (
                f"{image_download_dir}/"
                + row["DNA_dotted_notation"][0].replace(
                    f"_{target_frame}.tif", f"_{target_frame}_IC_TARGET.tif"
                )
            )
        frames_to_tiffs[f"{target_frame}_IC"] = pybasic_IC_target_frame_to_tiff(
            frames_as_arrays=[
                skimage.io.imread(fname=tiff_file)
                for tiff_file in frames_to_tiffs.values()
            ],
            target_frame=list(
                idx
                for idx, frame in enumerate(frames_to_tiffs.keys())
                if frame == str(target_frame)
            )[0],
            destination_filename=destination_filename,
        ) if not pathlib.Path(destination_filename).is_file() else destination_filename

        # create record batches from the frames_to_tiffs
        pylist_rows = []  # switch to pylist; simpler than many small RecordBatches

        for frame_number, frame_tiff in frames_to_tiffs.items():
            # Load the image for this frame (keep dtype; choose channel if needed)
            img = iio.imread(frame_tiff)
            if img.ndim == 3 and img.shape[-1] in (2, 3, 4):
                # choose a channel if multichannel; adjust if your frames are known to be single-channel
                img_for_struct = img[..., 0]
            else:
                img_for_struct = img

            # (optional) normalize to float in [0,1] if your downstream expects it
            # img_for_struct = to_float01(img_for_struct)

            # Build the OME-Arrow struct (name/id fields are up to you)
            ome_struct = make_ome_arrow_row(
                image_id=f"{pathlib.Path(local_ch5_file).stem}__{frame_number}",
                col_name="ome-arrow_original",
                name=pathlib.Path(frame_tiff).name,
                pixels=img_for_struct,
                physical_size_xy_um=0.108,        # put your real XY
                physical_size_z_um=1.0,           # put your real Z
                physical_unit="µm",
                prefer_dimension_order_xyzct=False,  # frames are 2D → XY* hint
                acquisition_dt=datetime.now(timezone.utc),
            )

            base_row = {k: (v[0] if isinstance(v, list) else v) for k, v in row.items()}

            # Preserve your existing logic about “matching the original row” vs “IC frame”
            if str(frame_number) == str(base_row["Frames"]):
                new_row = {
                    **base_row,
                    "Frames": str(frame_number),
                    "DNA_dotted_notation": str(frame_tiff),
                    "Frame_type": "IC_FRAME" if "_IC" not in frame_number else "IC_TARGET_FRAME",
                    "ome-arrow_original": ome_struct["ome-arrow_original"],
                }
            else:
                new_row = {
                    **base_row,
                    "Frames": str(frame_number),
                    "DNA_dotted_notation": str(frame_tiff),
                    "Frame_type": "IC_FRAME" if "_IC" not in frame_number else "IC_TARGET_FRAME",
                    "ome-arrow_original": ome_struct["ome-arrow_original"],
                }
            pylist_rows.append(new_row)

        # Build a table with a fixed schema that includes the OME struct column.
        # If you know all your other field types, declare them here too; otherwise let Arrow infer,
        # but force the struct to use OME_STRUCT_TYPE so batches stay compatible.
        # Build the table letting Arrow infer all non-OME fields
        out_tbl = pa.Table.from_pylist(pylist_rows)

        # Force the OME struct to the canonical type (so batches stay compatible)
        if "ome-arrow_original" in out_tbl.column_names:
            idx = out_tbl.column_names.index("ome-arrow_original")
            out_tbl = out_tbl.set_column(
                idx,
                pa.field("ome-arrow_original", OME_STRUCT_TYPE),
                out_tbl["ome-arrow_original"].cast(OME_STRUCT_TYPE),
            )

        # Use the scalar target_frame you computed earlier for naming
        parquet.write_table(
            out_tbl,
            f"{export_dir}/{pathlib.Path(local_ch5_file).stem}.frame_{target_frame}.parquet",
        )
        # remove the tiff files as we no longer need them
        """for tiff in frames_to_tiffs.values():
            pathlib.Path(tiff).unlink()"""

    # remove the ch5 file as we no longer need it
    # pathlib.Path(local_ch5_file).unlink()
