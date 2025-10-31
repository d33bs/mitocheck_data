# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Example for using packaged data
#
# This notebook shows how to use the packaged data from this project.

# +
import lancedb
from serpula_rasa.image import show_images_from_lance

# setup a connection to lancedb
ldb = lancedb.connect(uri=(lance_dir := "packaged/lancedb/mitocheck_data"))
# -

# show all table names
# note: all table names include numbers corresponing to the step they came from.
ldb.table_names(limit=20)

ldb.open_table(name="5.data_packaging.location_and_ch5_frame_image_data").to_pandas().head()

show_images_from_lance(
    db_path=lance_dir,
    table_name="5.data_packaging.location_and_ch5_frame_image_data",
    col_name="ome-arrow_original",
    max_images=20,
    pick="first",
    cmap="gray",
    base_size=10,
    cols=1,
)
