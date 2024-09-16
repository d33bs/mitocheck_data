"""
Python module for gathering schema of data related to this project
for visibility of relative data.
"""

import pathlib
import pprint

from packager.constants import DATA_FILES
from packager.tables import get_arrow_schema_str_from_csv, write_schema_str_to_file

# specify a dir where the schema may go
schema_dir = pathlib.Path("5.data_packaging/schema")

# if we don't have a schema dir, make it
if not schema_dir.is_dir():
    schema_dir.mkdir()


# gather arrow schema strings and write them to file
schemas = [
    write_schema_str_to_file(
        filename_write=f"{schema_dir}/{filename.replace('/','.')}.arrow.schema.txt",
        schema=get_arrow_schema_str_from_csv(filename_read=filename),
    )
    for filename in DATA_FILES
]

# show the filenames of the schemas afterwards
print("Schema files:")
pprint.pp(schemas)
