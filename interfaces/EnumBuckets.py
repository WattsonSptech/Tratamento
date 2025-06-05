from enum import Enum
import os
from decouple import config

class EnumBuckets(Enum):
    RAW = config("BUCKET_NAME_RAW")
    TRUSTED = config("BUCKET_NAME_TRUSTED")
    CLIENT = config("BUCKET_NAME_CLIENT")