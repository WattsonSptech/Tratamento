from enum import Enum
import os

class EnumBuckets(Enum):
    RAW = os.getenv("BUCKET_NAME_RAW")
    TRUSTED = os.getenv("BUCKET_NAME_TRUSTED")
    CLIENT = os.getenv("BUCKET_NAME_CLIENT")