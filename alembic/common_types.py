from sqlalchemy import LargeBinary, Numeric
from sqlalchemy.dialects.postgresql import UUID

UINT256_DIGITS = 78
SA_TYPE_TXHASH = LargeBinary(32)
SA_TYPE_ADDR = LargeBinary(20)
SA_TYPE_VALUE = Numeric(precision=UINT256_DIGITS, scale=0)