from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
    Boolean,
    func,
    MetaData
)
from sqlalchemy.ext.declarative import declarative_base

metadata = MetaData()
BaseModel = declarative_base(metadata=metadata)

class DataPing(BaseModel):
    __tablename__ = 'data_pings'

    id = Column(BigInteger, primary_key=True)
    data_source = Column(String, nullable=False)
    test_started_at = Column(DateTime, nullable=False)
    test_ended_at = Column(DateTime, nullable=False)
    test_time_elapsed_ms = Column(Integer, nullable=False)
    test_error_status = Column(String)
    test_error_message = Column(String)
    test_http_status = Column(Integer)
    count = Column(BigInteger)
    source_count = Column(BigInteger)
