from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
    Boolean,
    ForeignKey,
    func,
    MetaData
)
from sqlalchemy.ext.declarative import declarative_base

metadata = MetaData()
BaseModel = declarative_base(metadata=metadata)

class DataTest(BaseModel):
    __tablename__ = 'data_tests'

    id = Column(BigInteger, primary_key=True)
    test_name = Column(String, nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    error_status = Column(String)
    error_message = Column(String)
    query_started_at = Column(DateTime, nullable=False)
    query_ended_at = Column(DateTime, nullable=False)
    query_time_elapsed_ms = Column(Integer, nullable=False)
    query_error_status = Column(String)
    query_error_message = Column(String)
    query_http_status = Column(Integer)
    count = Column(BigInteger)
    source_count = Column(BigInteger)
    last_count = Column(BigInteger)
