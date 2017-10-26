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

    def __repr__(self, *args, **kwargs):
        return '<DataTest id: {} test_name: {} timestamp: {} error_status: {} >'.format(
            self.id,
            self.test_name,
            self.timestamp,
            self.error_status)

class Alert(BaseModel):
    __tablename__ = 'alerts'

    id = Column(BigInteger, primary_key=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    test_name = Column(String, nullable=False, index=True)
    data_test_id = Column(BigInteger, ForeignKey('data_tests.id'))
    alert_hash = Column(String, nullable=False)

    ## TODO: unique indeex across all fields but id?

    def __repr__(self, *args, **kwargs):
        return '<Alert id: {} data_test_id: {} timestamp: {} test_name: {} >'.format(
            self.id,
            self.data_test_id,
            self.timestamp,
            self.test_name)

