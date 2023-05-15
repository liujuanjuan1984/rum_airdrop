from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AirDropLog(Base):
    """the log of airdrop"""

    __tablename__ = "airdrop_logs"

    id = Column(Integer, primary_key=True)
    pubkey = Column(String, index=True, default=None)
    address = Column(String, index=True, default=None)
    amount = Column(Integer, default=None)
    trx_id = Column(String, index=True, default=None)
    target_id = Column(String, index=True, default=None)
    airdrop_type = Column(String, default=None)
    airdrop_day = Column(Integer, default=None)
    memo = Column(String, default=None)
    eth_tid = Column(String, default=None)


class TargetTrxs(Base):

    __tablename__ = "target_trxs"

    id = Column(Integer, primary_key=True)
    trx_id = Column(String, index=True, unique=True, default=None)
    post_id = Column(String, index=True, unique=True)
    who = Column(String)
    pubkey = Column(String, index=True, default=None)
