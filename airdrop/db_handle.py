import logging

from sqlalchemy import asc, create_engine
from sqlalchemy.orm import sessionmaker

from airdrop.module import AirDropLog, Base, TargetTrxs

logger = logging.getLogger(__name__)


class DBHandle:
    def __init__(self, db_url: str, echo: bool = False):
        logger.info("db_url: %s", db_url)
        self.engine = create_engine(db_url, echo=echo)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def add(self, table, payload: dict):
        with self.Session() as session:
            obj = table(**payload)
            session.add(obj)
            try:
                session.commit()
                return True
            except Exception as err:
                session.rollback()
                logger.info(err)
                return False

    def add_log(self, payload: dict):
        _amount = payload.get("amount", 0)
        if _amount <= 0:
            result = False
        else:
            result = self.add(AirDropLog, payload)
        logger.info(
            "add_log %s %s %s",
            payload.get("airdrop_type"),
            _amount,
            result,
        )
        return result

    def add_target(self, payload: dict):
        result = self.add(TargetTrxs, payload)
        logger.info(
            "add_target %s %s %s", payload.get("who"), payload.get("post_id"), result
        )
        return result

    def is_target(self, post_id: str, who: str):
        with self.Session() as session:
            return (
                session.query(TargetTrxs)
                .filter(TargetTrxs.post_id == post_id, TargetTrxs.who == who)
                .count()
                > 0
            )

    def update(self, table, payload: dict, pk: str):
        with self.Session() as session:
            session.query(table).filter_by(**{pk: payload[pk]}).update(payload)
            try:
                session.commit()
                return True
            except Exception as err:
                session.rollback()
                logger.info(err)
                return False

    def get_latest_trx(self):
        with self.Session() as session:
            target = (
                session.query(TargetTrxs)
                .filter(TargetTrxs.trx_id != None)
                .order_by(TargetTrxs.id.desc())
                .first()
            )
            if target is None:
                tid = None
            else:
                tid = target.trx_id
            logger.info("get_latest_trx %s", tid)
            return tid

    def get_airdrop_todo(self, day: int):
        with self.Session() as session:
            return (
                session.query(AirDropLog)
                .filter(AirDropLog.airdrop_day == day, AirDropLog.eth_tid == None)
                .order_by(asc(AirDropLog.pubkey))
                .all()
            )

    def is_first_ever(self, pubkey: str):
        with self.Session() as session:
            return session.query(AirDropLog).filter_by(pubkey=pubkey).count() == 0

    def is_first_daily(self, pubkey: str, day: int, exclude_type: str = "FIRST_EVER"):
        with self.Session() as session:
            return (
                session.query(AirDropLog)
                .filter(
                    AirDropLog.pubkey == pubkey,
                    AirDropLog.airdrop_day == day,
                    AirDropLog.airdrop_type != exclude_type,
                )
                .count()
                == 0
            )

    def get_day_sum(self, pubkey: str, day: int, exclude_type: str = "FIRST_EVER"):
        with self.Session() as session:
            logs = (
                session.query(AirDropLog)
                .filter(
                    AirDropLog.pubkey == pubkey,
                    AirDropLog.airdrop_day == day,
                    AirDropLog.airdrop_type != exclude_type,
                    AirDropLog.eth_tid != None,
                )
                .all()
            )
            total = sum(log.amount for log in logs)
            logger.info("%s day %s already got %s", pubkey, day, total)
            return total

    def get_airdroped_sum(self, day):
        with self.Session() as session:
            logs = (
                session.query(AirDropLog)
                .filter(
                    AirDropLog.airdrop_day == day,
                    AirDropLog.eth_tid != None,
                    AirDropLog.eth_tid != "dailylimit",
                )
                .all()
            )
            total = sum(log.amount for log in logs)
            return total
