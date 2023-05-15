import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from airdrop.module import AirDropLog, Base, TargetTrxs

logger = logging.getLogger(__name__)


class DBHandle:
    def __init__(self, db_url: str, echo: bool = False):
        logger.info("db_url: %s", db_url)
        self.engine = create_engine(db_url, echo=echo)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def add(self, table, payload):
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

    def is_target(self, post_id, who):
        with self.Session() as session:
            return (
                session.query(TargetTrxs)
                .filter(TargetTrxs.post_id == post_id, TargetTrxs.who == who)
                .count()
                > 0
            )

    def update(self, table, payload, pk):
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
            target = session.query(TargetTrxs).order_by(TargetTrxs.id.desc()).first()
            if target is None:
                return None
            return target.trx_id

    def get_airdrop_todo(self, day):
        with self.Session() as session:
            return (
                session.query(AirDropLog)
                .filter(AirDropLog.airdrop_day == day, AirDropLog.eth_tid == None)
                .all()
            )

    def is_first_ever(self, pubkey):
        with self.Session() as session:
            return (
                session.query(AirDropLog).filter(AirDropLog.pubkey == pubkey).count()
                == 0
            )

    def is_first_daily(self, pubkey, day):
        with self.Session() as session:
            return (
                session.query(AirDropLog)
                .filter(
                    AirDropLog.pubkey == pubkey,
                    AirDropLog.airdrop_day == day,
                    AirDropLog.airdrop_type != "FIRST_EVER",
                )
                .count()
                == 0
            )

    def get_day_sum(self, pubkey, day):
        with self.Session() as session:
            logs = (
                session.query(AirDropLog)
                .filter(
                    AirDropLog.pubkey == pubkey,
                    AirDropLog.airdrop_day == day,
                    AirDropLog.airdrop_type != "FIRST_EVER",
                    AirDropLog.eth_tid != None,
                )
                .all()
            )
            total = sum(log.amount for log in logs)
            logger.info("%s day %s total: %s", pubkey, day, total)
            return total
