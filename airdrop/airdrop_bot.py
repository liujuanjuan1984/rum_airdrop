import datetime
import logging

from quorum_data_py import get_trx_type
from quorum_eth_py import RumERC20Instance
from quorum_mininode_py import MiniNode
from quorum_mininode_py.crypto import account

from airdrop.db_handle import DBHandle
from airdrop.module import AirDropLog, TargetTrxs

logger = logging.getLogger(__name__)


AIRDROPS = {
    "FIRST_EVER": 300,
    "FIRST_DAILY": 30,
    "LIKE": 20,
    "COMMENT": 100,
    "LIKED": 0,
    "OWNER_POST": 300,
    "OWNER_COMMENT": 50,
}


class AirDropBot:
    def __init__(
        self,
        seed: str,
        db_url: str,
        contract_address: str,
        pvtkey: str,
        start_trx: str,
        target_pubkeys: list,
        start_at: str = "2023-05-13T22:20",
        daily_limit: int = 1000,
        airdrop_map: dict = None,
    ):
        self.db = DBHandle(db_url)
        self.erc20 = RumERC20Instance(contract_address, pvtkey=pvtkey)
        self.start_trx = self.db.get_latest_trx() or start_trx
        self.rum = MiniNode(seed, pvtkey)
        self.target_pubkeys = target_pubkeys
        self.start_at = start_at
        self.daily_limit = daily_limit
        self.airdrop_map = airdrop_map or AIRDROPS

    def run(self):
        while True:
            trxs = self.rum.api.get_content(start_trx=self.start_trx)
            logger.info("%s get trx %s", len(trxs), self.start_trx)
            if len(trxs) == 0:
                break
            for trx in trxs:
                self.start_trx = trx["TrxId"]
                self.handle_trx(trx)

    def get_day(self, trx: dict):
        trx_ts = int(str(trx["TimeStamp"])[:10])
        dt_trx = datetime.datetime.utcfromtimestamp(trx_ts)
        dt_start = datetime.datetime.strptime(self.start_at, "%Y-%m-%dT%H:%M")
        day = 1 + (dt_trx - dt_start).days
        logger.info("trx %s day %s", trx["TrxId"], day)
        return day

    def get_today(self):
        dt_start = datetime.datetime.strptime(self.start_at, "%Y-%m-%dT%H:%M")
        today_dt = datetime.datetime.now()
        today = 1 + (today_dt - dt_start).days
        return today

    def handle_trx(self, trx: dict):
        pubkey = trx["SenderPubkey"]
        trx_type = get_trx_type(trx)
        logger.info("trx: %s type: %s", trx["TrxId"], trx_type)
        if trx_type not in ["post", "comment", "counter", "relation"]:
            return
        try:
            post_id = trx["Data"]["object"]["id"]
        except Exception as err:
            logger.warning("trx %s has no post_id", trx["TrxId"])
            logger.info(err)
            post_id = None

        who = "owner" if pubkey in self.target_pubkeys else "user"
        _taget = {
            "trx_id": trx["TrxId"],
            "pubkey": pubkey,
            "post_id": post_id,
            "who": who,
        }

        airdrop_type = None
        target_id = post_id
        # the post and comment that owner posted.
        if who == "owner":
            if trx_type in ["post", "comment"]:
                self.db.add_target(_taget)
                airdrop_type = "OWNER_" + trx_type.upper()
            elif trx_type == "counter":  # liked by owner
                self.db.add_target({"post_id": post_id, "who": "user"})
        # the comment that owner liked.
        elif post_id and trx_type == "comment" and self.db.is_target(post_id, "user"):
            airdrop_type = "LIKED"
            _taget["who"] = "user"
            self.db.update(TargetTrxs, _taget, "post_id")
            logger.info("user update target %s %s", airdrop_type, post_id)
        # the like/comment that to owner post/comment
        elif trx_type == "counter":
            try:
                target_id = trx["Data"]["object"]["object"]["id"]
            except Exception as err:
                logger.info(err)
            if self.db.is_target(target_id, "owner"):
                airdrop_type = "LIKE"
        elif trx_type == "comment":
            target_id = trx["Data"]["object"]["inreplyto"]["id"]
            if self.db.is_target(target_id, "owner"):
                airdrop_type = "COMMENT"

        day = self.get_day(trx)
        if airdrop_type:
            _log = {
                "trx_id": trx["TrxId"],
                "pubkey": pubkey,
                "address": account.pubkey_to_address(pubkey),
                "airdrop_day": day,
                "target_id": target_id,
            }

            if self.db.is_first_ever(pubkey):
                self._add_log(_log, "FIRST_EVER")
            if self.db.is_first_daily(pubkey, day):
                self._add_log(_log, "FIRST_DAILY")

            self._add_log(_log, airdrop_type)

    def _add_log(self, _log: dict, airdrop_type: str):
        _log["airdrop_type"] = airdrop_type
        _log["amount"] = self.airdrop_map.get(airdrop_type, 0)
        self.db.add_log(_log)

    async def airdrop(self):
        today = self.get_today()
        if today < 1:
            logger.info("not ready yet %s", today)
            return
        for day in range(1, today + 1):
            done_sum = self.db.get_airdroped_sum(day)
            logger.info("day %s already airdroped %s", day, done_sum)
            for todo in self.db.get_airdrop_todo(day):
                logger.info("to airdrop %s %s", todo.pubkey, todo.amount)
                if (
                    self.daily_limit
                    and self.db.get_day_sum(todo.pubkey, day) > self.daily_limit
                ):
                    logger.info("daily limit %s", todo.pubkey)
                    self.db.update(
                        AirDropLog, {"eth_tid": "dailylimit", "id": todo.id}, "id"
                    )
                    continue
                eth_tid = self.erc20.transfer(todo.address, todo.amount)
                onchain = await self.erc20.chain.check_trx(eth_tid)
                if onchain:
                    self.db.update(
                        AirDropLog, {"eth_tid": str(eth_tid), "id": todo.id}, "id"
                    )

                    logger.info("airdrop %s %s %s", eth_tid, todo.address, todo.amount)
                else:
                    logger.warning(
                        "airdrop fail %s %s %s", eth_tid, todo.address, todo.amount
                    )
