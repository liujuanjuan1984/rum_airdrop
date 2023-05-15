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
    "LIKED": 50,
    "OWNER_POST": 300,
    "OWNER_COMMENT": 50,
}

DAILY_LIMIT = 1000


class AirDropBot:
    def __init__(
        self,
        seed,
        db_url,
        contract_address,
        pvtkey,
        start_trx,
        target_pubkeys,
        start_at="2023-05-13T22:20",
    ):
        self.db = DBHandle(db_url)
        self.erc20 = RumERC20Instance(contract_address, pvtkey=pvtkey)
        self.start_trx = self.db.get_latest_trx() or start_trx
        self.rum = MiniNode(seed, pvtkey)
        self.target_pubkeys = target_pubkeys
        self.start_at = start_at

    def run(self):
        while True:
            trxs = self.rum.api.get_content(start_trx=self.start_trx)
            logger.info("%s get trx %s", len(trxs), self.start_trx)
            if len(trxs) == 0:
                break
            for trx in trxs:
                self.start_trx = trx["TrxId"]
                self.handle_trx(trx)

    def get_day(self, trx):
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

    def handle_trx(self, trx):
        pubkey = trx["SenderPubkey"]
        trx_type = get_trx_type(trx)
        logger.info("trx %s %s", trx["TrxId"], trx_type)
        if trx_type not in ["post", "comment", "counter", "relation"]:
            return
        try:
            post_id = trx["Data"]["object"]["id"]
        except Exception as err:
            logger.warning("trx %s has no post_id", trx["TrxId"])
            logger.info(err)
            post_id = None

        _taget = {
            "trx_id": trx["TrxId"],
            "pubkey": pubkey,
            "post_id": post_id,
            "who": "owner",
        }

        do_log = False
        target_id = post_id
        # the post and comment that owner posted.
        if pubkey in self.target_pubkeys:
            if trx_type in ["post", "comment"]:
                self.db.add(TargetTrxs, _taget)
                airdrop_type = "OWNER_" + trx_type.upper()
                do_log = True
            elif trx_type in ["counter"]:  # liked by owner
                self.db.add(
                    TargetTrxs, {"post_id": trx["Data"]["object"]["id"], "who": "user"}
                )
                logger.info("user add target %s", trx["Data"]["object"]["id"])
        # the comment that owner liked.
        elif post_id and trx_type in ["comment"] and self.db.is_target(post_id, "user"):
            airdrop_type = "LIKED"
            do_log = True
            _taget["who"] = "user"
            self.db.update(TargetTrxs, _taget, "post_id")
            logger.info("user update target %s %s", airdrop_type, post_id)
        # the like/comment that to owner post/comment
        elif trx_type in ["counter"]:
            try:
                target_id = trx["Data"]["object"]["id"]
            except Exception as err:
                target_id = trx["Data"]["object"]["object"]["id"]
                logger.warning("trx %s has no target_id", trx["TrxId"])
                logger.info(err)
            if target_id and self.db.is_target(target_id, "owner"):
                airdrop_type = "LIKE"
                do_log = True
        elif trx_type in ["comment"]:
            target_id = trx["Data"]["object"]["inreplyto"]["id"]
            if self.db.is_target(target_id, "owner"):
                airdrop_type = "COMMENT"
                do_log = True

        day = self.get_day(trx)
        if do_log:
            _target = {
                "trx_id": trx["TrxId"],
                "pubkey": pubkey,
                "address": account.pubkey_to_address(pubkey),
                "airdrop_day": day,
                "target_id": target_id,
            }

            if self.db.is_first_ever(pubkey):
                _target.update(
                    {"amount": AIRDROPS["FIRST_EVER"], "airdrop_type": "FIRST_EVER"}
                )
                self.db.add(AirDropLog, _target)
                logger.info("first ever %s", pubkey)
            if self.db.is_first_daily(pubkey, day):
                _target.update(
                    {"amount": AIRDROPS["FIRST_DAILY"], "airdrop_type": "FIRST_DAILY"}
                )
                self.db.add(AirDropLog, _target)
                logger.info("first daily %s", pubkey)
            _target.update(
                {"amount": AIRDROPS[airdrop_type], "airdrop_type": airdrop_type}
            )
            self.db.add(AirDropLog, _target)
            logger.info(
                "airdrop log %s %s %s", pubkey, airdrop_type, AIRDROPS[airdrop_type]
            )

    async def airdrop(self):
        today = self.get_today()
        if today < 1:
            logger.info("not ready yet %s", today)
            return
        for day in range(1, today + 1):
            logger.info("airdrop day %s", day)
            for todo in self.db.get_airdrop_todo(day):
                logger.info("to airdrop %s %s", todo.pubkey, todo.amount)
                if self.db.get_day_sum(todo.pubkey, day) > DAILY_LIMIT:
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
