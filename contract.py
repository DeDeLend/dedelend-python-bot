from json import load
from web3 import Web3
from pandas import DataFrame 
from time import time 

class DDL_contract():
    def __init__(self, ddl_address: str) -> None: 
        with open('config.json') as f:
            config = load(f)
        self.w3 = Web3(Web3.HTTPProvider(config["web3_provider_url"]))
        with open('DDL.json') as f:
            self.abi_ddl = load(f)
        self.ddl = self.w3.eth.contract(address=ddl_address, abi=self.abi_ddl)
        self.ddl_address = ddl_address
        self.private_key = config["private_key"]
        self.address = config["address"]

    def get_current_block(self):
        return self.w3.eth.block_number

    def get_ddl_events_id(self, fromBlock: int):
        dirLogs = {}
        for el in [self.ddl.events.Borrow, self.ddl.events.Unlock, self.ddl.events.Liquidate, self.ddl.events.ForcedExercise, self.ddl.events.ExerciseByPriorLiqPrice]:
            logsFilter = el.createFilter(fromBlock=fromBlock)
            arrEvents = self.w3.eth.get_filter_logs(logsFilter.filter_id)
            arrDecodeEvents = [logsFilter.format_entry(event) for event in arrEvents]
            dirLogs[el.event_name] = list(set([event['args']['optionID'] for event in arrDecodeEvents]))
        return dirLogs

    def get_collateral_info(self, id: int):
        return self.ddl.functions.collateralInfo(id).call()

    def get_expiration_by_id(self, id: int):
        return self.get_collateral_info(id)[1][3]

    def is_option_expired(self, id: int):
        return int(time()) > self.get_expiration_by_id(id) - 30*60 

    def loanState(self, id: int):
        return self.ddl.functions.loanState(id).call()

    def loanStateByPriorLiq(self, id: int):
        return self.ddl.functions.loanStateByPriorLiqPrice(id).call()

    def liquidate(self, id: int):
        nonce = self.w3.eth.get_transaction_count(self.address)
        transaction = self.ddl.functions.liquidate(id).buildTransaction({
            'gas': 70000,
            'gasPrice': self.w3.eth.gas_price,
            'from': self.address,
            'nonce': nonce,
        }) 
        signed_txn = self.w3.eth.account.signTransaction(transaction, private_key=self.private_key)
        print(self.w3.eth.sendRawTransaction(signed_txn.rawTransaction))
        print("OK")

    def forcedExercise(self, id: int):
        nonce = self.w3.eth.get_transaction_count(self.address)
        transaction = self.ddl.functions.forcedExercise(id).buildTransaction({
            'gas': 70000,
            'gasPrice': self.w3.eth.gas_price,
            'from': self.address,
            'nonce': nonce,
        }) 
        signed_txn = self.w3.eth.account.signTransaction(transaction, private_key=self.private_key)
        print(self.w3.eth.sendRawTransaction(signed_txn.rawTransaction))
        print("OK")

    def exerciseByPriorLiqPrice(self, id: int):
        nonce = self.w3.eth.get_transaction_count(self.address)
        transaction = self.ddl.functions.exerciseByPriorLiqPrice(id).buildTransaction({
            'gas': 70000,
            'gasPrice': self.w3.eth.gas_price,
            'from': self.address,
            'nonce': nonce,
        }) 
        signed_txn = self.w3.eth.account.signTransaction(transaction, private_key=self.private_key)
        print(self.w3.eth.sendRawTransaction(signed_txn.rawTransaction))
        print("OK")


