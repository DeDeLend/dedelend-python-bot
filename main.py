from contract import DDL_contract
from database import DB, DB_last_update
from time import sleep
from json import load
from telegram import Bot

if __name__ == "__main__":
    with open('config.json') as f:
        config = load(f)
    db = DB()
    db_lu = DB_last_update()
    ddl_contracts = {"eth": DDL_contract(config["ddl_address_eth"]), "btc": DDL_contract(config["ddl_address_btc"])}
    bot = Bot(token=config["telegram_token"])
    while True:
        try:
            fromBlock = db_lu.read_last_update()
            print("fromBlock: ", fromBlock)
            for symbol in ddl_contracts:
                print(f"UPDATE {symbol} DDL DATA")
                dirOptionId = ddl_contracts[symbol].get_ddl_events_id(fromBlock)
                for id in dirOptionId["Borrow"]:
                    db.insert_options(id, True, symbol)
                for id in dirOptionId["Unlock"] + dirOptionId["Liquidate"] + dirOptionId["ForcedExercise"] + dirOptionId["ExerciseByPriorLiqPrice"]:
                    db.update_options(id, False)
            db_lu.update_last_update(ddl_contracts["eth"].get_current_block())
            for symbol in ddl_contracts:
                print("Check ", symbol)
                bot.send_message(chat_id=config["chat_id"], text=f"Check {symbol}")
                arrActiveOptions = db.read_options(symbol)
                bot.send_message(chat_id=config["chat_id"], text=f"arrActiveOptions {arrActiveOptions}")
                for option in arrActiveOptions:
                    if ddl_contracts[symbol].loanState(option[0]): 
                        print("liquidate option №: ", option[0])
                        ddl_contracts[symbol].liquidate(option[0])
                        bot.send_message(chat_id=config["chat_id"], text=f"liquidate option №: {option[0]}")
                    elif ddl_contracts[symbol].is_option_expired(option[0]):
                        print("option expired №: ", option[0])
                        ddl_contracts[symbol].forcedExercise(option[0])
                        bot.send_message(chat_id=config["chat_id"], text=f"option expired №: {option[0]}")
                    elif ddl_contracts[symbol].loanStateByPriorLiq(option[0]):
                        print("exercise by prior liq price №: ", option[0])
                        ddl_contracts[symbol].exerciseByPriorLiqPrice(option[0])
                        bot.send_message(chat_id=config["chat_id"], text=f"exercise by prior liq price №: {option[0]}")
            bot.send_message(chat_id=config["chat_id"], text="wait 60 seconds...")
        except Exception as e:
            print(e)
        print("wait 60 seconds...")
        sleep(60)