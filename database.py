from contextlib import closing
from psycopg2 import sql, connect
from json import load

class DB():
    def __init__(self) -> None:
        with open('config.json') as f:
            config = load(f)
        self.psql_url = config["psql_url"]

    def create_options(self) -> None:
        with closing(connect(self.psql_url)) as conn:
            with conn.cursor() as cursor:
                conn.autocommit = True
                insert = sql.SQL('''
                    CREATE TABLE options
                    (
                        Id INTEGER,
                        State BOOLEAN,
                        Symbol VARCHAR(10)
                    );
                ''')
                cursor.execute(insert)
        print("CREATE TABLE options.")

    def insert_options(self, id: int, state: bool, symbol: str) -> None:
        with closing(connect(self.psql_url)) as conn:
            with conn.cursor() as cursor:
                conn.autocommit = True
                values = [(id, state, symbol)]
                insert = sql.SQL('INSERT INTO options (id, state, symbol) VALUES {}').format(
                    sql.SQL(',').join(map(sql.Literal, values))
                )
                cursor.execute(insert)
        print("INSERT TABLE options:", values)

    def update_options(self, id: int, state: bool) -> None:
        with closing(connect(self.psql_url)) as conn:
            with conn.cursor() as cursor:
                conn.autocommit = True
                insert = sql.SQL(f'UPDATE options SET state = {state} WHERE id = {id};')
                cursor.execute(insert)
        print("UPDATE TABLE options:", id, state)

    def read_options(self, symbol: str) -> None:
        with closing(connect(self.psql_url)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM options WHERE state AND symbol = '{symbol}';")
                return [row for row in cursor]

class DB_last_update():
    def __init__(self) -> None:
        with open('config.json') as f:
            config = load(f)
        self.psql_url = config["psql_url"]
        self.block_create = config["block_create"]

    def create_last_update(self) -> None:
        with closing(connect(self.psql_url)) as conn:
            with conn.cursor() as cursor:
                conn.autocommit = True
                insert = sql.SQL('''
                    CREATE TABLE last_update
                    (
                        BlockNumber INTEGER
                    );
                ''')
                cursor.execute(insert)
        print("CREATE TABLE last_update.")

    def insert_last_update(self, block_number: int) -> None:
        with closing(connect(self.psql_url)) as conn:
            with conn.cursor() as cursor:
                conn.autocommit = True
                insert = sql.SQL(f'INSERT INTO last_update (blocknumber) VALUES ({block_number})')
                cursor.execute(insert)
        print("INSERT TABLE last_update:", block_number)

    def update_last_update(self, block_number: int) -> None:
        with closing(connect(self.psql_url)) as conn:
            with conn.cursor() as cursor:
                conn.autocommit = True
                insert = sql.SQL(f'UPDATE last_update SET blocknumber = {block_number}  WHERE true;')
                cursor.execute(insert)
        print("UPDATE TABLE last_update:", block_number)

    def read_last_update(self) -> None:
        with closing(connect(self.psql_url)) as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT * FROM last_update LIMIT 1;')
                return [row for row in cursor][0][0]

if __name__ == "__main__":
    db = DB()
    # db.create_options()

    db_lu = DB_last_update()
    # db_lu.create_last_update()
    # db_lu.insert_last_update(db_lu.block_create)
    db_lu.update_last_update(db_lu.block_create)
    # print(db_lu.read_last_update())
