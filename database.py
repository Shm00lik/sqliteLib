import sqlite3
import threading


class Database:
    __instance = None

    @staticmethod
    def getInstance(databasePath="./db.sqlite") -> "Database":
        if Database.__instance == None:
            Database.__instance = Database(databasePath)

        return Database.__instance

    def __init__(self, databasePath: str) -> None:
        if Database.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            Database.__instance = self

        self.connection = sqlite3.connect(databasePath, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        self.lock = threading.Lock()

    def execute(self, query: str, params: tuple = ()) -> None:
        with self.lock:
            print(query)
            print(params)
            self.cursor.execute(query, params)
            self.connection.commit()

    def fetch(self, query: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self.lock:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()

    def fetchOne(self, query: str, params: tuple = ()) -> sqlite3.Row:
        with self.lock:
            self.cursor.execute(query, params)
            return self.cursor.fetchone()

    def fetchMany(
        self, query: str, params: tuple = (), size: int | None = None
    ) -> list[sqlite3.Row]:
        with self.lock:
            self.cursor.execute(query, params)
            return self.cursor.fetchmany(size)

    def close(self):
        self.connection.close()
