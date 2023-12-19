import sqlite3
import threading
import os

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

        self.databasePath = databasePath
        self.connection: sqlite3.Connection = sqlite3.connect(self.databasePath)
        self.cursor: sqlite3.Cursor = self.connection.cursor()
        self.queriesToExecute: list[str] = []

        self.executerThread: threading.Thread = threading.Thread(target=self.run)

        self.shouldRun: bool = True

        self.start()

    def start(self) -> None:
        self.executerThread.start()

    def run(self) -> None:
        self.connection = sqlite3.connect(self.databasePath)
        self.cursor = self.connection.cursor()

        while self.shouldRun:
            if len(self.queriesToExecute) > 0:
                self.executeQuery(self.queriesToExecute.pop(0))

    def execute(self, *queries: str) -> None:
        self.queriesToExecute.append(*queries)

    def executeQuery(self, query: str) -> None:
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            print(f"Error while executing '{query}': \n" + str(e))

    def fetch(self, query: str) -> list[tuple]:
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def fetchOne(self, query: str) -> tuple:
        self.cursor.execute(query)
        return self.cursor.fetchone()

    def fetchMany(self, query: str, size: int | None = None) -> list[tuple]:
        self.cursor.execute(query)
        return self.cursor.fetchmany(size)

    def close(self):
        self.shouldRun = False
        self.executerThread.join()
        
        self.connection.close()
