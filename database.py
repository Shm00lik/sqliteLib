import sqlite3


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
        self.cursor = self.connection.cursor()

    def execute(self, *queris: str) -> None:
        for query in queris:
            self.cursor.execute(query)

        self.connection.commit()

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
        self.connection.close()
