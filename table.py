from enum import Enum
from .database import Database
import sqlite3


class FetchType(Enum):
    NONE = 0
    ALL = 1
    ONE = 2
    MANY = 3


class Table:
    def __init__(self, name: str) -> None:
        self.name = name

        self.query = ""
        self.params = ()
        self.whereQuery = ""
        self.orderQuery = ""
        self.limitQuery = ""

    @staticmethod
    def getTable(name: str):
        return Table(name)

    def getName(self):
        return self.name

    def getColumns(self):
        return Database.getInstance().fetch.fetch(
            f"SELECT name FROM PRAGMA_TABLE_INFO('{self.name}')"
        )

    def create(self, *columns: str) -> "Table":
        self.query = f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(columns)})"
        return self

    def select(self, *columns: str) -> "Table":
        self.query = f"SELECT {', '.join(columns)} FROM {self.name}"
        return self

    def insert(self, **values: str) -> "Table":
        queryKeys = ", ".join(values.keys())
        queryValues = ", ".join(["?" for value in values.values()])

        self.params = tuple(values.values())

        self.query = f"INSERT INTO {self.name} ({queryKeys}) VALUES ({queryValues})"
        return self

    def delete(self) -> "Table":
        self.query = f"DELETE FROM {self.name}"
        return self

    def where(self, operator="AND", **conditions: str) -> "Table":
        self.whereQuery = "WHERE " + (f" {operator} ").join(
            [f"{key} = ?" for key in conditions.keys()]
        )

        self.params = tuple(conditions.values())

        return self

    def order(self, column: str, order: str) -> "Table":
        self.orderQuery = f"ORDER BY {column} {order}"
        return self

    def limit(self, limit: int) -> "Table":
        self.limitQuery = f"LIMIT {limit}"
        return self

    def execute(
        self,
        fetchType: FetchType = FetchType.NONE,
        fetchSize: int | None = None,
    ) -> None | sqlite3.Row | list[sqlite3.Row]:
        self.query = self.buildQuery()

        result = None

        match fetchType:
            case FetchType.NONE:
                result = Database.getInstance().execute(self.query, self.params)
            case FetchType.ALL:
                result = Database.getInstance().fetch(self.query, self.params)
            case FetchType.ONE:
                result = Database.getInstance().fetchOne(self.query, self.params)
            case FetchType.MANY:
                result = Database.getInstance().fetchMany(
                    self.query, self.params, fetchSize
                )

        self.reset()

        return result

    def buildQuery(self) -> str:
        return (
            self.query
            + " "
            + self.whereQuery
            + " "
            + self.orderQuery
            + " "
            + self.limitQuery
        )

    def reset(self) -> None:
        self.query = ""
        self.params = ()
        self.whereQuery = ""
        self.orderQuery = ""
        self.limitQuery = ""

    def __str__(self) -> str:
        return self.buildQuery()
