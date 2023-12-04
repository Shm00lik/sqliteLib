from enum import Enum
from .database import Database


class FetchType(Enum):
    NONE = 0
    ALL = 1
    ONE = 2
    MANY = 3


class Table:
    def __init__(self, name: str) -> None:
        self.name = name

        self.query = ""
        self.whereQuery = ""
        self.orderQuery = ""
        self.limitQuery = ""

    @staticmethod
    def getTable(name: str):
        return Table(name)

    def create(self, *columns: str) -> "Table":
        self.query = f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(columns)})"
        return self

    def select(self, *columns: str) -> "Table":
        self.query = f"SELECT {', '.join(columns)} FROM {self.name}"
        return self

    def insert(self, **values: str) -> "Table":
        queryKeys = ", ".join(values.keys())
        queryValues = ", ".join([f"'{value}'" for value in values.values()])

        self.query = f"INSERT INTO {self.name} ({queryKeys}) VALUES ({queryValues})"
        return self

    def where(self, **conditions: str) -> "Table":
        self.whereQuery = "WHERE " + " AND ".join(
            [f"{key} = '{value}'" for key, value in conditions.items()]
        )
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
    ) -> list[tuple] | tuple | None:
        self.query = self.buildQuery()

        self.reset()

        match fetchType:
            case FetchType.NONE:
                return Database.getInstance().execute(self.query)
            case FetchType.ALL:
                return Database.getInstance().fetch(self.query)
            case FetchType.ONE:
                return Database.getInstance().fetchOne(self.query)
            case FetchType.MANY:
                return Database.getInstance().fetchMany(self.query, fetchSize)

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
        self.whereQuery = ""
        self.orderQuery = ""
        self.limitQuery = ""

    def __str__(self) -> str:
        return self.buildQuery()
