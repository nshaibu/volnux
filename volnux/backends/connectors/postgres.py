import psycopg

from volnux.backends.connection import BackendConnectorBase


class PostgresConnector(BackendConnectorBase):
    def __init__(self, host, port, db=None, username=None, password=None):
        super().__init__(
            host=host, port=port, db=db, username=username, password=password
        )
        self._connection = psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.username,
            password=self.password,
        )
        self._cursor = self._connection.cursor()

    def connect(self):
        if not self._cursor:
            try:
                self._connection = psycopg.connect(
                    host=self.host,
                    port=self.port,
                    dbname=self.database,
                    user=self.username,
                    password=self.password,
                )
                self._cursor = self._connection.cursor()
            except psycopg.Error as e:
                raise ConnectionError(f"Error connecting to PostgreSQL: {str(e)}")
        return self._cursor

    def disconnect(self) -> None:
        if self._cursor:
            self._cursor.close()
        self._cursor = None

    def is_connected(self) -> bool:
        try:
            if self._connection is not None:
                self._cursor.execute("SELECT 1")  # type: ignore
                return True
            return False
        except (psycopg.Error, AttributeError):
            return False
