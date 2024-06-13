import sqlalchemy
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text, create_engine, MetaData, Table, Column
from sqlalchemy.orm import sessionmaker
import warnings


class Creds:
    def __init__(self):
        self.user = "user"
        self.host = "password"
        self.database = "postgres"
        self.password = "postgres"


class DBOperations:
    def __init__(self):
        creds = Creds()
        self.engine = create_async_engine(
            f"postgresql+asyncpg://{creds.user}:{creds.password}@{creds.host}/{creds.database}",
            future=True,
            echo=True,
        )
        self.sync_engine = create_engine(f"postgresql://{creds.user}:{creds.password}@{creds.host}/{creds.database}")
        self.async_session = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    async def get_session(self):
        async with self.async_session() as session:
            yield session

    async def execute_ddl(self, query):
        async for session in self.get_session():
            async with session.begin():
                await session.execute(text(query))
                await session.commit()

    async def execute_dql(self, query):
        async for session in self.get_session():
            try:
                async with session.begin():
                    try:
                        result = await session.execute(text(query))
                        return result.fetchall()
                    except:
                        return None
            except Exception as e:
                if type(e).__name__ == 'PendingRollbackError':
                    warnings.warn('PostgreSQL server is down')

    def create_table(self, table_name):
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", sqlalchemy.Integer, primary_key=True),
            Column("name", sqlalchemy.String(255)),
        )
        metadata.create_all(self.sync_engine)
