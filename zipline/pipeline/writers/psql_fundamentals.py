import psycopg2
import pandas as pd
import sqlalchemy as sa

from tiingo import TiingoClient

from zipline.data import bundles

from zipline.utils.cli import maybe_show_progress

FUNDAMENTALS_TABLE = 'fundamentals'

class PSQLFundamentalsWriter:

    def __init__(self, db_path):
        self.conn = psycopg2.connect(db_path)
        self.engine = sa.create_engine(db_path)

    def _ensure_table(self):
        metadata = sa.MetaData()

        f_table = sa.Table(
            'fundamentals',
            metadata,
            sa.Column('date', sa.DateTime()),
            sa.Column('id', sa.Integer()),
            sa.Column('name', sa.String()),
            sa.Column('value', sa.Float()),
            sa.UniqueConstraint('id', 'date', 'name', name='fundamentals_id_day_name_ux')
        )

        sa.Index('fundamentals_id_day_value', f_table.c.id, f_table.c.date, f_table.c.name)

        metadata.create_all(self.engine)

    def _write_to_db(self, df):
        """
        Using cursor.mogrify() to build the bulk insert query
        then cursor.execute() to execute the query
        """
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]

        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))

        # SQL quert to execute
        cursor = self.conn.cursor()
        values = [cursor.mogrify("(%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        query  = "INSERT INTO %s(%s) VALUES " % (FUNDAMENTALS_TABLE, cols) + ",".join(values)
        query += "ON CONFLICT(date, id, name) DO UPDATE SET value = excluded.value"
        
        try:
            cursor.execute(query, tuples)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    def write(self, data, show_progress=False):
        self._ensure_table()

        for table in data:
            self._write_to_db(table)





