import sqlite3
import logging
import pandas as pd
from pandas import DataFrame

logging.basicConfig(level=logging.DEBUG, filename='./db_vs_df.log', filemode='w')


def benchmark(iterations):
    def actual_decorator(func):
        import time

        def wrapper(*args, **kwargs):
            total = 0
            for i in range(iterations):
                start = time.time()
                return_value = func(*args, **kwargs)
                end = time.time()
                total = total + (end - start)
            logging.info(f'[*] Execution time of {func.__name__} is: {total / iterations} sec.')
            return return_value

        return wrapper

    return actual_decorator


def data_import(connection, cursor):
    cursor.execute(
        'CREATE TABLE REPORTS (Year number, Industry_aggregation_NZSIOC text, Industry_code_NZSIOC number, Value number)'
    )
    connection.commit()
    report = pd.read_csv('annual-enterprise-survey-2019-financial-year-provisional-csv.csv')
    df = DataFrame(report, columns=['Year', 'Industry_aggregation_NZSIOC', 'Industry_code_NZSIOC', 'Value'])
    df.to_sql('REPORTS', connection, if_exists='replace', index=False)
    return df


@benchmark(iterations=1)
def max_year_db(cursor):
    cursor.execute('''SELECT Industry_code_NZSIOC, max(Year) FROM REPORTS''')
    return DataFrame(cursor.fetchall(), columns=['Industry_code_NZSIOC', 'Year'])


@benchmark(iterations=1)
def max_year_df(data_frame):
    return data_frame['Year'].max()


@benchmark(iterations=1)
def max_value_db(cursor):
    cursor.execute('''SELECT Industry_code_NZSIOC, max(Value) FROM REPORTS''')
    return DataFrame(cursor.fetchall(), columns=['Industry_code_NZSIOC', 'Value'])


@benchmark(iterations=1)
def max_value_df(data_frame):
    return data_frame['Value'].max()


@benchmark(iterations=1)
def mean_year_db(cursor):
    cursor.execute('''SELECT AVG(Year) FROM REPORTS''')
    return DataFrame(cursor.fetchall(), columns=['Value'])


@benchmark(iterations=1)
def mean_year_df(data_frame):
    return data_frame['Year'].mean()


def run():
    connection = sqlite3.connect('TestDB.db')
    cursor = connection.cursor()

    data_frame = data_import(connection, cursor)

    max_year_db(cursor)
    max_year_df(data_frame)

    max_value_db(cursor)
    max_value_df(data_frame)

    mean_year_db(cursor)
    mean_year_df(data_frame)


if __name__ == '__main__':
    run()
