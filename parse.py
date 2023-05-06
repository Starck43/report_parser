import sqlalchemy as db
from pandas import read_excel

from logic import *

SOURCE_FILE = 'data.xlsx'
OUTPUT_FILE = 'reports/report.txt'
DBNAME = 'sqlite:///sqldata.db'
DB_TABLE_NAME = 'stats'
HEADER_ROWS = 2
DAYS_RANGE = 3


def generate_report():
	table = read_excel(SOURCE_FILE, usecols="A:J")
	body_df = copy_body_frame(table, HEADER_ROWS)
	body_df.columns = columns_names_list(table, HEADER_ROWS)
	body_df['date'] = add_date_col(body_df, [2022, 10, 1], days_range=DAYS_RANGE)

	engine = db.create_engine(DBNAME, echo=False)
	save_table_to_db(body_df, engine, DB_TABLE_NAME, if_exists="replace")
	selected_df = read_table_from_db(engine, DB_TABLE_NAME, sort_by='date', index_col='id')
	grouped_date_list = selected_df.groupby('date').agg({"date": "count"}).index.tolist()
	report_df = generate_grouped_report(selected_df, grouped_date_list, group_by_col='date')

	title = f'Report range: {grouped_date_list[0]} - {grouped_date_list[-1]}'
	print(title, report_df, sep='\n\n')
	write_to_file(report_df, OUTPUT_FILE, title=title, rewrite=True)
