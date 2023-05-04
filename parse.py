import sqlalchemy as db
from pandas import read_excel

from logic import *

source_file = 'data.xlsx'
output_file = 'report.txt'
db_name = 'sqlite:///sqldata.db'
db_table_name = 'stats'
header_rows = 2
days_range = 3


def generate_report():
	table = read_excel(source_file, usecols="A:J")
	body_df = copy_body_frame(table, header_rows)
	body_df.columns = columns_names_list(table, header_rows)
	body_df['date'] = add_date_col(body_df, [2022, 10, 1], days_range=days_range)

	engine = db.create_engine(db_name, echo=False)
	save_table_to_db(body_df, engine, db_table_name, if_exists="replace")
	selected_df = read_table_from_db(engine, db_table_name, sort_by='date', index_col='id')
	date_grouped_list = selected_df.groupby('date').agg({"date": "count"}).index.tolist()
	report_df = generate_grouped_report(selected_df, date_grouped_list, group_by_col='date')

	title = f'Report range: {date_grouped_list[0]} - {date_grouped_list[-1]}'
	print(title, report_df, sep='\n\n')
	write_to_file(report_df, output_file, title=title, rewrite=True)
