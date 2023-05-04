from typing import List
import datetime
import random

from pandas import DataFrame, concat, read_sql_query


def columns_names_list(table: DataFrame, rows: int = 1) -> List[str]:
	header = table.head(rows).to_dict('list')

	names_list = []
	prev = None
	for title, sub_title in header.items():
		names = [prev[0] if prev and title.startswith("Unnamed") else title]

		for i, v in enumerate(sub_title):
			if str(v) == 'nan':
				if prev and str(prev[1][i]) != "nan":
					names.append(prev[1][i])
			else:
				names.append(v)

		prev = [names[0], sub_title]
		names_list.append('_'.join(list(names)))

	return names_list


def add_date_col(df: DataFrame, date: List[int], days_range: int = 1):
	return df.apply(
		lambda x: datetime.date(*date) + datetime.timedelta(days=random.randint(0, days_range-1)), axis=1
	)


def copy_body_frame(table: DataFrame, header_offset: int = 0):
	body = table.tail(len(table) - header_offset).copy(deep=False)
	return body


def save_table_to_db(df: DataFrame, engine, table_name: str, **kwargs):
	df.to_sql(table_name, con=engine, **kwargs)


def read_table_from_db(engine, table_name: str, sort_by='id', **kwargs):
	sql_query = f'SELECT * FROM {table_name} ORDER BY {sort_by}'
	df = read_sql_query(sql_query, engine, **kwargs)
	return df.drop("index", axis=1)


def generate_grouped_report(df: DataFrame, grouped_list: List[str], group_by_col: str) -> DataFrame:
	merged_df = DataFrame()
	for s in grouped_list:
		union_rows = df[group_by_col] == s
		if not union_rows.empty:
			group_df = df[union_rows].copy(deep=False)
			row_sum = group_df[:].sum(numeric_only=True)
			group_df.loc['total'] = row_sum
			group_df.fillna('', inplace=True)
			merged_df = concat([merged_df, group_df], ignore_index=False)

	return merged_df


def write_to_file(df: DataFrame, filename: str, title: str, rewrite: bool = False):
	with open(filename, 'w' if rewrite else 'a') as f:
		f.write(title+'\n\n')
		f.write(df.to_string())
