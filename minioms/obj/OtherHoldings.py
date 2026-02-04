from ..oms_db.classes_io import OtherHoldings_IO
from jackutil.microfunc import types_validate
import pandas as pd

class io_utility:
	def bookkeeper_report_load_wrapper(df0):
		holding = df0.loc[:,["symbol","quantity","note"]]
		holding.columns = ['symbol','other_holding','note']
		return holding.iloc[:,:2]

class br_utility:
	def group_by_symbol(df0):
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		return df0.groupby("symbol").sum().reset_index(drop=False)

