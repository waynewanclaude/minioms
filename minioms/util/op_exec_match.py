from ..obj.AccountOrders import io_utility as acctord_io
from ..obj.AcctDailyOrders import io_utility as acctdord_io
from ..obj.Executions import io_utility as executions_io
from ..obj.Executions import br_utility as executions_br
from ..oms_db.classes_io import Matchings_IO
from ..oms_db.classes_io import Allocations_IO
from jackutil.microfunc import types_validate
import pandas as pd

class op_exec_match:
	# --
	def load_required_objects(*,db_dir,account):
		types_validate(db_dir,msg="db_dir",types=[ type("") ],allow_none=False)
		types_validate(account,msg="account",types=[ type("") ],allow_none=False)
		# --
		acct_orders = acctord_io.load(db_dir=db_dir,account=account)
		daily_ordrs = acctdord_io.load(db_dir=db_dir,account=account)
		executions = executions_io.load(db_dir=db_dir,account=account)
		# --
		return acct_orders,daily_ordrs,executions

	def exec_match(acct_orders,daily_orders,executions):
		executions_df = executions_br.prepare_executions_for_matching(executions.df.copy())
		matched,unmatch,matching = account_orders_and_executions_match(acct_orders.df.copy(),executions_df)
		# --
		executions_df = executions_br.prepare_executions_for_alloc(executions.df.copy())
		# (CLU) NEED_REVIEW: daily_orders.df passed without .copy(), inconsistent with lines above
		# (CLU) NEED_REVIEW: where .copy() is used explicitly. pd.merge does not mutate in place
		# (CLU) NEED_REVIEW: so it is safe here, but Fix: add .copy() for consistency.
		allocations = allocate_daily_orders(daily_orders.df,executions_df)
		# --
		# (CLU) NEED_REVIEW: .db_dir and .account are set in the subclass __init__ but are not
		# (CLU) NEED_REVIEW: part of the DataFile base class interface, making this fragile.
		# (CLU) NEED_REVIEW: Fix: either promote db_dir and account to DataFile base class
		# (CLU) NEED_REVIEW: attributes, or pass them explicitly as parameters to exec_match.
		db_dir = executions.db_dir
		account = executions.account
		return (
			Matchings_IO(db_dir=db_dir,account=account,df0=matched),
			Matchings_IO(db_dir=db_dir,account=account,df0=unmatch),
			Matchings_IO(db_dir=db_dir,account=account,df0=matching),
			Allocations_IO(db_dir=db_dir,account=account,df0=allocations),
		)

	def validate(match_result,raise_on_err=True):
		matched,unmatch,matching,allocation = match_result
		error = len(unmatch.df) > 0
		if(raise_on_err and error):
			raise ValueError("unmatch executions found.")
		return not error

	def commit_result(match_result):
		op_exec_match.validate(match_result,raise_on_err=True)
		matched,unmatch,matching,allocations = match_result
		matching.write()
		allocations.write()

# -- --------------------------------------------------------------------------------
# -- helpers
# -- --------------------------------------------------------------------------------
def account_orders_and_executions_match(orders,executions):
	if(len(orders)==0):
		return (None,None,None)
	# --
	# (CLU) NEED_REVIEW: column names are reassigned in place, assuming exact column count and
	# (CLU) NEED_REVIEW: order from upstream. If the column spec of AccountOrders or Executions
	# (CLU) NEED_REVIEW: changes, this breaks silently with wrong data rather than an error.
	# (CLU) NEED_REVIEW: Fix: use df.rename(columns={...}) with explicit old->new mapping so
	# (CLU) NEED_REVIEW: mismatches raise a visible error instead of silently corrupting data.
	orders.columns = ['symbol','order_unit']
	executions.columns = ['date','symbol','exec_unit','price','amount','pkey']
	# --
	matching = pd.merge(orders, executions, on='symbol', how='outer')
	matching.loc[:,'status'] = 'diff-qty'
	matching.loc[matching['order_unit'].isnull(),'status'] = 'exec-wo-order'
	matching.loc[matching['exec_unit'].isnull(),'status'] = 'missing-exec'
	matching.loc[matching['exec_unit']==matching['order_unit'],'status'] = 'matched'
	matching.columns = ['symbol','ord_qty','date','exec_qty','exec_price','ttl_cost','exec_pkey','match']
	matching = matching[ ['date','symbol','ord_qty','exec_qty','exec_price','ttl_cost','match','exec_pkey'] ]
	# --
	matched = matching[matching['match']=='matched']
	unmatch = matching[matching['match'] !='matched']
	return (matched,unmatch,matching)

def allocate_daily_orders(daily_orders,executions):
	# --
	allocation = pd.merge(daily_orders, executions, on='symbol', suffixes=['_ord','_exe'], how='outer')
	allocation['cost'] = -1 * allocation['unit'] * allocation['exec_price']
	allocation = allocation["book,portfolio,pkey_ord,date,symbol,action,unit,exec_price,cost,linked_buy_pkey".split(",")]
	allocation.columns = "book,portfolio,pkey,date,symbol,action,unit,exec_price,cost,linked_buy_pkey".split(",")
	# --
	return allocation

