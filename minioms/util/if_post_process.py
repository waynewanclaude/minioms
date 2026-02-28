from .op_exec_match import op_exec_match
from .op_alloc_exec import op_alloc_exec

def _print_obj(obj):
	if(isinstance(obj, dict)):
		for key, val in obj.items():
			print(f"[{key}] {val.full_path}")
			print(val.df.to_string())
			print()
	else:
		print(obj.full_path)
		print(obj.df.to_string())
		print()

def post_process_account(db_dir,account,auto_commit=True):
	# --
	# -- match and alloc per account executions
	# --
	objects_for_match = op_exec_match.load_required_objects(db_dir=db_dir,account=account)
	matching_results = op_exec_match.exec_match(*objects_for_match)
	try:
		op_exec_match.validate(matching_results)
	except Exception:
		for obj in objects_for_match:
			_print_obj(obj)
		raise
	if(auto_commit):
		op_exec_match.commit_result(matching_results)

	# --
	# -- post account alloc to per portfolio
	# --
	objects_for_post = op_alloc_exec.load_required_objects(db_dir=db_dir,account=account)
	alloc_results = op_alloc_exec.alloc_exec(*objects_for_post)
	try:
		op_alloc_exec.validate(alloc_results)
	except Exception:
		for obj in objects_for_post:
			_print_obj(obj)
		raise
	if(auto_commit):
		op_alloc_exec.commit_result(alloc_results)
		
	return matching_results, alloc_results

# -- (HUM) REVIEWED;pending_rm -- def display_results(results):
# -- (HUM) REVIEWED;pending_rm -- 	matching_results, alloc_results = results
# -- (HUM) REVIEWED;pending_rm -- 	print("#"*120)
# -- (HUM) REVIEWED;pending_rm -- 	print("# matching")
# -- (HUM) REVIEWED;pending_rm -- 	display(matching_results[2].df)
# -- (HUM) REVIEWED;pending_rm -- 	print("#"*120)
# -- (HUM) REVIEWED;pending_rm -- 	print("# allocations")
# -- (HUM) REVIEWED;pending_rm -- 	display(matching_results[3].df)
# -- (HUM) REVIEWED;pending_rm -- 	print("#"*120)
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	print("#"*120)
# -- (HUM) REVIEWED;pending_rm -- 	for strat_portf,alloc_result in alloc_results.items():
# -- (HUM) REVIEWED;pending_rm -- 		print("#"*120)
# -- (HUM) REVIEWED;pending_rm -- 		print(strat_portf)
# -- (HUM) REVIEWED;pending_rm -- 		print("*"*120)
# -- (HUM) REVIEWED;pending_rm -- 		print("**** error ****", alloc_result[2])
# -- (HUM) REVIEWED;pending_rm -- 		print("*"*120)
# -- (HUM) REVIEWED;pending_rm -- 		print("paired_txns [tail]")
# -- (HUM) REVIEWED;pending_rm -- 		display(alloc_result[1].df.tail())
# -- (HUM) REVIEWED;pending_rm -- 		print("open_pos [tail]")
# -- (HUM) REVIEWED;pending_rm -- 		display(alloc_result[0].df.tail())
