def print_obj(obj):
	if(isinstance(obj, dict)):
		for key, val in obj.items():
			print(f"[{key}] {val.full_path}")
			print(val.df.to_string())
			print()
	else:
		print(obj.full_path)
		print(obj.df.to_string())
		print()
