# import boto
# from dateutil import parser
# import datetime

# def less_than_a_week_since_last_upload(s3folder):
# 	s3 = boto.connect_s3()
# 	bucket = s3.get_bucket("fsatestbucket")
# 	keys = bucket.get_all_keys()

# 	keys2 = [k for k in keys if s3folder in k.key]
# 	keys2 = [k for k in keys2 if (".zip" in k.key or ".csv" in k.key)]

# 	#Only keep files whose dates can be parsed
# 	keys3 = []
# 	for k in keys2:
# 	    try:
# 		a = k.key.replace(s3folder,"")
# 		a = parser.parse(a[:8])
# 		keys3.append(k)
# 	    except:
# 		pass

# 	dates = [k.key.replace(s3folder,"") for k in keys3]
# 	dates = [parser.parse(d[:8]).date() for d in dates]

# 	today = datetime.datetime.now().date()
# 	deltas = [(today-d).days for d in dates]

# 	if min(deltas)<7:
# 	    return True
# 	else:
# 	    return False
