import json
from collections import Counter
from collections import defaultdict


def load_dictionary(dictionary_file, requested_words):
	for line in open(dictionary_file):
		tokens=line[:-1].split('\t')
		query_hash = long(tokens[0])
		if query_hash in requested_words:
			requested_words[query_hash]=tokens[1]
	return requested_words

def load_userstats(user_stats_file,  requested_users):
	for line in open(user_stats_file):
		tokens=line.split('\t')
		user_hash=long(tokens[0])
		if user_hash in requested_users:
			requested_users[user_hash]=json.loads(tokens[1])
	return requested_users

def max_occurence(elements):
	counts=(Counter(elements)).values()
	return max(counts)

def top_shared_elmmt(elemnt_keys, user_list, user_stats):
	key_data=[[fkey,[]] for fkey in elemnt_keys]
	for user in user_list:
		user_data=user_stats[user]
		for el_key, el_list in key_data:
			el_list.extend(user_data[el_key])
	numusers=len(user_list)
	output=[]
	for el_key,el_list in key_data:
		prcent_occurence=100*(1.0*max_occurence(el_list))/numusers
		output.append(["most shared "+el_key,str(prcent_occurence) +" % of users"])
	return output

def feature_avg(feature_keys, user_list,user_stats):
	feature_avgs=[[fkey,0.0] for fkey in feature_keys]
	for user in user_list:
		user_data=user_stats[user]
		for i in range (0,len(feature_avgs)):
			feature_avgs[i][1]+=user_data[feature_avgs[i][0]]
	numusers=len(user_list)
	output=[]
	for fkey, f_sum in feature_avgs:
		f_sum/=numusers
		f_sum*=100
		output.append([fkey+" feature avg",str(f_sum) +" %"])
	return output

def eval_svd_groups(src_file, dictionary_file, userstats_file):
query_dict={}
user_stats={}
for line in open(src_file):
	linedata=json.loads(line[line.index("{"):])
	query_dict[linedata['query']]=None
	for query in linedata['qhashes']:
		query_dict[query]=None
	for user in linedata['uids']:
		user_stats[user]=None
query_dict=load_dictionary(dictionary_file,query_dict)
user_stats=load_userstats(userstats_file,user_stats)
session_stats=["js","rss","limit"]
access_stats=["ip","ua"]
outputdata=[]
for line in open(src_file):
	linedata=json.loads(line)
	users=linedata['uids']
	groupdata=[["query", query_dict[linedata['query']]], ["users", len(linedata['uids'])],["focusness", linedata['foc']]]
	groupdata.extend(['queries', [query_dict[i] for i in linedata['qhashes']]])
	groupdata.extend(top_shared_elmmt(access_stats, users, user_stats))
	groupdata.extend(feature_avg(session_stats, users, user_stats))
	outputdata.append(groupdata)
return outputdata

def eval_focusness_groups(src_file, dictionary_file, userstats_file):
	query_dict={}
	user_stats={}
	for line in open(src_file):
		linedata=json.loads(line[line.index("{"):])
		query_dict[linedata['query']]=None
		for user in linedata['uids']:
			user_stats[user]=None
	query_dict=load_dictionary(dictionary_file,query_dict)
	user_stats=load_userstats(userstats_file,user_stats)
	session_stats=["js","rss","limit"]
	access_stats=["ip","ua"]
	outputdata=[]
	for line in open(src_file):
		linedata=json.loads(line)
		users=linedata['uids']
		groupdata=[["query", query_dict[linedata['query']]], ["users", len(linedata['uids'])],["group_queries", len(linedata['qhashes'])],["focusness", linedata['foc']]]
		groupdata.extend(top_shared_elmmt(access_stats, users, user_stats))
		groupdata.extend(feature_avg(session_stats, users, user_stats))
		outputdata.append(groupdata)
	return outputdata

'''
dictionary_file="dictionary" # file with query_hash -> query mappings
userstats_file="userstats" # file with user statistics 
src_file="focusness_groups" # file with focusness>0.9 groups 
outputdata=eval_focusness_groups(src_file, dictionary_file, userstats_file)
outputfname="focusness_groups_statistics"
output= open(outputfname,"w")

for groupdata in outputdata:
	for el in groupdata:
		output.write(el[0]+" "+str(el[1]))
		output.write("\n")
	output.write("-"*40)
	output.write("\n")
output.close()
'''


dictionary_file="dictionary" # file with query_hash -> query mappings
userstats_file="userstats" # file with user statistics 
src_file="bot_groupstats" # file with focusness>0.9 groups 
outputdata=eval_svd_groups(src_file, dictionary_file, userstats_file)
outputfname="svd_groups_statistics"
output= open(outputfname,"w")

for groupdata in outputdata:
	for el in groupdata:
		output.write(el[0]+" "+str(el[1]))
		output.write("\n")
	output.write("-"*40)
	output.write("\n")
output.close()
