from scipy.sparse.linalg import svds
from scipy.sparse.linalg import norm
from scipy.sparse import csc_matrix
from collections import defaultdict
import os
import json
import numpy as np
import time

def load_groupdata(line):
        jsdata=json.loads(line)
        query=jsdata['query']
        url=jsdata['url']
        rows=np.array(jsdata['rows']) ## row (query) indices
        cols=np.array(jsdata['cols']) ## column (user) indices
        orig_data=np.array(jsdata['data']) ## data at given indices
        bin_data=np.arange(1.0*len(orig_data)) ## binarized data
        xshape=len(jsdata['qhashes'])
        yshape=len(jsdata['uids'])
        mat=csc_matrix((bin_data,(rows,cols)),shape=(xshape,yshape))
        query_hashes=np.array(jsdata['qhashes']) # query hashes
        udata=np.array(jsdata['uids']) # list of user ids
        foc=jsdata['foc'] # group focusness 
        return mat, orig_data,query,url,query_hashes,foc,udata

def flip_svd_res(usvt):
	usvt[0]=np.fliplr(usvt[0])
	usvt[1][-1]=usvt[1][-2]
	usvt[1][-2]=0
	usvt[2]=np.flipud(usvt[2])
	return usvt

def get_svd_res(mat, components):
	usvt= svds(mat,k=components,which='LM') ## compute svd
	if usvt[1][-1]==0: usvt=flip_svd_res(usvt)
	enrgy=(usvt[1][-1]/norm(mat)) ## energy of first principal component
	var=enrgy**2 ## variance of first principal component
	return usvt, enrgy, var

def bot_data(mat,orig_data,u, qhashes, uids,k):
	'''
	Finds the nearest to the 1st Principal Component users and returns the hashes of queries they clicked and
	their respective user ids  
	'''
	utx=((u*(u.T))*mat)**2
	b_col_norms=mat.sum(axis=0).A1 ## A1 returns flattened array
	bot_user_indices=np.argsort(np.abs(b_col_norms-utx))[-k:]
	mat.data=orig_data
	qsums=mat.sum(axis=1)
	clicksums=mat[:,bot_user_indices].sum(axis=1)
	bot_query_inds=np.argsort(clicksums,axis=0)[-100:]
	clicks=[s[0][0].A1[0] for s in  qsums[bot_query_inds]]
	query_hashes=[s[0] for s in qhashes[bot_query_inds]]
	return query_hashes,clicks, uids[bot_user_indices]

def run_svds(srcfname,savedir,var_thresh=0.6,components=2,verbose=True, savemat=False):
	if savemat:
		matdata_dir=savedir+"/matrices/"
		if not os.path.exists(matdata_dir):
			os.mkdir(matdata_dir)
	lines=0
	skipped=[]
	group_stats=open('groupstats','w')
	bot_groupstats=open('bot_groupstats','w')
	start=time.time()
	for line in open(srcfname):
		lines+=1
		if verbose and lines%1000==0: print '%d lines processed, %.1f elapsed' %(lines,time.time()-start)
		mat,orig_data,query,url,qhashes,foc, udata=load_groupdata(line)
		if foc>0.9: continue
		if mat.shape[0]<components+1 or mat.shape[1]<components+1:
			skipped.append([query,url])
			continue
		usvt, enrgy, var = get_svd_res(mat,components)
		group_stats.write(json.dumps({'query': query, 'url': url, 'var': var}) + "\n")
		if var>0.6:
			k=int(mat.shape[1]*enrgy) # number of selected usrs, eq from section 3.3 of paper k=m*(E1/E)
			mat.data=orig_data
			if savemat:
				fname=str(query)+""+str(url)
				np.save(matdata_dir+fname,[mat,usvt[0][:,-1], query, url, foc,qhashes,udata,k,var])
			b_qhashes, b_clicks, botids=bot_data(mat,orig_data,usvt[0][:,-1], qhashes, udata,k)
			bot_groupstats.write(json.dumps({'query': query, 'url': url,'foc': foc, 'var': var, 'qhashes': list(b_qhashes), 'clicks': list(b_clicks) ,'uids': list(botids)}) + "\n")
			bot_groupstats.close()
			break
	return skipped





savedir="/auto/brno2/home/tomak/eval/var"
srcfile="/auto/brno2/home/tomak/eval/all"

d=run_svds(srcfile,savedir, savemat=True)

