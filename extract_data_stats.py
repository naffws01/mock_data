import pandas as pd
from glob import glob
from datetime import datetime
import os

basins = ['Kerian', 'Muda', 'Padas', 'Sarawak']
obs_types = ['RF', 'WL']

def convert_timestamp(t, fmt1, fmt2):
	t_obj = datetime.strptime(t, fmt1)
	t_fmt = datetime.strftime(t_obj, fmt2)
	return t_fmt

for observable in obs_types:
	for basin in basins:
		locs = []
		data_path = './{0}/DID'.format(basin)
		files = sorted(glob('{0}/*{1}.txt'.format(data_path, observable)))
		
		tfmt1a = '%Y-%m-%d,%H:%M:%S'
		tfmt1b = '%Y-%d-%m,%H:%M:%S'
		tfmt2 = '%Y-%m-%d %H:%M:%S'
		
		min_obs = {}
		max_obs = {}
		last_timestamp = {}
		
		for infile in files:
			print('{0} | {1} | {2}'.format(observable, basin, infile))
			target = os.path.basename(infile).split('.')[0]
			df = pd.DataFrame()
			try:
				df = pd.read_csv(infile, header=None, sep=';')
			except:
				continue
			if df.shape[0] > 0:
				df.columns = ['date_time', 'obs']
				df['obs'] = df.obs.str.rstrip('#')
				df['obs'].replace('', -9999., inplace=True)
				df['obs'] = df.obs.apply(lambda x: float(x))
				try:
					df['date_time'] = df.date_time.apply(lambda x: convert_timestamp(x, tfmt1a, tfmt2))
				except:
					df['date_time'] = df.date_time.apply(lambda x: convert_timestamp(x, tfmt1b, tfmt2))
				locs.append(target)
				last_timestamp[target] = df.date_time.values[-1]
				df['obs'].replace(-9999., np.nan, inplace=True)
				min_obs[target] = np.nanmin(df.obs.values)
				max_obs[target] = np.nanmax(df.obs.values)
		DF = pd.DataFrame(last_timestamp.items(), columns=['filename', 'last_timestamp'])
		DF['min_obs'] = DF.filename.apply(lambda x: min_obs[x])
		DF['max_obs'] = DF.filename.apply(lambda x: max_obs[x])
		DF.to_csv('summary_{0}_data_{1}.txt'.format(observable.lower(), basin), index=None)

