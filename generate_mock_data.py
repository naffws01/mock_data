import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime
import time
import os

basins = ['Kerian', 'Muda', 'Padas', 'Sarawak']
obs_types = ['RF', 'WL']

while(True):
	t_now = datetime.now()
	YYYY = t_now.year
	mm = t_now.month
	dd = t_now.day
	HH = t_now.hour
	MM = t_now.minute
	SS = 0
	
	if MM % 15 == 0:
		timestamp = datetime.strftime(t_now, '%Y-%m-%d,%H:%M:00')
		print('Workflow initiated at: {0}'.format(timestamp))
		for observable in obs_types:
			for basin in basins:
				infile = '.\summary_{0}_data_{1}.txt'.format(observable.lower(), basin)
				data_path = '.\mock_data\{0}\DID'.format(basin)
				df = pd.read_csv(infile)
				
				for idx, row in df.iterrows():
					filename = row['filename']
					obs = row['min_obs']
					if np.isnan(obs):
						obs = -9999.
					data_string = '{0};{1}#'.format(timestamp, obs)
					
					if not os.path.exists(data_path):
						os.system('mkdir {0}'.format(data_path))
					
					outfile = '{0}/{1}.txt'.format(data_path, filename)
					if not os.path.exists(outfile):
						os.system('echo.> {0}'.format(outfile))
					
					os.system('echo {0} >> {1}'.format(data_string, outfile))
		print('All tasks completed. Go to sleep till next cycle .. ')
	time.sleep(60)

