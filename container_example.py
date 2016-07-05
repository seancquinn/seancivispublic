# This code imports various python libraries that will be used later on.
import os
import requests
import sys
import time
from time import sleep
import json
import pandas as pd

endpoint = "https://api.civisanalytics.com"
api_key = os.environ['CIVIS_API_KEY']

def main():
	parser = argparse.ArgumentParser(description='Import data from redshift and write it back')
    parser.add_argument('--database', default=None, type=int, help='Database Id of the database to query')
    parser.add_argument('-s', '--schema', default=None, type=str, help='Schema to import the file to')
    parser.add_argument('-t', '--table', default=None, type=str, help='Table to import the file to')
    parser.add_argument('--sql', default=None, type=str, help='sql to run when querying the database')
    args = parser.parse_args()

	source_database_id = args.database
	sql = args.sql

	df = query_to_pandas(sql,source_database_id)

	destination_database_id = args.database
	schema = args.schema
	table = args.table
	
	pandas_to_redshift(df, schema, table, destination_database_id)

def query_to_pandas(sql,database_id,preview_rows = 100):
  # The access point for Civis' API. You should not change this.
	endpoint = "https://api.civisanalytics.com"
	
	# Enter your API Key here
	api_key = os.environ['CIVIS_API_KEY']
	
	# A POST API request to the /queries endpoint. Uses the database_id determined earlier
	# and the sql string specified above to run the query. In this example, 'previewRows' is
	# set to 20, but you may change that to anything less than or equal to 100.
	query = requests.post(endpoint + '/queries',
				auth=requests.auth.HTTPBasicAuth(api_key, ''),
				json={"database":database_id,"sql":sql,"previewRows":preview_rows}).json()
	
	# Get the ID of the query we just created
	query_id = query['id']
	
	# Use a GET call to check the status of the query
	query_results = requests.get(
		endpoint + '/queries/%d' % query_id,
		auth = requests.auth.HTTPBasicAuth(api_key, ''),
		json={}).json()
	
	# If the next step in your process should wait until the query above finishes running
	# you can use this block of code to check the status every 5 seconds
	while query_results['state'] in ('running','queued'):
		print("Waiting for query to complete. Query is %s" % query_results['state'])
		sleep(5) # waits for 5 seconds. Can change 5 to another number if you want to wait a different number of seconds
		# After the 5 seconds have passed, checks the status of the query again.
		query_results = requests.get(
			endpoint + '/queries/%d' % query_id,
			auth = requests.auth.HTTPBasicAuth(api_key, ''),
			json={}).json()
	
	if query_results['state'] in ('failed','cancelled'):
		print("Query Failed")
		raise ValueError(query_results['exception'])
	else:
		print("Query Succeeded")
	
	df = pd.DataFrame.from_records(query_results['resultRows'],columns = query_results['resultColumns'])
	return df

def get_default_credential():
	get_cred = requests.get(endpoint + '/credentials',
													auth=requests.auth.HTTPBasicAuth(api_key, ''),
													json={"type":"Database"}).json()


	for c in get_cred:
		if c['remoteHostId'] is None:
			return c['id']


def pandas_to_redshift(dataframe, schema, table, database_id, credential_id = None, 
											existing_table_rows = 'append', filestring = '/tmp/dataframe.csv'):

	dataframe.to_csv(filestring, index=False)
	if credential_id is None:
		credential_id = get_default_credential()

	import_one = requests.post(endpoint + '/imports/files',
														auth=requests.auth.HTTPBasicAuth(api_key, ''),
														json={"schema":schema,"name":table,
																	"remoteHostId":database_id,"credentialId":credential_id,
																	"existingTableRows":existing_table_rows}).json()
	# Get the returned "uploadUri" parameter 
	uploadUri = import_one['uploadUri']
	
	# Get the returned "runUri" parameter
	runUri = import_one['runUri']
	
	put_file = requests.put(uploadUri,
							data=open(filestring, 'rb'))

	put_file.raise_for_status()
  
	# POST request to the runUri to actually run the Import job you created earlier
	run_import = requests.post(runUri,
														auth=requests.auth.HTTPBasicAuth(api_key, ''),
														json={}).json()

	# Get the ID of the import job
	import_id = run_import['importId']

	# Get the ID of the individual run for that job
	run_id = run_import['id']
	
	# Check on the status of the Import Job
	import_status = requests.get(endpoint + '/imports/files/%d/runs/%d' % (import_id, run_id),
															auth=requests.auth.HTTPBasicAuth(api_key, ''),
															json={}).json()
	
	# Wait until the import job completes by checking it's status every 5 seconds
	while import_status['state'] in ('queued','running'):
		print("Waiting for import %d to complete" % import_id)
		sleep(5)
		import_status = requests.get(endpoint + '/imports/files/%d/runs/%d' % (import_id,run_id),
																auth=requests.auth.HTTPBasicAuth(api_key, ''),
																json={}).json()

	if import_status['state'] in ('failed','cancelled'):
		print("Import Failed")
		raise ValueError(import_status['error'])
	else:
		print("Import %d Succeeded" % import_id)

if __name__ == '__main__':
  main()
