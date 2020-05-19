"""
Imports
"""
import pandas as pd
import warnings
import requests
import json
import os

"""
Main Scripts
"""
## Loading static files
resource_filepath = os.path.join(os.path.dirname(__file__), 'stream_to_resource_id.json')
with open(resource_filepath, 'r') as fp:
    stream_to_id_map = json.load(fp)
    
URL = 'http://data.nationalgrideso.com'

## Main class
class Wrapper():        
    def NG_request(self, params={}):    
        url_root = self.get_url('datastore_search')

        params.update({'resource_id':self.resource_id})

        if 'sql' in params.keys():
            url_root += '_sql'

        r = requests.get(url_root, params=params)

        return r

    def get_package_resources(self, package_id: str):
        url = self.get_url('package_show')
        response = self.json_request(url, params={'id': package_id})
        return response['resources']

    def get_package_list(self):
        url = self.get_url('package_list')
        return self.json_request(url)

    def get_group_list(self):
        url = self.get_url('group_list')
        return self.json_request(url)

    def get_tag_list(self):
        url = self.get_url('tag_list')
        return self.json_request(url)

    def get_url(self, action: str, version: int = 3):
        return URL + '/api/{version}/action/{action}'.format(version=version, action=action)

    def json_request(self, url: str, params: dict = None):
        try:
            r = requests.get(url, params = params)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # TODO: log something
            raise e
        else:
            response = r.json()
            if response['success'] != True:
                # TODO: check the value of the "error" key.
                # {
                #     "help": "Creates a package",
                #     "success": false,
                #     "error": {
                #         "message": "Access denied",
                #         "__type": "Authorization Error"
                #         }
                #  }
                raise

            return response['result']

    def raise_(self, err_txt, error=ValueError): 
        raise error(err_txt)

    def check_request_success(self, r_json):
        if r_json['success'] == False:
            err_msg = r_json['error']['message']
            self.raise_(err_msg)

    date_between = lambda self, dt_col, start_date, end_date: f'SELECT * from "{self.resource_id}" WHERE "{dt_col}" BETWEEN \'{start_date}\'::timestamp AND \'{end_date}\'::timestamp ORDER BY "{dt_col}"' 
    date_less_than = lambda self, dt_col, date: f'SELECT * from "{self.resource_id}" WHERE "{dt_col}" < \'{date}\'::timestamp ORDER BY "{dt_col}"' 
    date_greater_than = lambda self, dt_col, date: f'SELECT * from "{self.resource_id}" WHERE "{dt_col}" > \'{date}\'::timestamp ORDER BY "{dt_col}"' 

    def form_dt_rng_sql_query(self, dt_col, start_date=None, end_date=None):
        start_end_date_exist = (start_date!=None, end_date!=None)

        func_map = {
            (False, False) : {'error' : 'A start and/or end date should be passed'},
            (True, True) : self.date_between(dt_col, start_date, end_date),
            (False, True) : self.date_less_than(dt_col, end_date),
            (True, False) : self.date_greater_than(dt_col, start_date),
        }

        sql = func_map[start_end_date_exist]

        if not isinstance(sql, str):
            self.raise_(sql['error'])

        return sql

    def query_API(self, params={}, start_date=None, end_date=None, dt_col=None, sql='', return_raw=False):
        ## Handling SQL queries
        if start_date or end_date:
            if sql != '':
                warnings.warn('The start and end date query will overwrite the provided SQL')

            if not dt_col:
                warnings.warn('If a start or end date has been provided the \'dt_col\' parameter must be provided')

            sql = self.form_dt_rng_sql_query(dt_col, start_date=start_date, end_date=end_date)
            params.update({'sql':sql})

        elif sql != '':
            params.update({'sql':sql})
            
        elif 'sql' in params.keys():
            params.pop('sql')

        ## Making the request
        r = self.NG_request(params=params)

        if return_raw == True:
            return r

        ## Checking and parsing the response
        r_json = r.json()
        self.check_request_success(r_json)

        df = pd.DataFrame(r_json['result']['records'])

        return df
    
    def assign_stream(self, stream):
        self.stream = stream
        self.resource_id = stream_to_id_map[self.stream]
        
    def __init__(self, stream):
        self.assign_stream(stream)
        self.streams = list(stream_to_id_map.keys()) 

    
if __name__ == "__main__":
    main()
