__author__ = "Web Begole"
__version__ = "2.0.0"
__maintainer__ = "ExanteData"
__email__ = "info@exantedata.com"

import requests
import json
import inspect
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
TOKEN = None

API_URL = 'https://apidata.exantedata.com/'         # THIS SHOULD NOT BE CHANGED UNLESS ADVISED OTHERWISE
VERIFY_SSL = True                               # THIS SHOULD NOT BE CHANGED UNLESS ADVISED OTHERWISE

PROXIES = {
    # 'https': 'https://<IPADDR>:<PORT>'        # ONLY USE IF YOUR IT DEPARTMENT SPECIFIES
}






def _errorHandler(r):
    """
    Generic handler for errors returned from the API.

    :param r:
        Requests results.

    :return:
        None.

    """
    print ('\n---\nError retrieving data from API in {}'.format(inspect.stack()[1][3]))
    if 'ERROR' in r.json().keys():
        print('API Error Code: \t{}'.format(r.json()['ERROR']))
    if 'MESSAGE' in r.json().keys():
        print('API Error Message: \t{}'.format(r.json()['MESSAGE']))
    print ('---\n')
    return None



def _getToken(username='', password=''):

    """
    This private function retrieves the authentication token based on username and password supplied.
    You will pass this token in the header (as 'token') of any subsequent requests.

    :param username:
        String.
        Exante Data supplied username.
    :param password:
        String.
        Exante Data supplied password.
    :return:
        Authorization token to be used in header of subsequent requests, or False if error.
    """
    global TOKEN
    if TOKEN:
        return TOKEN                    # USES CACHED TOKEN FOR REPEATED REQUESTS

    payload = {
        "username": username,
        "password": password
    }

    headers = {
    }

    url = API_URL + 'getToken'

    r = requests.post(url, headers=headers, data=payload, proxies=PROXIES, verify=VERIFY_SSL)

    if r.status_code == 200 and r.json():
        jsonResponse = r.json()
        # print jsonResponse['MESSAGE']
        TOKEN = jsonResponse['TOKEN']
        return jsonResponse['TOKEN']
    else:
        _errorHandler(r)
        return False



def _checkLastUpdated(tickerQuery):
    """
    This private function checks the timestamp for when the data was last updated.

    :param tickerQuery:
        String for the ticker.  Uses MySQL % for wild-card carding and comma-separation for multiple requests.

    :return:
        Dictionary of timestamps keyed by Ticker.
    """

    payload = {
        'ticker': tickerQuery,
    }

    headers = {
        'Authorization': 'Bearer ' + _getToken()
    }

    url = API_URL + 'Data/Updated'

    r = requests.post(url, headers=headers, data=(payload), proxies=PROXIES, verify=VERIFY_SSL)
    
    if r.status_code == 200 and r.json():
        jsonResponse = r.json()
        # print (jsonResponse['MESSAGE'])
        return jsonResponse['UPDATED']
    else:
        _errorHandler(r)
        return False


def _getMetaData(tickerQuery):
    """
    This private function collects the available meta-data for the tickers including the last updated timestamp
    and the last value.

    :param tickerQuery:
        String for the ticker.  Uses MySQL % for wild-card carding and comma-separation for multiple requests.

    :return:
        Dictionary of meta-data keyed by Ticker.  Under each Ticker is a dictionary of available meta-data.

    """
    payload = {
        'ticker': tickerQuery,
    }

    headers = {
        'Authorization': 'Bearer ' + _getToken()
    }

    url = API_URL + 'Data/Metadata'

    r = requests.post(url, headers=headers, data=(payload), proxies=PROXIES, verify=VERIFY_SSL)
    if r.status_code == 200 and r.json():
        jsonResponse = r.json()
        # print (jsonResponse['MESSAGE'])
        return jsonResponse['METADATA']
    else:
        _errorHandler(r)
        return False

def _getLastValue(tickerQuery):
    """
    This private function collects the available meta-data for the tickers including the last updated timestamp
    and the last value.

    :param tickerQuery:
        String for the ticker.  Uses MySQL % for wild-card carding and comma-separation for multiple requests.

    :return:
        Dictionary of meta-data keyed by Ticker.  Under each Ticker is a dictionary of available meta-data.

    """
    payload = {
        'ticker': tickerQuery,
    }

    headers = {
        'Authorization': 'Bearer ' + _getToken()
    }

    url = API_URL + 'Data/Last'

    r = requests.post(url, headers=headers, data=(payload), proxies=PROXIES, verify=VERIFY_SSL)

    if r.status_code == 200 and r.json():
        jsonResponse = r.json()
        # print (jsonResponse['MESSAGE'])
        return jsonResponse['DATA']
    else:
        _errorHandler(r)
        return False



def _getData(tickerQuery, startDate, endDate, end_of_period = True):
    """
        This is a private function that does the API call (including requesting the token prior) and returns a dictionary
         of the raw API output.

        For the sake of speed, when creating a complex script with potentially multiple API calls, you may wish to save
        the authorization token out into a global variable to re-use within this function.

    :param tickerQuery:
        String for the ticker.  Uses MySQL % for wild-card carding and comma-separation for multiple requests.
    :param startDate:
        String in format: YYYY-MM-DD
        If none provided it will return the -30 from endDate.
    :param endDate:
        String in format: YYYY-MM-DD
        If none provided it will default to today.
    :param end_of_period:
        Set to False for Beginning-of-period.
        Defaults to True.
    :return:
        Dictionary of time series data keyed by Ticker.  Under each ticker, the time series is keyed by date.

    """

    period = 'begin'
    if end_of_period:
        period = 'end'

    payload = {
        'ticker': tickerQuery,
        'startDate': startDate,
        'endDate': endDate,
        'period': period
    }

    headers = {
        'Authorization': 'Bearer ' + _getToken()
    }

    url = API_URL + 'Data/Data'

    r = requests.post(url, headers=headers, data=(payload), proxies=PROXIES, verify=VERIFY_SSL)
    if r.status_code == 200 and r.json():
        jsonResponse = r.json()
        # print (jsonResponse['MESSAGE'])
        return jsonResponse['DATA']

    else:
        _errorHandler(r)
        return False



if __name__ == '__main__':
    """"""
    username = ''                                          # EMAIL ADDRESS OF ACCOUNT AT exantedata.com
    password = ''                                          # YOUR PASSWORD AT exantedata.com

    """"""

    
    """
    MAIN FUNCTION CALLS
    
    """
    tickerQuery = 'CN.CBINT.M'        # Comma separated tickers, use % for wildcards.

    startDate = '2020-01-01'            # FORMAT: YYYY-MM-DD .  SET TO None FOR endDate MINUS 6M.
    endDate = None                      # FORMAT: YYYY-MM-DD .  SET TO None FOR MOST RECENT DATA

    end_of_period = True                # Set to False for Beginning-of-period


    token = _getToken(username, password)

    if token:
        print ('Access Token: {}\n'.format(token))

        data_updated = _checkLastUpdated(tickerQuery)
        data_metadata = _getMetaData(tickerQuery)
        data_data = _getData(tickerQuery, startDate, endDate, end_of_period)
        data_lastdata = _getLastValue(tickerQuery)

        if data_updated:
            print ('\n\nData last updated: \n')
            if PANDAS_AVAILABLE:
                print (pd.DataFrame.from_dict({'UPDATED':data_updated}))
            else:
                print (data_updated)

        if data_metadata:
            print ('\n\nTicker Meta Data: \n')
            if PANDAS_AVAILABLE:
                df = pd.DataFrame.from_dict(data_metadata)
                print (df)
                df.to_csv('metadata_from_api.csv')
            else:
                print (data_metadata)

        if data_data:
            print ('\n\nTicker Data: \n')
            if PANDAS_AVAILABLE:
                df = pd.DataFrame.from_dict(data_data)
                df.index = pd.to_datetime(df.index)
                print (df)
                df.to_csv('data_from_api.csv')
            else:
                print (data_data)

        if data_lastdata:
            print ('\n\nTicker Data: \n')
            if PANDAS_AVAILABLE:
                df = pd.DataFrame.from_dict(data_lastdata)
                df.index = pd.to_datetime(df.index)
                print (df)
            else:
                print (data_lastdata)
