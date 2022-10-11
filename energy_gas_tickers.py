# Script to get the series of gas flows from Russia to Europe in hourly frequency 

import requests
import json
import inspect
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


"""
Fill in the following fields.

"""

username = ''      # YOUR ASSIGNED USERNAME AT exantedata.com i.e. maria.castaneda@exantedata.com
password = ''      # YOUR PASSWORD AT exantedata.com
ticker = 'RU.NSGAS.GER.FLOWS.H'    # Use one of the following tickers RU.NSGAS.GER.FLOWS.H (Nord Stream), RU.YAMALGAS.FLOWS.H (Yamal) or RU.UKGAS.FLOWS.H (Ukraine)

startDate = '2021-03-01'            # FORMAT: YYYY-MM-DD .  SET TO None FOR endDate MINUS 6M.
endDate = None                # FORMAT: YYYY-MM-DD .  SET TO None FOR MOST RECENT DATA

""""""




TOKEN = None
API_URL = 'https://apidata.exantedata.com/'       # THIS SHOULD NOT BE CHANGED UNLESS ADVISED OTHERWISE

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


def _getToken():

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
    global username, password, TOKEN
    if TOKEN:
        return TOKEN                    # USES CACHED TOKEN FOR REPEATED REQUESTS

    payload = {
        "username": username,
        "password": password
    }

    headers = {
        'Content-type': 'application/json',
    }

    url = API_URL + 'getToken'

    r = requests.post(url, headers=headers, data=json.dumps(payload), verify=True)

    if r.status_code == 200 and r.json():
        jsonResponse = r.json()
        # print jsonResponse['MESSAGE']
        TOKEN = jsonResponse['TOKEN']
        return jsonResponse['TOKEN']
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

    period = 'bop'
    if end_of_period:
        period = 'eop'

    payload = {
        'ticker': tickerQuery,
        'startDate': startDate,
        'endDate': endDate,
        'period': period,
    }

    headers = {
        'Content-type': 'application/json',
        'Authorization': 'Bearer ' + _getToken(),
    }

    url = API_URL + 'Data/Data'

    r = requests.post(url, headers=headers, data=json.dumps(payload), verify=True)
    if r.status_code == 200 and r.json():
        jsonResponse = r.json()
        # print (jsonResponse['MESSAGE'])
        return jsonResponse['DATA']

    else:
        _errorHandler(r)
        return False


if __name__ == '__main__':
    """
    MAIN FUNCTION CALLS
    
    """
    
    tickerQuery = ticker # Comma separated tickers, use % for wildcards.

    print(tickerQuery)
    
    #global startDate, endDate
    
    end_of_period = True                      # Set to False for Beginning-of-period


    token = _getToken()   
    if token:
        print ('Access Token: {}\n'.format(token))
        data_data = _getData(tickerQuery, startDate, endDate, end_of_period)

        if data_data:
            print ('\n\nTicker Data: \n')
            if PANDAS_AVAILABLE:
                print (pd.DataFrame.from_dict(data_data))
                pd.DataFrame.from_dict(data_data).to_csv('data_from_api.csv')
            else:
                print (data_data)
