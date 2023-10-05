import requests
import json
import pandas as pd
import sys
import time
from datetime import datetime


def load_data_subset(url:str) -> pd.DataFrame:
    """Returns a subset of the data based on a "N" limit of rows loaded from the given API's URL.
    \nAfter the second iteration, the function will start with an offset equal to the limit pecified 
    \nin the URL.

    Args:
        URL (str): This URL includes a starting point and a limit (per iteration) 
                    on the number of rows that will be returned - default 1K.

    Returns:
        pd.DataFrame: A data frame containing a subset of incremental data from the API
    """    
    try:
        # Open new connection:
        response = requests.get(url)
        if response.status_code == 200:
            try:
                data = response.json()
                response.close()
            except Exception as link_error:
                print(link_error)
        else:
            err = (f"\n{'*'*65}\nError in response: \n\nStatus from API: {response.status_code}"
                   f"\nError: \n{response.text}\nLink: {url}\n\n{'*'*65}")
            print(err)

        # re-write to json file:
        with open('data.json', 'w') as f:
            f.truncate(0)
            json.dump(data, f)

        # json to pandas df:
        df = pd.read_json('data.json', orient='records')
    except Exception as e:
        print(f'\nAn error occurred: \n{e}\n\nLink: {url}\n')
        sys.exit()

    return df


def load_full_data(url:str) -> pd.DataFrame:
    """This function loads the full data set from the API from the given beginning date by running 
    \nload_data_subset multiple times

    Args:
        url (str): URL of API with offset and limit parameters.

    Returns:
        pd.DataFrame: A data frame containing a full set of data from the API within a given 
        \n\tperiod of time.
    """    
    crime_df = pd.DataFrame()
    fetch_full_data, offset_mul = True, 0
    start_time = time.time()

    if fetch_full_data == True:
        # init starting link:
        url = url

        while True:
            inc_df = load_data_subset(url)

            # run until api retrieve data:
            if len(inc_df) > 0:
                crime_df = pd.concat([crime_df, inc_df])

                # Over the previous iteration, increase the offset by one
                offset_mul += 1

                # create next-link (increase offset for next iteration)
                url = url[:103] + f'&$offset={80000 * offset_mul}'
                print(f'Loaded: {len(crime_df):,}')
            else:
                break

        print('Done!')
        end_time = time.time()

    # Send a message indicating that the loading has been successful.
    comleted_load_msg = ("\n"
                         f"{'*'*108}\nLoad succesed! Start time: {datetime.now()}, elapsed time (sec): {(end_time - start_time):.2f}, "
                         f"Total Rows Loaded: {len(crime_df):,}\n{'*'*108}"
                         )

    print(comleted_load_msg)
    return crime_df


def clean_data(df:pd.DataFrame) -> pd.DataFrame:
    # include eda + cleaning of the data
    pass


def transform_data(df:pd.DataFrame) -> pd.DataFrame:
    pass


def render_graph_html(df:pd.DataFrame) -> pd.DataFrame:
    pass


def main(url:str):
    raw_data_df = load_full_data(url)

    # Clean data
    clean_data_df = clean_data(raw_data_df)

    # Transform data
    transform_data_df = clean_data(clean_data_df)

    # Render Graph inside html page:

    return raw_data_df


if __name__ == '__main__':
    api_url = 'https://data.lacity.org/resource/2nrs-mtv8.json?$where=date_rptd>"2022-01-01T00:00:00.000"&$limit=80000'

    # load crimes to data freame
    df = main(api_url)

    print(df.head(5))
    print(f'Max date inside dataframe : {df.date_rptd.max()}')



    