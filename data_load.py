import requests
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import sys
import time
from datetime import datetime


def load_data_subset(url) -> pd.DataFrame:
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


def load_full_data(url) -> pd.DataFrame:
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


#def eda(df:pd.DataFrame, run_eda_flag:bool):
#   pass



def transform_data(raw_data_df) :

    #area_df = raw_data_df.groupby('area_name').agg({'dr_no': 'count','time_occ':'mean', 'lat': 'mean', 'lon': 'mean'}).reset_index()
    #area_df.columns = ['area_name', 'num_crimes','avg_time_occ', 'avg_lat', 'avg_lon',]
    gdf = gpd.read_file("los-angeles.json")
    gdf = gdf[['name','cartodb_id','geometry']]
    #gdf.crs = "EPSG:4326"

    # Load your Pandas DataFrame
    df = raw_data_df[['dr_no','area_name','lon','vict_sex','lat','time_occ','date_rptd','date_occ']]
    #df.to_excel('output.xlsx', index=False)

    # Ensure the DataFrame has a 'geometry' column with Point geometries
    df['geometry'] = df.apply(lambda row: Point(row['lon'], row['lat']), axis=1)
    df = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry='geometry')


    # Perform the spatial join
    joined_data = gpd.sjoin(gdf,df, predicate='contains', how='inner')

    # Grouping by Location name
    joined_data['Male_victims'] = joined_data['vict_sex'].apply(lambda x: 1 if x == 'M' else 0)
    joined_data['Female_victims'] = joined_data['vict_sex'].apply(lambda x: 1 if x == 'F' else 0)
    joined_data['Unknown_sex'] = joined_data['vict_sex'].apply(lambda x: 1 if x not in ['M','F'] else 0)
    joined_data=joined_data.groupby(['name','geometry']).agg({
        'dr_no': 'count',
        'Male_victims':'sum',
        'Female_victims':'sum',
        'Unknown_sex': 'sum',
    }).reset_index()
    joined_data = joined_data.rename(columns={'dr_no':"Number_of_crimes" })
    return joined_data