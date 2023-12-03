import json

if __name__ == '__main__':
    import argparse
    import datetime
    from datetime import date
    import pandas as pd
    import geopandas as gpd
    import json
    import plotly.express as px
    from openpyxl.workbook import Workbook
    from data_load import load_full_data, transform_data

    parser = argparse.ArgumentParser()
    parser.add_argument('--date',
                        dest='date',
                        type=lambda d:datetime.datetime.strptime(d, '%Y-%m-%d').date(),
                        default=date.today()-datetime.timedelta(days=360))  # Corrected the default value

    parser.add_argument('--limit',
                        dest='limit',
                        type=int,
                        default=10000)
    args = parser.parse_args()

    crime_date = args.date
    rows_limit = args.limit
    api_url = f"https://data.lacity.org/resource/2nrs-mtv8.json?$where=date_occ>'{crime_date}T00:00:00.000'&$limit={rows_limit}"

    def main(url=api_url) -> pd.DataFrame:
        raw_data_df = load_full_data(url)
        transform_data_df = transform_data(raw_data_df)
        return transform_data_df


    def create_la_map(transform_data_df):
        df = transform_data_df
        file_path = "los-angeles.json"
        with open(file_path, 'r') as f:
             geojson = json.load(f)

        fig = px.choropleth_mapbox(df, geojson=geojson, color="Number_of_crimes",
                            locations="name",featureidkey="properties.name",
                            #range_color=(0,25),
                            color_continuous_scale="Viridis",
                            labels={"inter_pct": "Marriage (%)"},
                            center={"lat": 34.0522, "lon": -118.2437},
                            title= f"Resolve crimes in LA from {crime_date}" ,
                            hover_data={'Male_victims','Female_victims','Unknown_sex'},
                            zoom= 9,
                            opacity= 0.7,
                            mapbox_style="open-street-map"
                        )

        fig.update_geos(fitbounds="locations", visible=True)
        fig.update_layout( coloraxis_colorbar=dict(
                bgcolor="rgba(22,33,49,1)",
                tickfont=dict(
                    color="rgba(255,255,255,1)"
                ),)
        )




        return fig


    # Call the main function
    result_df = main()
    fig = create_la_map(result_df)
    fig.show()

    result_df.to_excel('output.xlsx', index=False)