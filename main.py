import json

if __name__ == '__main__':
    import argparse
    import datetime
    from datetime import date
    import pandas as pd
    import geopandas as gpd
    import json
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    from data_load import load_full_data, transform_data

    parser = argparse.ArgumentParser()
    parser.add_argument('--date',
                        dest='date',
                        type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date(),
                        default=date.today() - datetime.timedelta(days=360))  # Corrected the default value

    parser.add_argument('--limit',
                        dest='limit',
                        type=int,
                        default=10000)
    args = parser.parse_args()

    crime_date = args.date
    rows_limit = args.limit
    api_url = f"https://data.lacity.org/resource/2nrs-mtv8.json?$where=date_rptd>'{crime_date}T00:00:00.000'&$limit={rows_limit}"

    def main(url=api_url) -> pd.DataFrame:
        raw_data_df = load_full_data(url)
        # Transform data
        transform_data_df = transform_data(raw_data_df)
        return transform_data_df  # You should return the result of your main function

    def create_la_map(transform_data_df):
        # max_marker_size = 70
        # largest_num_crimes = transform_data_df['num_crimes'].max()
        # marker_size = (transform_data_df['num_crimes'] / largest_num_crimes) * max_marker_size
        # fig = go.Figure(go.Scattermapbox(
        #     lon = transform_data_df['avg_lon'],
        #     lat = transform_data_df['avg_lat'],
        #     mode='markers',
        #     marker=dict(
        #     size= marker_size, #transform_data_df['num_crimes'],  # You can adjust the marker size as needed
        #     color=transform_data_df['avg_time_occ'],
        #     colorbar=dict(title='time to solve'),  # Set the color bar title
        #     colorscale='Viridis',  # Choose the color scale you prefer
        #     showscale=True,  # Show the color scale
        # ),
        #     text=transform_data_df[['area_name', 'num_crimes', 'avg_time_occ']],
        # ))
        # fig.update_layout(
        #     mapbox_style="open-street-map",
        #     mapbox={'center': go.layout.mapbox.Center(lon=-118.2437, lat=34.0522), 'zoom': 10}
        # )
        df = transform_data_df
        file_path = "los-angeles.json"
        with open(file_path, 'r') as f:
             geojson = json.load(f)

        fig = px.choropleth_mapbox(df, geojson=geojson, color="time_occ",
                            locations="name",featureidkey="properties.name",
                            range_color=(0,25),
                            color_continuous_scale="Viridis",
                            labels={"inter_pct": "Marriage (%)"},
                            center={"lat": 34.0522, "lon": -118.2437},
                            title= f"Resolve crimes in LA from {crime_date}" ,
                            zoom= 9,
                            opacity= 0.5,
                            mapbox_style="open-street-map"
                        )

        fig.update_geos(fitbounds="locations", visible=True)
        fig.update_layout( coloraxis_colorbar=dict(
                bgcolor="rgba(22,33,49,1)",
                tickfont=dict(
                    color="rgba(255,255,255,1)"
                ),)
        )
        # scatter_trace = px.scatter_mapbox(df, lat="lat", lon="lon", size_max=15, zoom=10)

        # # You can customize the scatter trace here as needed

        # fig.data += scatter_trace.data



        return fig


    # Call the main function
    result_df = main()
    fig = create_la_map(result_df)
    fig.show()
    #print(result_df.head())