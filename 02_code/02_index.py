import madina as md
import geopandas as gpd
import pandas as pd
import math
from shapely.geometry import box
import madina.una.tools as una
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
from concurrent.futures import ProcessPoolExecutor



def process_record(origin, collison, nycstreet):
    try:
        # Time
        mask = (collison['DATETIME'] >= origin['DATETIME'] - pd.Timedelta(days=30)) & \
               (collison['DATETIME'] < origin['DATETIME'])

        # Space
        delta_lat = 2 / 111  
        delta_lon = 2 / (111 * math.cos(math.radians(origin['LATITUDE'])))

        min_lat = origin['LATITUDE'] - delta_lat
        max_lat = origin['LATITUDE'] + delta_lat
        min_lon = origin['LONGITUDE'] - delta_lon
        max_lon = origin['LONGITUDE'] + delta_lon
        bounding_box = box(min_lon, min_lat, max_lon, max_lat)

        # Slice
        destination = collison.loc[mask]
        space = nycstreet[nycstreet['geometry'].intersects(bounding_box)]
        destination = destination[destination['geometry'].within(bounding_box)]

        if destination.shape[0] == 0 or space.shape[0] == 0:
            gdf = origin.copy()
            gdf = gdf.to_crs('EPSG:32619')
            gdf['gravity_to_previous'] = 0
        else:
            # Weight
            destination['duration'] = (origin['DATETIME'] - destination['DATETIME']).dt.total_seconds() / 3600
            destination['weight'] = 1 / destination['duration']

            # Projection
            space = space.to_crs('EPSG:32619')
            origin = origin.to_crs('EPSG:32619')
            destination = destination.to_crs('EPSG:32619')

            # Load Layers
            nyc = md.Zonal()
            nyc.load_layer(name='street', source=space)
            nyc.load_layer('collison', source=origin)
            nyc.load_layer('time_lag', source=destination)

            # Create network
            nyc.create_street_network(source_layer="street")
            nyc.insert_node(label='origin', layer_name="collison")
            nyc.insert_node(label='destination', layer_name="time_lag")

            nyc.create_graph()

            una.accessibility(
                nyc,
                search_radius=3000,
                beta=0.001,
                save_gravity_as='gravity_to_previous',
                destination_weight='duration'
            )

            gdf = nyc['collison'].gdf
        return gdf

    except Exception as e:
        print(f"Error processing origin: {origin['id']} with error: {e}")
        return pd.DataFrame()



def main():
    # upload data
    collison = gpd.read_file('../01_data/03_intermediate/Collision_21_23/Collision.shp')
    nycstreet = gpd.read_file("../01_data/03_intermediate/cut/cut.shp")

    # set up
    collison['DATETIME'] = pd.to_datetime(collison['CRASH DATE'] + ' ' + collison['CRASH TIME'])
    

    results = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_record, row, collison, nycstreet) for index, row in collison.iterrows()]
        for future in futures:
            result = future.result()
            if not result.empty:
                results.append(result)

    # results
    final_results = pd.concat(results, ignore_index=True)
    final_results.to_file('../01_data/03_intermediate/index/index.shp', driver='ESRI Shapefile')

if __name__ == "__main__":
    main()