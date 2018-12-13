import pandas as pd
from datetime import timedelta
from geopy.distance import great_circle


def retrieve_neighbors(index_center, data, eps_spatial, eps_temporal):
    neigborhood = []
    center_point = data.loc[index_center]

    # filter by time
    min_time = center_point['initial_time'] - timedelta(seconds=eps_temporal)
    max_time = center_point['end_time'] + timedelta(seconds=eps_temporal)
    data = data[((data['initial_time'] >= min_time) & (data['initial_time'] <= max_time)) |
                ((data['end_time'] >= min_time) & (data['end_time'] <= max_time))]

    # filter by distance
    for index, point in data.iterrows():
        if index != index_center:
            distance = great_circle(
                (center_point['latitude'], center_point['longitude']),
                (point['latitude'], point['longitude'])).meters
            if distance <= eps_spatial:
                neigborhood.append(index)

    return neigborhood

def st_dbscan(data, eps_spatial, eps_temporal, min_neighbors):
        cluster = 0
        unmarked = 888888
        noise = -1
        stack = []
        data['cluster'] = unmarked
        for index, point in data.iterrows():
            if data.loc[index]['cluster'] == unmarked:
                neighborhood = retrieve_neighbors(index, data, eps_spatial, eps_temporal)

                if len(neighborhood) < min_neighbors:
                    data.at[index, 'cluster'] = noise
                else:
                    cluster += 1
                    data.at[index, 'cluster'] = cluster
                    for neig_index in neighborhood:
                        data.at[neig_index, 'cluster'] = cluster
                        stack.append(neig_index)  # append neighborhood to stack
                    # find new neighbors from core point neighborhood
                    while len(stack) > 0:
                        current_point_index = stack.pop()
                        new_neighborhood = retrieve_neighbors(current_point_index, data, eps_spatial, eps_temporal)

                        # current_point is a new core
                        if len(new_neighborhood) >= min_neighbors:
                            for neig_index in new_neighborhood:
                                neig_cluster = data.loc[neig_index]['cluster']
                                if any([neig_cluster == noise,
                                        neig_cluster == unmarked]):
                                    data.at[neig_index, 'cluster'] = cluster
                                    stack.append(neig_index)
        return data

def clustering(data, eps_spatial, eps_temporal):
    """
    Cluster alerts

    Input:
    data :: pd.Dataframe with columns --> latitude, longitude, initial_time, end_time, uuid (grouped by uuid)
    eps_spatial :: float representing the temporal epsilon in seconds
    eps_temporal :: float representing spatial epsilon in meters

    output:
        clusters ::  list of dict. Each dict with the keys
              'cluster_id': int or string,
              'alerts_uuid': list of uuids,
              'initial_time' : timestamp,
              'end_time' : timestamp,
              'total_time' : timestamp,
              'estimated_center' : {'latitude': float, 'longitude': float}
      """

    if not data.empty:
        data = st_dbscan(data, eps_spatial, eps_temporal, 0).sort_values('cluster')
        clusters = []
        cluster = 0
        uuid = []
        lat = 0
        long = 0
        initial_time = 0
        end_time = 0
        for index, alert in data.iterrows():
            if alert['cluster']!=cluster:
                if uuid:
                    clusters.append({'cluster':cluster, 'uuids':uuid, 'initial_time':initial_time,
                                       'end_time':end_time, 'total_time':end_time-initial_time,
                                      'estimated_center' : {'latitude': lat/len(uuid), 'longitude': long/len(uuid)}})
                uuid = []
                lat = 0
                long = 0
                initial_time = alert['initial_time']
                end_time = alert['end_time']
                cluster = alert['cluster']
            uuid.append(alert['uuid'])
            lat += alert['latitude']
            long += alert['longitude']
            if alert['initial_time'] < initial_time:
                initial_time = alert['initial_time']
            if alert['end_time'] > end_time:
                end_time = alert['end_time']
        clusters.append({'cluster':cluster, 'uuids':uuid, 'initial_time':initial_time,
                           'end_time':end_time, 'total_time':end_time-initial_time,
                          'estimated_center' : {'latitude': lat/len(uuid), 'longitude': long/len(uuid)}})
        return clusters