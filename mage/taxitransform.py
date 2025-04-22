import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Optimized transformer: Uses sample of data (first 500 rows) and selected columns
    for fast processing during development or testing.
    """
    # Select only required columns for testing and limit to 500 rows
    df = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
             'trip_distance', 'RatecodeID', 'PULocationID', 'DOLocationID',
             'payment_type', 'fare_amount']].head(500)

    # Convert to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')

    # Datetime dimension
    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim['datetime_id'] = datetime_dim.index

    # Passenger count dimension
    passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index

    # Trip distance dimension
    trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index

    # Rate code dimension
    rate_code_type = {
        1: "Standard rate", 2: "JFK", 3: "Newark", 4: "Nassau or Westchester",
        5: "Negotiated fare", 6: "Group ride"
    }
    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)

    # Pickup location dimension
    pickup_location_dim = df[['PULocationID']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index

    # Dropoff location dimension
    dropoff_location_dim = df[['DOLocationID']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index

    # Payment type dimension
    payment_type_name = {
        1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute",
        5: "Unknown", 6: "Voided trip"
    }
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)

    # Merge to create fact table
    fact_table = df.merge(passenger_count_dim, on='passenger_count') \
                   .merge(trip_distance_dim, on='trip_distance') \
                   .merge(rate_code_dim, on='RatecodeID') \
                   .merge(pickup_location_dim, on='PULocationID') \
                   .merge(dropoff_location_dim, on='DOLocationID') \
                   .merge(datetime_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime']) \
                   .merge(payment_type_dim, on='payment_type') \
                   [['datetime_id', 'passenger_count_id', 'trip_distance_id',
                     'rate_code_id', 'pickup_location_id', 'dropoff_location_id',
                     'payment_type_id', 'fare_amount']]

    return {
        "datetime_dim": datetime_dim.to_dict(orient="dict"),
        "passenger_count_dim": passenger_count_dim.to_dict(orient="dict"),
        "trip_distance_dim": trip_distance_dim.to_dict(orient="dict"),
        "rate_code_dim": rate_code_dim.to_dict(orient="dict"),
        "pickup_location_dim": pickup_location_dim.to_dict(orient="dict"),
        "dropoff_location_dim": dropoff_location_dim.to_dict(orient="dict"),
        "payment_type_dim": payment_type_dim.to_dict(orient="dict"),
        "fact_table": fact_table.to_dict(orient="dict")
    }


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
