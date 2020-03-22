import argparse
import json
import logging
import os
import fastparquet as fp
import psycopg2
import s3fs
from pandas import DataFrame

import Immobilienscout24

logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)
logging.basicConfig(
    filename='app.log',
    format='%(asctime)s %(levelname)s %(message)s', datefmt='%H:%M:%S',
    filemode='w')


# Is to flatted the nested array we have
def flatten_arr(arr):
    return [item for sublist in arr for item in sublist]


# Is to flatten the dict we have
def flatten_dict(dictionary, separator='_', prefix=''):
    return {prefix + separator + k if prefix else k: v
            for kk, vv in dictionary.items()
            for k, v in flatten_dict(vv, separator, kk).items()
            } if isinstance(dictionary, dict) else {prefix: dictionary}


# get the all flat data from the API
def get_json_data(summary):
    # to get the total_page we need to iterate
    for k, v in summary.items():
        if k == "total_pages":
            total_pages = v
    flat_id = []
    # iterate over the total page and get List from all the pages
    for page in range(1, total_pages):
        getList = Crsl.getList(page)
        flat_id.append(getList)

    all_flat_id = []
    # get all the flat_id
    for fid in flat_id:
        for k, v in fid.items():
            if k == 'ids':
                all_flat_id.append(v)
    flatten_flat_ids = flatten_arr(all_flat_id)

    flat_data = []
    # get all the data from the flatten flat_id
    for flatten_flat_id in flatten_flat_ids:
        flat_data.append(Crsl.getData(flatten_flat_id))
    return flat_data


def process_json(flat_data):
    # get all the relavent column out from the output
    fact_flats_list = ['realEstate_@id',
                       'realEstate_livingSpace',
                       'realEstate_numberOfRooms',
                       'realEstate_floor',
                       'realEstate_apartmentType',
                       'realEstate_builtInKitchen',
                       'realEstate_lift',
                       'realEstate_balcony',
                       'realEstate_garden',
                       'realEstate_guestToilet',
                       'realEstate_handicappedAccessible',
                       'realEstate_heatingType',
                       'realEstate_thermalCharacteristic',
                       'realEstate_totalRent',
                       'realEstate_calculatedTotalRent',
                       'realEstate_baseRent',
                       'realEstate_serviceCharge',
                       'realEstate_deposit']

    dim_address_list = ['realEstate_address_city',
                        'realEstate_address_quarter',
                        'realEstate_address_postcode',
                        'realEstate_address_street',
                        'realEstate_address_houseNumber',
                        'realEstate_address_wgs84Coordinate_longitude',
                        'realEstate_address_wgs84Coordinate_latitude'
                        ]

    dim_agency_list = ['contactDetails_company',
                       'contactDetails_firstname',
                       'contactDetails_lastname',
                       'contactDetails_salutation',
                       'contactDetails_email',
                       'contactDetails_email',
                       'contactDetails_phoneNumberCountryCode',
                       'contactDetails_phoneNumberAreaCode',
                       'contactDetails_phoneNumberSubscriber',
                       'contactDetails_phoneNumber',
                       'contactDetails_phoneNumber',
                       'contactDetails_address_city',
                       'contactDetails_address_street',
                       'contactDetails_address_postcode',
                       'contactDetails_address_houseNumber']
    fact_flats = []
    dim_address = []
    dim_agency = []
    for i in range(len(flat_data)):
        for k, v in flat_data[i].items():
            if k == 'expose.expose':
                fact_flats.append(dict((k, flatten_dict(v)[k]) for k in fact_flats_list
                                       if k in flatten_dict(v)))
                dim_address.append(dict((k, flatten_dict(v)[k]) for k in dim_address_list
                                        if k in flatten_dict(v)))
                dim_agency.append(dict((k, flatten_dict(v)[k]) for k in dim_agency_list
                                       if k in flatten_dict(v)))

    return fact_flats, dim_address, dim_agency


def process_df(fact_flats, dim_address, dim_agency):
    df_fact_flats = DataFrame(fact_flats)
    df_fact_flats.rename(columns={'realEstate_@id': 'immoscout_id', 'realEstate_livingSpace': 'area_sq_m',
                                  'realEstate_numberOfRooms': 'cnt_rooms',
                                  'realEstate_floor': 'cnt_floors', 'floor': 'floor',
                                  'realEstate_apartmentType': 'type',
                                  'realEstate_builtInKitchen': 'has_fitted_kitchen', 'realEstate_lift': 'has_lift',
                                  'realEstate_balcony': 'has_balcony',
                                  'realEstate_garden': 'has_garden', 'realEstate_guestToilet': 'has_guest_toilet',
                                  'realEstate_handicappedAccessible': 'is_barrier_free',
                                  'realEstate_heatingType': 'heating_type',
                                  'realEstate_thermalCharacteristic': 'thermal_characteristic',
                                  'realEstate_totalRent': 'total_rent',
                                  'realEstate_calculatedTotalRent': 'calculatedTotalRent',
                                  'realEstate_baseRent': 'base_rent',
                                  'realEstate_serviceCharge': 'service_charge', 'realEstate_deposit': 'deposit'}, inplace=True)

    df_dim_address = DataFrame(dim_address)
    df_dim_address.rename(columns={'realEstate_address_city': 'city', 'realEstate_address_quarter': 'district',
                                   'realEstate_address_postcode': 'zip_code', 'realEstate_address_street': 'street',
                                   'realEstate_address_houseNumber': 'house_number',
                                   'realEstate_address_wgs84Coordinate_longitude': 'lng',
                                   'realEstate_address_wgs84Coordinate_latitude': 'lat'}, inplace=True)

    df_dim_agency = DataFrame(dim_agency)
    # df_dim_agency['id'] = df_dim_agency.index + 1
    df_dim_agency.rename(columns={'contactDetails_company': 'company_name',
                                  'contactDetails_firstname': 'contactDetails_firstname',
                                  'contact_lastname': 'contactDetails_lastname',
                                  'contactDetails_salutation': 'salutation', 'contactDetails_email': 'email',
                                  'contactDetails_phoneNumberCountryCode': 'phoneNumberCountryCode',
                                  'contactDetails_phoneNumberAreaCode': 'phoneNumberAreaCode',
                                  'contactDetails_phoneNumberSubscriber': 'phoneNumberSubscriber',
                                  'contactDetails_phoneNumber': 'mobile_number',
                                  'contactDetails_address_city': 'address_city',
                                  'contactDetails_address_street': 'address_street',
                                  'contactDetails_address_postcode': 'address_zipcode',
                                  'contactDetails_address_houseNumber': 'address_house_number'}, inplace=True)
    return df_fact_flats, df_dim_address, df_dim_agency


def write_to_s3(s3_path, df, partition_cols=None, file_scheme='hive'):
    print(df.dtypes)
    if partition_cols:
        fp.write(s3_path, df, file_scheme=file_scheme, partition_on=partition_cols, open_with=s3fs.S3FileSystem().open)
    else:
        fp.write(s3_path, df, file_scheme=file_scheme, open_with=s3fs.S3FileSystem().open)


def insert_into_tables(config, insert_stmt, df_values):
    """ create tables in the PostgreSQL database"""
    conn = None
    try:
        # read the connection parameters
        config = json.loads(config)
        user = config.get("user")
        password = config.get("password")
        port = config.get("port")
        host = config.get("host")
        database = config.get("database")
        logger.debug("user = %s, database = %s", user, database)
        # connect to the PostgreSQL server
        conn = psycopg2.connect(user=user,
                                password=password,
                                host=host,
                                port=port,
                                database=database)
        cur = conn.cursor()
        logger.debug("Connection established for postgres")

        # create table one by one
        logger.debug("Executing commands in postgres")
        psycopg2.extras.execute_batch(cur, insert_stmt, df_values)
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
    finally:
        if conn is not None:
            conn.close()


def insert_procees(df, table):
    if len(df) > 0:
        df_columns = list(df)
        # create (col1,col2,...)
        columns = ",".join(df_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

        # create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
    return insert_stmt, df.values


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    args = parser.parse_args()
    config = json.loads(args.config)
    logger.debug("Done with all the config collection")

    key = config.get("key")
    Crsl = Immobilienscout24.Immobilienscout(key)
    summary = Crsl.getSummary()
    flat_data = get_json_data(summary)
    fact_flats, dim_address, dim_agency = process_json(flat_data)
    df_fact_flats, df_dim_address, df_dim_agency = process_df(fact_flats, dim_address, dim_agency)

    logger.info("Writing into s3")
    s3_output_location = config.get("s3_output_location")
    logger.debug("s3 output location : %s", s3_output_location)

    s3_path_flats = os.path.join(s3_output_location, 'flats/')
    s3_path_address = os.path.join(s3_output_location, 'address/')
    s3_path_agency = os.path.join(s3_output_location, 'agency/')
    logger.debug("Writing to s3_path_flats = %s", s3_path_flats)
    logger.debug("Writing to s3_path_address = %s", s3_path_address)
    logger.debug("Writing to s3_path_agency = %s", s3_path_agency)

    write_to_s3(s3_path_flats, df_fact_flats)
    write_to_s3(s3_path_address, df_dim_address)
    write_to_s3(s3_path_agency, df_dim_agency)
    logger.info("Done: Writing into s3")

    flat_insert_stmt, flat_df_values = insert_procees(df_fact_flats, "flats")
    address_insert_stmt, address_df_values = insert_procees(df_dim_address, "address")
    agency_insert_stmt, agency_df_values = insert_procees(df_dim_agency, "agency")

    logger.info("Postgres: Inserting data into tables")
    insert_into_tables(config, flat_insert_stmt, flat_df_values)
    insert_into_tables(config, address_insert_stmt, address_df_values)
    insert_into_tables(config, agency_insert_stmt, agency_df_values)
    logger.info("Done :Inserting data into tables")
