import argparse
import Immobilienscout24
import pandas as pd


def flatten_arr(arr):
    return [item for sublist in arr for item in sublist]


def flatten_dict(dictionary, separator='_', prefix=''):
    return {prefix + separator + k if prefix else k: v
            for kk, vv in dictionary.items()
            for k, v in flatten_dict(vv, separator, kk).items()
            } if isinstance(dictionary, dict) else {prefix: dictionary}


def get_json_data(summary):
    for k, v in summary.items():
        if k == "total_pages":
            total_pages = v
    flat_id = []
    for page in range(1, 2):
        #         print(page)
        getList = Crsl.getList(page)
        flat_id.append(getList)
    #     print(flat_id)
    all_flat_id = []
    for id in flat_id:
        for k, v in id.items():
            if k == 'ids':
                all_flat_id.append(v)
    flatten_all_flat_id = flatten_arr(all_flat_id)

    flat_data = []
    for id in flatten_all_flat_id:
        flat_data.append(Crsl.getData(id))
    return flatten_all_flat_id, flat_data


def process_json(flat_data):
    accounts_list = ['realEstate_@id',
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

    accounts_list2 = ['realEstate_address_city',
                      'realEstate_address_quarter',
                      'realEstate_address_postcode',
                      'realEstate_address_street',
                      'realEstate_address_houseNumber',
                      'realEstate_address_wgs84Coordinate_longitude',
                      'realEstate_address_wgs84Coordinate_latitude'
                      ]

    accounts_list3 = ['contactDetails_company',
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
    final = []
    final2 = []
    final3 = []
    for i in range(1):
        for k, v in flat_data[0].items():
            if k == 'expose.expose':
                print("----------------------------------------------------\n")
                final.append(dict((k, flatten_dict(v)[k]) for k in accounts_list
                                  if k in flatten_dict(v)))
                final2.append(dict((k, flatten_dict(v)[k]) for k in accounts_list2
                                   if k in flatten_dict(v)))
                final3.append(dict((k, flatten_dict(v)[k]) for k in accounts_list3
                                   if k in flatten_dict(v)))
    return final, final2, final3


def process_df(final, final2, final3):
    df_final = pd.DataFrame(final)
    df_final['id'] = df_final.index + 1
    df_final2 = pd.DataFrame(final2)
    df_final2['id'] = df_final2.index + 1

    df_final3 = pd.DataFrame(final3)
    df_final3['id'] = df_final3.index + 1
    df_final3.rename(columns={'id': 'id', 'contactDetails_company': 'company_name',
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
                              'contactDetails_address_houseNumber': 'address_house_number'
                              }, inplace=True)
    return df_final, df_final2, df_final3


# 'dffbab93-44e9-41c2-bfff-6bab66c89b6c'
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    key = parser.add_argument('--key', type=str, required=True)
    key = 'dffbab93-44e9-41c2-bfff-6bab66c89b6c'
    Crsl = Immobilienscout24.Immobilienscout(key)
    summary = Crsl.getSummary()
    flatten_all_flat_id, flat_data = get_json_data(summary)
    final, final2, final3 = process_json(flat_data)
    process_df(final, final2, final3)
