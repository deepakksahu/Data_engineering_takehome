import requests
class Immobilienscout(object):
    __DEMO_HOST = "https://immoscout-api-ji3l2ohvha-lz.a.run.app"

    def __init__(self, api_keys, env="demo"):
        self.keys = api_keys
        self.env = env

    def _get_response(self, url):
        host = self.__DEMO_HOST
        key = self.keys
        url = host + "/" + url
        headers = {'X-API-KEY': key,
                   'accept': 'application/json'}
        resp = requests.get(url, headers=headers)
        return resp

    def getSummary(self):
        response = self._get_response("get_summary")
        print(response.status_code)
        print(response.text)
        if response.status_code not in (200, 202):
            print(response.status_code)
            print(response.text)
            raise Exception("getSummary couldn't proceed")

    def getList(self, page_number):
        response = self._get_response("get_list?page={}".format(page_number))
        print(response.status_code)
        print(response.text)
        if response.status_code not in (200, 202):
            print(response.status_code)
            print(response.text)
            raise Exception("getList couldn't proceed")

    def getData(self, flat_id):
        response = self._get_response("/get_data?id={}".format(flat_id))
        print(response.status_code)
        print(response.text)
        if response.status_code not in (200, 202):
            print(response.status_code)
            print(response.text)
            raise Exception("getData couldn't proceed")