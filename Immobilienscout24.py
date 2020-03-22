import requests
import logging

class Immobilienscout(object):
    __HOST = "https://immoscout-api-ji3l2ohvha-lz.a.run.app"

    def __init__(self, api_keys):
        self.keys = api_keys
        self.logger = logging.getLogger(__name__)

    def _get_response(self, url):
        host = self.__HOST
        key = self.keys
        url = host + "/" + url
        headers = {'X-API-KEY': key,
                   'accept': 'application/json'}
        resp = requests.get(url, headers=headers)
        return resp

    def getSummary(self):
        self.logger.info("Started with API: getSummary")
        response = self._get_response("get_summary")
        if response.status_code not in (200, 202):
            print(response.status_code)
            print(response.text)
            raise Exception("getSummary couldn't proceed")
        self.logger.info("Done with API: getSummary")
        return response.json()

    def getList(self, page_number):
        self.logger.info("Started with API: getList")
        response = self._get_response("get_list?page={}".format(page_number))
        if response.status_code not in (200, 202):
            print(response.status_code)
            print(response.text)
            raise Exception("getList couldn't proceed")
        self.logger.info("Done with API: getList")
        return response.json()

    def getData(self, flat_id):
        self.logger.info("Started with API: getData")
        response = self._get_response("/get_data?id={}".format(flat_id))
        if response.status_code not in (200, 202):
            print(response.status_code)
            print(response.text)
            raise Exception("getData couldn't proceed")
        self.logger.info("Done with API: getData")
        return response.json()
