import logging
from requests.auth import HTTPBasicAuth
import requests

class GithubApi(object):
    __HOST = "http://api.github.com/repos"

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.logger = logging.getLogger(__name__)

    def _get_auth(self):
        return HTTPBasicAuth(self.username, self.password)

    def _get_github_response(self, url, headers=None):
        host = self.__HOST
        url = host + "/" + url
        auth = self._get_auth()
        if headers:
            resp = requests.get(url, auth=auth, headers=headers)
        else:
            resp = requests.get(url, auth=auth)
        return resp

    def repoDetails(self,url):
        response = self._get_github_response(url)
        return response.json()

    def starGazerDetails(self,url):
        headers = {"Accept": "application/vnd.github.v3.star+json"}
        response = self._get_github_response(url, headers = headers)
        return response.json()