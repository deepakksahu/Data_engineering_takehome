import argparse
import logging
import math

import pandas

import GithubApi

MAX_GITHUB_SUPPORTED_PAGE_IN_A_TRANSACTION = 100  # limitation from Github

logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)
logging.basicConfig(
    filename='GithubApi_app.log',
    format='%(asctime)s %(levelname)s %(message)s', datefmt='%H:%M:%S',
    filemode='w')


def flatten_dict(dd, separator='_', prefix=''):
    return {prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
            } if isinstance(dd, dict) else {prefix: dd}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', type=str, required=True)
    parser.add_argument('--password', type=str, required=True)

    args = parser.parse_args()
    logger.debug("Done with all the config collection")

    githubapi = GithubApi.GithubApi(args.username, args.password)

    star_count = githubapi.repoDetails('timqian/star-history')['stargazers_count']
    # print(star_count)

    pages = math.ceil(star_count / 100)
    # print(pages)

    stargazer_details = []
    for page in range(pages):
        x = githubapi.starGazerDetails(
            'timqian/star-history/stargazers?per_page={}&page={}'.format(MAX_GITHUB_SUPPORTED_PAGE_IN_A_TRANSACTION,
                                                                         page + 1))
        for i in x:
            stargazer_details.append(flatten_dict(i))
    print(len(stargazer_details))
    print(len(pandas.DataFrame(stargazer_details)))
    print(pandas.DataFrame(stargazer_details).head())

#https://developer.github.com/changes/2020-02-14-deprecating-password-auth/
main()
