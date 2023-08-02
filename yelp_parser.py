import random
import requests.exceptions
from random_user_agent.user_agent import UserAgent
import requests as req
import json
from threading import Thread
from queue import Queue
import logging

class ClosedAccess(Exception):
    def __init__(self, message="Server returned 503"):
        self.message = message
        super().__init__(self.message)

logger = logging.getLogger("parser")
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

class business:
    def __init__(self, name = None, rating = None, rev_num = None, yelp_url = None, web_url = None, revs = None, **kwargs):
        self.name = name
        self.rating = rating
        self.rev_num = rev_num
        self.yelp_url = yelp_url
        self.web_url = web_url
        self.revs = revs

    def dict(self):
        return {
            'name': self.name,
            'rating': self.rating,
            'rev_num': self.rev_num,
            'yelp_url': self.yelp_url,
            'web_url': self.web_url,
            'revs': self.revs,
        }

    def __repr__(self):
        return f"<Business {self.name}>"
    def __str__(self):
        return f"{self.name} {self.rating}⭐ {len(self.revs)}"

def parse_resp(resp):
    businesses = resp['searchPageProps']['mainContentComponentsListProps']
    clean_businesses = []

    for bus in businesses:
        if 'bizId' in bus and not bus['searchResultBusiness'].get("isAd", True): clean_businesses.append(bus)

    return clean_businesses

def parse_buz(buz, ignore_rev = False):

    if not ignore_rev:
        rev_req = req.get(f'https://www.yelp.com/biz/{buz["bizId"]}/props')

        if rev_req.status_code != 200:

            if rev_req.status_code == 503: raise ClosedAccess()

            logger.error(f"{rev_req.status_code} {rev_req.text}")

            return parse_buz(buz)

        reviews = rev_req.json()['bizDetailsPageProps']['reviewFeedQueryProps']['reviews']

        clean_reviews = []

        for rev in reviews:
            clean_reviews.append(
                {
                    "author": rev['user']['markupDisplayName'],
                    "location": rev['user']['displayLocation'],
                    "date": rev['localizedDate'],
                }
            )
    else:
        clean_reviews = None

    try:
        web_site = buz['searchResultBusiness']['website']['href']
    except TypeError:
        web_site = None
    except ValueError:
        web_site = None


    return business(
        buz['searchResultBusiness']['name'],
        buz['searchResultBusiness']['rating'],
        buz['searchResultBusiness']['reviewCount'],
        buz['searchResultBusiness']['businessUrl'],
        web_site,
        clean_reviews[:min(5, len(clean_reviews))],
    )

def get_categories(resp):
    return resp['searchPageProps']['filterPanelProps']['filterSetMap']['category']['moreFilters'][0]['subfilters']

def get_attributes(resp):

    attrs = []

    filters = resp['searchPageProps']['filterPanelProps']['filterSetMap']

    # if 'price' in filters:
    #     attrs.extend(filters['price']['filters'])
    #
    if 'feature' in filters:
        for sub_fil in filters['feature']['moreFilters']:
            attrs.extend(sub_fil['subfilters'])
    #
    # if 'promoted_intent' in filters:
    #     attrs.extend(filters['promoted_intent']['filters'])

    return attrs

def get_locations(resp):

    def add_p(a): return 'p:' + a

    return list(map(add_p, resp['searchPageProps']['filterPanelProps']['filterSetMap']['place']['moreFilters'][0]['subfilters']))


def get_filters(resp):
    all_filters = resp['searchPageProps']['filterPanelProps']['filterInfoMap']

    features = []
    categories = []

    filter_map = {
        'feature': features,
        'category': categories,
    }

    for f in all_filters:
        filter_map.get(all_filters[f]['name'], []).append(all_filters[f]['value'])

    return categories, features


def collect_threaded(categories, results):
    collect_all_with_set_params(categories, results)

def collect_threaded_queued(results, queue: Queue):

    try:
        while True:
            data = queue.get()

            res = collect_all_with_set_params(*data, results)

            for a in res:
                results[a] = res[a]

            queue.task_done()
    except ClosedAccess as e:
        queue.task_done()
        logger.error(str(e))
        while True:
            data = queue.get()
            queue.task_done()

def collect_all_with_set_params(categories, results):
    try: return _collect_all_with_set_params(categories, results)

    except req.exceptions.SSLError:
        return collect_all_with_set_params(categories, results)
    except req.exceptions.ProxyError:
        return collect_all_with_set_params(categories, results)
    except req.exceptions.RequestException as e:
        logger.error(e)
        return collect_all_with_set_params(categories, results)

def _collect_all_with_set_params(categories, results):

    logger.info(f"COLLECTING - {categories}")

    start = 0

    while True:

        try:
            response = req.get(f'https://www.yelp.com/search/snippet', params= {
                'find_desc': find_desc,
                'find_loc': find_loc,
                'cflt':categories,
                'start': start,
            },
            headers={
                "User-Agent": UserAgent().get_random_user_agent(),
            })
        except req.exceptions.RequestException as e:
            logger.error(e)
            continue

        if response.status_code != 200:

            if response.status_code == 503:
                raise ClosedAccess()

            logger.error(f"Server error: {response.status_code}")

            continue

        if 'searchExceptionProps' in response.json()['searchPageProps']:

            logger.error(response.json()['searchPageProps']['searchExceptionProps'])

            if response.json()['searchPageProps']['searchExceptionProps'].get('exceptionType', '') == 'excessivePaging':
                break

        try:
            buzs = parse_resp(response.json())

            if len(buzs) == 0:
                break

            for buz in buzs:

                b = parse_buz(buz)

                results[b.name] = b

        except KeyError as e:
            logger.error(response.text)
            logger.error(str(e))
            continue

        start += 10

    logger.info(f"DONE {categories}")

find_loc = input("Location: ")
find_desc = input("Category name: ")

threads_count = int(input("Thread num (rec. 3): "))

def main():
    logger.info("Getting starting request")

    logger.info(f"Making request with")

    try:
        first_request = req.get(f'https://www.yelp.com/search/snippet', params= {
            'find_desc': find_desc,
            'find_loc': find_loc
        },
        headers={
            "User-Agent": UserAgent().get_random_user_agent(),
        }
        )
    except req.exceptions.RequestException as e:
        logger.error(f"FAILED {str(e)}")
        return main()

    if first_request.status_code != 200:
        logger.error(f"FAILED {first_request.status_code} {first_request.text}")
        return main()

    try:
        first_request_json = first_request.json()
    except requests.exceptions.JSONDecodeError as e:
        logger.error(first_request.text)
        raise e

    logger.info("Parsing filters")

    categories, attributes = get_filters(first_request_json)

    random.shuffle(categories)
    random.shuffle(attributes)

    parsed_businesses = {}

    threads = []

    q = Queue()

    logger.info("Creating threads")

    for cat in categories:
        q.put([[cat]])

    for _ in range(threads_count): threads.append(Thread(target=collect_threaded_queued, args=[parsed_businesses, q]))

    logger.info("Starting threads")

    for t in threads: t.start()

    logger.info("Joining queue")

    q.join()

    results = []

    for b in parsed_businesses:
        logger.info(str(parsed_businesses[b]))
        results.append(parsed_businesses[b].dict())

    logger.info(len(parsed_businesses))

    with open(f"{find_loc} {find_desc}_results.json", 'w', encoding="utf-8") as f:
        f.write(json.dumps(results, indent=3, ensure_ascii=False))

if __name__ == "__main__": main()