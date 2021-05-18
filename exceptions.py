from urllib.parse import urlparse

class NoRobotsConfiguredException(Exception):
    def __init__(self, func, url, robot_urls):
        msg = f"Cannot run '{func.__name__}' for the given url: {url}. Robots must first be configured for the given url.\nThe following urls are configured: {robot_urls}"
        super().__init__(msg)


class RobotsBlockedException(Exception):
    def __init__(self, url, user_agent):
        o = urlparse(url)
        scheme = 'https' if o.scheme == '' else o.scheme
        robots_url = f'{scheme}://{o.netloc}' + '/robots.txt'

        msg = f'Cannot complete request to the following URL: {url}\nThis website may have blocked the use of web scrapers.\nTo check, you may want to inspect the robots.txt file, which will likely be found at the following URL: {robots_url}\nThe User-Agent for the request was: {user_agent}'
        super().__init__(msg)

class MissingLocationException(Exception):
    def __init__(self, func):
        msg = f"You must include a location when calling '{func.__name__}'. Location can either be the two character postal code abbreviation for a given province/territory; or 'Fed - MP', 'Fed - Sen', or 'Fed - Other' for federal legislators."
        super().__init__(msg)