import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# example usage when called from your development python file: link_request = request_url.UrlRequest.make_request(link, header)

class UrlRequest:
    # update with your own header
    header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
    @staticmethod
    def make_request(url, header):
        s = requests.Session()

        return UrlRequest.__requests_retry_session(session=s).get(url, timeout=10, headers=header)

    def __requests_retry_session(
            # adjust the number of retries and backoff_factor to stagger your requests
            retries=5,
            backoff_factor=0.5,
            status_forcelist=(500, 502, 504),
            session=None,


    ):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            connect=retries,
            read=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,

        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session