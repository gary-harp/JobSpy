from __future__ import annotations

import math
import json
import random
import time
from datetime import datetime
from typing import Optional, List
from urllib.parse import urlparse, urlunparse, unquote

import regex as re
from bs4 import BeautifulSoup
from bs4.element import Tag
from requests import Response

from jobspy.can_skip_job_post import CanSkipJobPost
from jobspy.exception import LinkedInException
from jobspy.is_seen import IsSeen
from jobspy.linkedin.company import Company
from jobspy.linkedin.constant import headers
from jobspy.linkedin.util import (
    is_job_remote,
    job_type_code,
    parse_job_type,
    parse_job_level,
    parse_company_industry
)
from jobspy.model import (
    JobPost,
    Location,
    JobResponse,
    Country,
    Compensation,
    DescriptionFormat,
    Scraper,
    ScraperInput,
    Site,
)
from jobspy.util import (
    extract_emails_from_text,
    currency_parser,
    markdown_converter,
    create_session,
    remove_attributes,
    create_logger,
)

log = create_logger("LinkedIn")
MAX_RECORDS = 50000




class LinkedIn(Scraper):
    base_url = "https://www.linkedin.com"
    delay = 3
    band_delay = 4
    jobs_per_page = 25

    def __init__(
        self,
            proxies: list[str] | str | None = None,
            ca_cert: str | None = None,
            is_async: bool = False
    ):
        """
        Initializes LinkedInScraper with the LinkedIn job search url
        """
        super().__init__(Site.LINKEDIN, proxies=proxies, ca_cert=ca_cert, is_async=is_async)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=ca_cert,
            is_tls=False,
            has_retry=True,
            delay=5,
            clear_cookies=True,
            is_async=is_async
        )
        self.session.headers.update(headers)
        self.scraper_input = None
        self.country = "worldwide"
        self.job_url_direct_regex = re.compile(r'(?<=\?url=)[^"]+')

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes LinkedIn for jobs with scraper_input criteria
        :param scraper_input:
        :return: job_response
        """
        self.scraper_input = scraper_input
        job_list: list[JobPost] = []
        can_skip = IsSeen()
        start = scraper_input.offset // 10 * 10 if scraper_input.offset else 0
        request_count = 0
        continue_search = (
            lambda: len(job_list) < scraper_input.results_wanted and start < MAX_RECORDS
        )
        while continue_search():
            request_count += 1
            log.info(
                f"search page: {request_count} / {math.ceil(scraper_input.results_wanted / 10)}"
            )

            max_page_fetch = scraper_input.results_wanted - len(job_list)
            max_page_fetch = min(max_page_fetch, MAX_RECORDS - start)
            page_jobs = self.get_job_ads_page_sync(scraper_input, start, can_skip, max_page_fetch)
            for job in page_jobs:
                can_skip.add_seen(job.id)
            job_list += page_jobs

            if continue_search():
                time.sleep(random.uniform(self.delay, self.delay + self.band_delay))
                #start += len(job_list)
                # TODO GARY not sure about this
                start += len(page_jobs)

        job_list = job_list[: scraper_input.results_wanted]
        return JobResponse(jobs=job_list)


    def get_job_ads_page_sync(self,
                              scraper_input: ScraperInput,
                              start: int,
                              can_skip: CanSkipJobPost,
                              max_page_fetch: Optional[int] = None) -> List[JobPost]:
        request_params = self._build_search_request(scraper_input, start)
        response = self._send_request_sync(request_params)
        if response is None:
            return []
        basic_job_infos = self._parse_search_response(response,
                                           scraper_input,
                                           can_skip, max_page_fetch)
        fetch_desc = scraper_input.linkedin_fetch_description
        if fetch_desc:
            for basic_info in basic_job_infos:
                job_details = self._get_job_details_sync(basic_info, scraper_input)
                basic_info.update(job_details)
        result = [JobPost(**basic_info) for basic_info in basic_job_infos]
        return result

    def _build_search_request(self,
                              scraper_input: ScraperInput,
                              start: int) -> dict:
        seconds_old = (
            scraper_input.hours_old * 3600 if scraper_input.hours_old else None
        )
        params = {
            "keywords": scraper_input.search_term,
            "location": scraper_input.location,
            "distance": scraper_input.distance,
            "f_WT": 2 if scraper_input.is_remote else None,
            "f_JT": (
                job_type_code(scraper_input.job_type)
                if scraper_input.job_type
                else None
            ),
            "pageNum": 0,
            "start": start,
            "f_AL": "true" if scraper_input.easy_apply else None,
            "f_C": (
                ",".join(map(str, scraper_input.linkedin_company_ids))
                if scraper_input.linkedin_company_ids
                else None
            ),
        }
        if seconds_old is not None:
            params["f_TPR"] = f"r{seconds_old}"

        params = {k: v for k, v in params.items() if v is not None}
        request_params = {
            'method' : 'GET',
            'url': f"{self.base_url}/jobs-guest/jobs/api/seeMoreJobPostings/search?",
            'params': params,
            'timeout' : 10
        }
        return request_params

    def _send_request_sync(self, request_params: dict) -> Optional[Response]:
        try:
            response = self.session.request(**request_params)
            if response.status_code not in range(200, 400):
                if response.status_code == 429:
                    err = (
                        f"429 Response - Blocked by LinkedIn for too many requests"
                    )
                else:
                    err = f"LinkedIn response status code {response.status_code}"
                    err += f" - {response.text}"
                log.error(err)
            return response
        except Exception as e:
            if "Proxy responded with" in str(e):
                log.error(f"LinkedIn: Bad proxy")
            else:
                log.error(f"LinkedIn: {str(e)}")
            return None

    async def _send_request_async(self, request_params: dict) -> Optional[Response]:
        try:
            response = await self.session.request_async(**request_params)
            if response.status_code not in range(200, 400):
                if response.status_code == 429:
                    err = (
                        f"429 Response - Blocked by LinkedIn for too many requests"
                    )
                else:
                    err = f"LinkedIn response status code {response.status_code}"
                    err += f" - {response.text}"
                log.error(err)
            return response
        except Exception as e:
            if "Proxy responded with" in str(e):
                log.error(f"LinkedIn: Bad proxy")
            else:
                log.error(f"LinkedIn: {str(e)}")
            return None

    def _parse_search_response(self,
                               response: Response,
                               scraper_input: ScraperInput,
                               can_skip: CanSkipJobPost,
                               max_page_fetch: Optional[int] = None) -> List[dict]:
        seen_ids = set()
        job_list = []
        soup = BeautifulSoup(response.text, "html.parser")
        job_cards = soup.find_all("div", class_="base-search-card")
        if len(job_cards) == 0:
            return job_list

        for job_card in job_cards:
            href_tag = job_card.find("a", class_="base-card__full-link")
            if href_tag and "href" in href_tag.attrs:
                href = href_tag.attrs["href"].split("?")[0]
                job_id = href.split("-")[-1]
                if can_skip.can_skip(job_id):
                    continue
                if job_id in seen_ids:
                    continue
                seen_ids.add(job_id)

                try:
                    basic_job_info = self._process_job(job_card, job_id)
                    if basic_job_info:
                        job_list.append(basic_job_info)
                    if max_page_fetch is not None and len(job_list) >= max_page_fetch:
                        break
                except Exception as e:
                    raise LinkedInException(str(e))
        return job_list

    def get_company_info_sync(self, company_name: str, company_url) -> Optional[Company]:
        request_params = self._build_company_info_request(company_name, company_url)
        response = self._send_request_sync(request_params)
        if response is None:
            return None
        return self._parse_company_response(company_name, response)

    async def get_company_info(self, company_name: str, company_url) -> Optional[Company]:
        request_params = self._build_company_info_request(company_name, company_url)
        response = await self._send_request_async(request_params)
        if response is None:
            return None
        return self._parse_company_response(company_name, response)


    def _build_company_info_request(self, company_name, company_url: str) -> dict:
        request_params = {
            'method' : 'GET',
            'url': f"{company_url}",
            'timeout' : 10
        }
        return request_params

    def _parse_company_response(self, company_name: str, response: Response) -> Optional[Company]:
        organization = None
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            script = soup.find('script', type='application/ld+json')
            data = json.loads(script.string)
            graph = data.get("@graph", None)
            if graph is None:
                raise Exception(f"graph key does not exist. data: {data}")
            for i in range(len(graph)):
                data_type = graph[i].get('@type', None)
                if data_type is None:
                    continue
                if data_type == 'Organization':
                    organization = Company()
                    organization.name = graph[i].get('name', '')
                    organization.url =  graph[i].get('url', '')
                    organization.description =  graph[i].get('description', '')
                    size = graph[i].get('numberOfEmployees', None)
                    if size:
                        size = size.get('value', None)
                    if size and isinstance(size, int):
                        organization.number_of_employees = size
                    else:
                        log.error(f"Unexpected number of employees format in company: {company_name}. Format: {graph[i]}")
        except Exception as ex:
            log.error(ex, f"Failed to parse company html for company: {company_name}")
        return organization

    def _process_job(
        self, job_card: Tag, job_id: str
    ) -> Optional[dict]:
        salary_tag = job_card.find("span", class_="job-search-card__salary-info")

        compensation = description = None
        if salary_tag:
            salary_text = salary_tag.get_text(separator=" ").strip()
            salary_values = [currency_parser(value) for value in salary_text.split("-")]
            salary_min = salary_values[0]
            salary_max = salary_values[1]
            currency = salary_text[0] if salary_text[0] != "$" else "USD"

            compensation = Compensation(
                min_amount=int(salary_min),
                max_amount=int(salary_max),
                currency=currency,
            )

        title_tag = job_card.find("span", class_="sr-only")
        title = title_tag.get_text(strip=True) if title_tag else "N/A"

        company_tag = job_card.find("h4", class_="base-search-card__subtitle")
        company_a_tag = company_tag.find("a") if company_tag else None
        company_url = (
            urlunparse(urlparse(company_a_tag.get("href"))._replace(query=""))
            if company_a_tag and company_a_tag.has_attr("href")
            else ""
        )
        company = company_a_tag.get_text(strip=True) if company_a_tag else "N/A"

        metadata_card = job_card.find("div", class_="base-search-card__metadata")
        location = self._get_location(metadata_card)

        datetime_tag = (
            metadata_card.find("time", class_="job-search-card__listdate")
            if metadata_card
            else None
        )
        date_posted = None
        if datetime_tag and "datetime" in datetime_tag.attrs:
            datetime_str = datetime_tag["datetime"]
            try:
                date_posted = datetime.strptime(datetime_str, "%Y-%m-%d")
            except:
                date_posted = None
        is_remote = is_job_remote(f'{title} {location.display_location()}'.lower())

        basic_job_info = {
            "id" :f"{job_id}",
            "title" : "title",
            "company_name" : company,
            "company_url" : company_url,
            "location" : location,
            "is_remote" : is_remote,
            "date_posted" : date_posted,
            "job_url" : f"{self.base_url}/jobs/view/{job_id}",
            "compensation" : compensation,
        }

        return basic_job_info

    def _get_job_details_sync(self, basic_job_info: dict, scraper_input: ScraperInput) -> dict:
        """
        Retrieves job description and other job details by going to the job page url
        :param job_page_url:
        :return: dict
        """
        job_id = basic_job_info['id']
        try:
            response = self.session.get(
                f"{self.base_url}/jobs/view/{job_id}", timeout=5
            )
            response.raise_for_status()
        except:
            return {}
        if "linkedin.com/signup" in response.url:
            return {}

        soup = BeautifulSoup(response.text, "html.parser")
        div_content = soup.find(
            "div", class_=lambda x: x and "show-more-less-html__markup" in x
        )
        description = None
        if div_content is not None:
            div_content = remove_attributes(div_content)
            description = div_content.prettify(formatter="html")
            if scraper_input.description_format == DescriptionFormat.MARKDOWN:
                description = markdown_converter(description)

        h3_tag = soup.find(
            "h3", text=lambda text: text and "Job function" in text.strip()
        )

        job_function = None
        if h3_tag:
            job_function_span = h3_tag.find_next(
                "span", class_="description__job-criteria-text"
            )
            if job_function_span:
                job_function = job_function_span.text.strip()

        company_logo = (
            logo_image.get("data-delayed-url")
            if (logo_image := soup.find("img", {"class": "artdeco-entity-image"}))
            else None
        )
        return {
            "description": description,
            "job_level": parse_job_level(soup),
            "company_industry": parse_company_industry(soup),
            "job_type": parse_job_type(soup),
            "job_url_direct": self._parse_job_url_direct(soup),
            "company_logo": company_logo,
            "job_function": job_function,
            "emails": extract_emails_from_text(description),
            "is_remote": basic_job_info["is_remote"] or is_job_remote(description.lower())
        }

    def _get_location(self, metadata_card: Optional[Tag]) -> Location:
        """
        Extracts the location data from the job metadata card.
        :param metadata_card
        :return: location
        """
        location = Location(country=Country.from_string(self.country))
        if metadata_card is not None:
            location_tag = metadata_card.find(
                "span", class_="job-search-card__location"
            )
            location_string = location_tag.text.strip() if location_tag else "N/A"
            parts = location_string.split(", ")
            if len(parts) == 2:
                city, state = parts
                location = Location(
                    city=city,
                    state=state,
                    country=Country.from_string(self.country),
                )
            elif len(parts) == 3:
                city, state, country = parts
                country = Country.from_string(country)
                location = Location(city=city, state=state, country=country)
        return location

    def _parse_job_url_direct(self, soup: BeautifulSoup) -> str | None:
        """
        Gets the job url direct from job page
        :param soup:
        :return: str
        """
        job_url_direct = None
        job_url_direct_content = soup.find("code", id="applyUrl")
        if job_url_direct_content:
            job_url_direct_match = self.job_url_direct_regex.search(
                job_url_direct_content.decode_contents().strip()
            )
            if job_url_direct_match:
                job_url_direct = unquote(job_url_direct_match.group())

        return job_url_direct
