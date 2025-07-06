import asyncio
import csv
import logging
import os
from pathlib import Path
from jobspy import scrape_jobs, ScraperInput, Site, Country, LinkedIn
from jobspy.is_seen import IsSeen
from jobspy.model import DescriptionFormat

SCRIPT_PATH = Path(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = SCRIPT_PATH.joinpath('output')


# https://www.linkedin.com/jobs/search/
# f_CM=5 Career growth and learning
# f_CM=3 Work-life balance


def scrape_test_jobs():
    jobs = scrape_jobs(
        site_name=["linkedin"],
        search_term="software engineer",
        location="Israel",
        results_wanted=20,
        hours_old=72,
        linkedin_fetch_description=True,
        verbose=2

        # linkedin_fetch_description=True # gets more info such as description, direct job url (slower)
        # proxies=["208.195.175.46:65095", "208.195.175.45:65095", "localhost"],
    )
    print(f"Found {len(jobs)} jobs")
    print(jobs.head())
    jobs.to_csv(DATA_DIR.joinpath("test-jobs.csv"), quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)  # to_excel


async def fetch_page():
    scraper_input = ScraperInput(
        site_type=[Site.LINKEDIN],
        #country=Country.ISRAEL,
        search_term="engineer",
        #google_search_term=google_search_term,
        location="Israel",
        #distance=distance,
        #is_remote=is_remote,
        #job_type=job_type,
        #easy_apply=easy_apply,
        description_format=DescriptionFormat.MARKDOWN,
        linkedin_fetch_description=False,
        results_wanted=20,
        #linkedin_company_ids=linkedin_company_ids,
        #offset=offset,
        #hours_old=hours_old,
    )

    can_skip = IsSeen()
    scraper = LinkedIn(is_async=True)
    ad_count = 1
    ad_dict = dict()
    start = 0
    for i in range(10):
        #results = scraper.get_job_ads_page_sync(scraper_input, start, can_skip)
        results = await scraper.get_job_ads_page(scraper_input, start, can_skip)
        received_ads = []
        for job in results:
            ad_number = ad_dict.get(job.id, None)
            if ad_number is None:
                ad_number = ad_count
                ad_dict[job.id] = ad_number
                ad_count += 1
            received_ads.append(ad_number)
        print(f"start: {start}, received_ads: {received_ads}")
        start += 1
        await asyncio.sleep(2)
    print("wtf")

async def fetch_company():
    # working with proxies
    #proxies = ["103.116.7.132:80"]
    #proxies = ["170.114.46.245:80"]
    proxies = None

    scraper = LinkedIn(is_async=True, proxies=proxies)
    name = 'microsoft'
    url = 'https://www.linkedin.com/company/microsoft'
    name = 'Ultron'
    url = 'https://www.linkedin.com/company/utron-solutions'
    #result = scraper.get_company_info_sync(name, url)
    result = await scraper.get_company_info(name, url)
    print("wtf")


if __name__ == "__main__":
    #logging.basicConfig(level=logging.DEBUG)
    httpx_logger = logging.getLogger("httpx")
    #httpx_logger.setLevel(logging.DEBUG)
    #scrape_test_jobs()
    #fetch_page()
    #fetch_company()
    asyncio.run(fetch_page())
