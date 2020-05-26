import argparse
import csv
import socket

import scrapy
from scrapy.crawler import CrawlerProcess


def get_attr(response, css):
    attr = ''
    for css_selector in css:
        if len(response.css(css_selector)) > 0:
            attr = response.css(css_selector).get()
    return attr


def merge_csv(source, destination):
    with open(source, newline='') as csvfile:
        csvreader  = csv.DictReader(csvfile, delimiter=",")
        sortedlist = sorted(csvreader, key=lambda row: (row['awsOrigin'], row['domain']), reverse=False)

    with open(destination, 'w') as f:
        fieldnames = csvreader.fieldnames
        print(fieldnames)
        # writer = csv.DictWriter(f, fieldnames=fieldnames)
        # writer.writeheader()
        # for row in sortedlist:
        #     writer.writerow(row)


class NewsSpider(scrapy.Spider):
    name = 'fakeNewsSpider'
    handle_httpstatus_list = [200, 301, 403, 404, 503]

    def start_requests(self):
        with open(self.__getattribute__('source_file')) as f:
            urls = csv.DictReader(f, delimiter=',')
            for url in urls:
                yield scrapy.Request('https://' + url['domain'], self.parse)

    def parse(self, response):
        origin_ip = ''
        try:
            origin_ip = socket.gethostbyname(response.url[8:])
        except socket.gaierror:
            pass

        site_info = {
            'awsOrigin': origin_ip,
            'domain': response.url[8:],
            'state': '',
            'lat': '',
            'lng': '',
            'locationVerified': '0',
            'httpResponseCode': response.status,
            'redirectsTo': '',
            'contentLength': '',
            'facebookUrl': '',
            'siteName': '',
            'twitterUrl': '',
            'itunesAppStoreUrl': '',
            'twitterAccountCreatedAt': '',
            'twitterUserId': '',
            'twitterFollowers': '',
            'twitterFollowing': '',
            'twitterTweets': '',
            'siteOperator': '',
        }

        if response.status == 200:
            site_info['siteName'] = get_attr(response, [
                '.logo > a:nth-child(1) > img:nth-child(1) ::attr("alt")',
                'h1.title > a:nth-child(1)::text'
            ])

            site_info['facebookUrl'] = get_attr(response, [
                '.social-nav > a:nth-child(1) ::attr("href")',
                '.footer__social > a:nth-child(1)::attr("href")'
            ])

            site_info['contentLength'] = len(response.body)

            for next_page in response.css('div.col-md-3:nth-child(1) > div:nth-child(1) > ul:nth-child(2) a'):
                yield response.follow(next_page.css('a::attr("href")').get(), self.parse)

            for next_page in response.css('div.col-sm-3:nth-child(1) > div:nth-child(1) > ul:nth-child(2) a'):
                yield response.follow(next_page.css('a::attr("href")').get(), self.parse)

            for next_page in response.css('div.col-sm-4 > ul > li a::attr("href")'):
                if next_page.get().endswith('.com') and next_page.get().startswith('https://'):
                    yield response.follow(next_page.get(), self.parse)

        elif response.status == 301:
            site_info['redirectsTo'] = response.headers.get('Location').decode('UTF-8')
            yield response.follow(site_info['redirectsTo'], self.parse)

        yield site_info


def main():
    # parse arguments and setup program
    parser = argparse.ArgumentParser(description='Crawl fake news sites for more information and new sites')
    parser.add_argument('-s', '--source-file', type=str, default='../../sites.csv', help='File with start urls.')
    parser.add_argument('-o', '--output-file', type=str, default='new_sites.csv', help='File to save crawl results to.')
    parser.add_argument('-f', '--format', type=str, default='csv', help='Format in which to save the results.')
    parser.add_argument('-d', '--dont-scrape', action='store_true', help='Don\'t scrape.')
    parser.add_argument('-m', '--merge', action='store_true', help='Merge the output(-file) into the source(-file).')
    args = parser.parse_args()

    if not args.dont_scrape:
        # start crawling process
        process = CrawlerProcess(settings={
            'FEED_FORMAT': args.format,
            'FEED_URI': args.output_file
        })

        process.crawl(NewsSpider, source_file=args.source_file)
        process.start()

    if args.merge:
        merge_csv(args.output_file, args.source_file)


if __name__ == '__main__':
    main()
