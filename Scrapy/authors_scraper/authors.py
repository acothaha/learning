import scrapy

# creating a new class to act as the spider
# the class must inherit from scrapy.Spider
class QuotesSpider(scrapy.Spider):
    # name of the spider (must be unique)
    name = 'authors'

    # list of urls that want to be scrapped
    start_urls = ['http://quotes.toscrape.com',]

    # parse method is the main driver of a spider
    def parse(self, response):

        urls = response.css('div.quote > span > a::attr(href)').extract()
        for url in urls:
            url = response.urljoin(url)
            yield scrapy.Request(url=url, callback=self.parse_details)

        next_page = response.css('li.next > a::attr("href")').extract_first()
        if next_page:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(url=next_page, callback=self.parse)

    def parse_details(self, response):
        yield {
            'name': response.css('h3.author-title::text').extract_first().strip(),
            'birth_data': response.css('span.author-born-date::text').extract_first()
        }