import scrapy

# creating a new class to act as the spider
# the class must inherit from scrapy.Spider
class QuotesSpider(scrapy.Spider):
    # name of the spider (must be unique)
    name = 'quotes_spider'

    # list of urls that want to be scrapped
    start_urls = ['http://quotes.toscrape.com',]

    # parse method is the main driver of a spider
    def parse(self, response):
        for quote in response.css('div.quote'):
            
            # after extracting the information, need to yield the item
            yield {
                'quote': quote.css('span.text::text').get(),
                'author': quote.xpath('span/small/text()').get(),
                'tags': quote.css('a.tag::text').extract(),
            }

        getting the next page link
        next_page = response.css('li.next > a::attr("href")').get()

        # make a request for the next page
        if next_page:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(url=next_page, callback=self.parse)