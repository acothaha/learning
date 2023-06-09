import json
import scrapy

# creating a new class to act as the spider
# the class must inherit from scrapy.Spider
class QuotessSrollSpider(scrapy.Spider):
    # name of the spider (must be unique)
    name = 'quotes-scroll'

    api_url = 'https://quotes.toscrape.com/api/quotes?page={}'

    # list of urls that want to be scrapped
    start_urls = [api_url.format('1'),]

    # parse method is the main driver of a spider
    def parse(self, response):
        data = json.loads(response.text)

        for quote in data['quotes']:
            
            # after extracting the information, need to yield the item
            yield {
                'name': quote['author']['name'],
                'text': quote['text'],
                'tags': quote['tags'],
            }

        if data['has_next']:
            next_page = data['page'] + 1
            yield scrapy.Request(url=self.api_url.format(next_page), callback=self.parse)