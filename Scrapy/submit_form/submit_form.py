import scrapy


class SubmitFormSpider(scrapy.Spider):
    name = 'login_spider'
    # allowed_domain = ['quotes.toscrape.com']
    login_url = 'http://quotes.toscrape.com/login'
    start_urls = [login_url]

    def parse(self, response):
        # extract the csrf token value
        token = response.css('input[name="csrf_token"]::attr(value)').get()
        # create a python dictionary with the form values
        data = {
            'username': 'john',
            'password': 'secret',
        }
        # submit a post request to it
        return scrapy.FormRequest.from_response(response, formdata=data, callback=self.parse_quotes)

    def parse_quotes(self, response):
        # Parse the main page after the spider is logged in
        for q in response.css('div.quote'):
            yield {
                'author_name': q.css('small.author::text').extract_first(),
                'author_url': q.css('small.author ~ a[href*="goodreads.com"]::attr[href]').extract_first()
                
            }


