FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt . 


# --trusted-host will tell docker to trust the site
# --no-cache-dir avoiding bloating image with cached files
RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows

RUN mkdir -p /opt/prefect/data/yellow

