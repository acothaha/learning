# This script is created based on this site https://realpython.com/fastapi-python-web-apis/#what-is-fastapi


from fastapi import FastAPI

app = FastAPI()

# @app.get("/")
# async def root():
#     return {"message": "Hello World"}


# @app.get("/items/{item_id}")
# async def read_item(item_id: int):
#     return {"item_id": item_id}

from typing import Optional
import openai
from fastapi import FastAPI
from pydantic import BaseModel

def chatgpt_proofread(prompt, model="gpt-3.5-turbo"):

    role = "Imagine you're a professional proofreader, you are here to help me generate the best-optimized proofreading. I will provide you texts and I would like you to review them for any spelling, grammar, or punctuation errors"

    messages = [
        {"role": 'system', "content": role},
        {"role": 'user', "content": prompt}]

    response = openai.ChatCompletion.create(

    model=model,

    messages=messages,

    temperature=0,

    )

    return response

class Item(BaseModel):
    name: str
    description: Optional[str] = None
    price: float
    tax: Optional[float] = None

app = FastAPI()

@app.post("/items/")
async def create_item(item: Item):
    return item

# To run - python -m uvicorn main:app --reload