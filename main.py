from sqlalchemy import create_engine, Column, Integer, String, MetaData, ForeignKey, Float, Text
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
import pandas as pd
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from func import *
from requests.exceptions import SSLError, ConnectionError

engine = create_engine("sqlite:///city.db", echo=True)
base = declarative_base()
session = sessionmaker(bind=engine)()
meta = MetaData()
req_session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
req_session.mount('https://', adapter)
req_session.mount('https://', adapter)


class Country(base):
    __tablename__ = "country"

    id = Column(Integer, primary_key=True)
    country_name = Column(String, nullable=False, unique=True)
    about = Column(Text, nullable=True)
    iso2 = Column(String)
    phone_cod = Column(String)
    capital = Column(String)
    currency = Column(String)
    city = relationship("City", backref="city")


class City(base):
    __tablename__ = "city"

    id = Column(Integer, primary_key=True)
    city_id = Column(Integer)
    city_name = Column(String, nullable=False)
    population = Column(Float)
    lat = Column(Float)
    lng = Column(Float)
    about = Column(Text, nullable=True)
    country_id = Column(Integer, ForeignKey("country.id"))


base.metadata.create_all(engine)

# an empty list that will take the country we already save in database if code stop somehow in exception

passed_country = []

# i have two country with a same name with this variable because country name in DB is unique
# i manage to fix that
count = 0
# i use https://www.back4app.com/ API to get all country names and some more information
url = "https://parseapi.back4app.com/classes/Continentscountriescities_Country?limit=300&order=name"

headers = {
    'X-Parse-Application-Id': '',  # This is your app's application id
    'X-Parse-REST-API-Key': ''  # This is your app's REST API key
}

# getting data of the country's
response_country = req_session.get(url, headers=headers).json()


# this function will get https://www.back4app.com/ API response as input and
# Scraping the www.britannica.com for information about the country and add this data to database
def add_country(response):
    global passed_country, count
    db_data = session.query(Country).all()
    for country in db_data:
        passed_country.append(country.country_name)

    else:
        for item in response["results"]:
            cod = item["code"]
            county_name = item["name"]
            phone_cod = f'+{item["phone"]}'
            capital = item["capital"]
            currency = item["currency"]
            correct_name = decode_text(county_name)
            search_name = country_name(correct_name)
            if county_name == "Paraguay" and count == 0:
                count += 1
            if county_name == "Paraguay" and count == 1:
                pass
            else:
                if county_name not in passed_country:
                    #  decide if name of country is too long just scape that
                    if search_name != "":
                        search_name = search_name
                        url = f"https://www.britannica.com/place/{search_name}"
                        try:
                            response = req_session.get(url, timeout=60).text
                        except SSLError or ConnectionError:
                            passed_country = []
                            add_country(response_country)

                        soap = BeautifulSoup(response, 'html.parser')
                        if soap.find(class_="topic-paragraph"):
                            about_country = soap.find(class_="topic-paragraph").text

                        else:
                            about_country = ""
                        if about_country != "":
                            new_country = Country(country_name=county_name,
                                                  about=about_country,
                                                  iso2=cod,
                                                  phone_cod=phone_cod,
                                                  capital=capital,
                                                  currency=currency)
                            session.add(new_country)
                            session.commit()


passed_citys = []


# this function will search in a free dataset and add the city's that have more than 450k
# population to country's with relation between two table in DB
# first it tries to take information from en.wiki-voyage.org and if we got no result
# it will try en.wikipedia.org
# I decide if I found no information about city's ( most of the chines town have no information bac-aus
# their name are too hard don't add them to DB if you want to have data feel free to change the end of function
def add_city():
    global passed_citys, about_city
    past_city = session.query(City).all()
    for item in past_city:
        passed_citys.append(item.city_id)
    city_db = pd.read_csv("worldcities.csv")
    for index, row in city_db.iterrows():
        city_name = decode_text(row[0])
        print(city_name)
        lat = row[2]
        lng = row[3]
        iso2 = row["iso2"]
        population = row["population"]
        city_id = row['id']
        try:
            if int(population) >= 450000:

                if city_id not in passed_citys:
                    sreach_city_name = city_name_change(city_name)

                    if sreach_city_name != "":
                        url = f"https://en.wikivoyage.org/wiki/{sreach_city_name}"
                        try:
                            response_city = req_session.get(url, timeout=60).text

                        except SSLError or ConnectionError:
                            passed_citys = []
                            add_city()

                        soap = BeautifulSoup(response_city, 'html.parser')
                        if soap.find(id="mw-content-text"):
                            if soap.find("div", id="mw-content-text").find_all("p"):
                                texts = soap.find("div", id="mw-content-text").find_all("p")
                                if len(texts) >= 3:
                                    about_city = f"{texts[1].text}  {texts[2].text}"
                                elif len(texts) == 2:
                                    about_city = texts[1].text
                                else:
                                    about_city = ""
                        if about_city == "":
                            sreach_city_name = f"{sreach_city_name}_City"
                            url = f"https://en.wikivoyage.org/wiki/{sreach_city_name}"

                            try:
                                response_city = req_session.get(url, timeout=60).text

                            except SSLError or ConnectionError:
                                passed_citys = []
                                add_city()

                            soap = BeautifulSoup(response_city, 'html.parser')
                            if soap.find(id="mw-content-text"):
                                if soap.find(id="mw-content-text").find_all("p"):
                                    texts = soap.find(id="mw-content-text").find_all("p")
                                    if len(texts) >= 3:
                                        about_city = f"{texts[1].text}  {texts[2].text}"
                                    elif len(texts) == 2:
                                        about_city = texts[1].text
                                    else:
                                        about_city = ""
                        if about_city == "":
                            sreach_city_name = sreach_city_name.replace("_City", "")
                            url = f"https://en.wikipedia.org/wiki/{sreach_city_name}"
                            try:
                                response_city = req_session.get(url, timeout=60).text

                            except SSLError or ConnectionError:
                                passed_citys = []
                                add_city()
                            soap = BeautifulSoup(response_city, "html.parser")
                            if soap.find("p"):
                                about_city = soap.find("p").text
                            if len(about_city) == 1:
                                print("find")
                                about_city = soap.find_all("p")[1].text

                        if city_name == "Washington":
                            url = "https://en.wikipedia.org/wiki/Washington,_D.C."
                            try:
                                response_city = req_session.get(url, timeout=60).text

                            except SSLError or ConnectionError:
                                passed_citys = []
                                add_city()
                            soap = BeautifulSoup(response_city, "html.parser")
                            if soap.find("p"):
                                about_city = soap.find_all("p")[1].text

                        if about_city != "" and len(about_city) != 0:
                            try:
                                country_id = session.query(Country).filter_by(iso2=iso2).first()
                                if country_id:

                                    new_city = City(city_name=city_name,
                                                    city_id=city_id,
                                                    population=population,
                                                    lat=lat,
                                                    lng=lng,
                                                    about=about_city,
                                                    country_id=country_id.id)
                                    session.add(new_city)
                                    session.commit()
                                else:
                                    print("no country detect")
                            except AttributeError:

                                pass
        except ValueError:
            pass


add_country(response_country)
add_city()
