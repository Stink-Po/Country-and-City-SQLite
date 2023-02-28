from unidecode import unidecode


# this function will change the city name to a format that I want for web scraping
def city_name_change(name):
    search = ""
    split = name.split()
    if len(split) == 1:
        search = name.title()
    elif len(split) <= 3:
        search = "_".join(split)
    else:
        pass

    return search


# this function will change the Country name to a format that I want for web scraping
def country_name(name):
    search = ""
    split = name.split()
    if len(split) == 1:
        search = name.title()
    elif len(split) <= 3:
        search = "-".join(split)
    else:
        pass
    return search


# if we have some special name in the name of city or country this function will chnage it
def decode_text(text):
    result = unidecode(text)
    return result
