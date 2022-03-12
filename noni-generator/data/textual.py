from multiprocessing import dummy
from faker import Faker
import random

fake = Faker(['pt_BR','en_US'])

def dummy_text():
    return lambda : 'TEXT'

generators = {
    'address': lambda : fake.street_address(),
    'affiliate': dummy_text(),
    'affiliation': dummy_text(),
    'age': lambda : str(random.uniform(21, 99)),
    'album': lambda : fake.sentence(nb_words = 4, variable_nb_words = True),
    'area': dummy_text(),
    'artist': lambda : f"The {fake.word().capitalize()}s",
    'birthDate': dummy_text(),
    'birthPlace': lambda : fake.city(),
    'brand': lambda : fake.company(),
    'capacity': dummy_text(),
    'category': dummy_text(),
    'city': lambda : fake.city(),
    'class': dummy_text(),
    'classification': dummy_text(),
    'club': dummy_text(),
    'code': dummy_text(),
    'collection': dummy_text(),
    'command': dummy_text(),
    'company': lambda : fake.company(),
    'component': dummy_text(),
    'continent': dummy_text(),
    'country': dummy_text(),
    'county': dummy_text(),
    'creator': dummy_text(),
    'credit': lambda : fake.credit_card_number(),
    'currency': lambda : fake.currency_name(),
    'day': dummy_text(),
    'depth': dummy_text(),
    'description': lambda : '', # TODO - Maybe change to lorem ipsum
    'director': dummy_text(),
    'duration': dummy_text(),
    'education': dummy_text(),
    'elevation': dummy_text(),
    'family': dummy_text(),
    'fileSize': lambda : fake.numerify('### KB' if fake.pybool() else '### MB'),
    'format': dummy_text(),
    'gender': dummy_text(), # Use samples from original data
    'genre': dummy_text(), # Movie genre?
    'grades': dummy_text(),
    'isbn': lambda : fake.isbn13(),
    'industry': dummy_text(),
    'jockey': dummy_text(),
    'language': lambda : fake.locale(),
    'location': lambda : fake.city(),
    'manufacturer': lambda : fake.company(),
    'name': lambda : fake.name(),
    'nationality': dummy_text(),
    'notes': dummy_text(),
    'operator': dummy_text(),
    'order': dummy_text(),
    'organisation': dummy_text(),
    'origin': dummy_text(),
    'owner': dummy_text(),
    'person': lambda : fake.name(),
    'plays': dummy_text(),
    'position': lambda : fake.job(),
    'product': dummy_text(),
    'publisher': dummy_text(),
    'range': dummy_text(),
    'rank': dummy_text(),
    'ranking': dummy_text(),
    'region': dummy_text(),
    'religion': dummy_text(),
    'requirement': dummy_text(),
    'result': dummy_text(),
    'sales': dummy_text(),
    'service': dummy_text(),
    'sex': dummy_text(), # Same as gender
    'species': dummy_text(),
    'state': dummy_text(),
    'status': dummy_text(),
    'symbol': dummy_text(),
    'team': dummy_text(),
    'teamName': dummy_text(),
    'type': dummy_text(),
    'weight': dummy_text(),
    'year': lambda : str(random.uniform(1950, 2050)) # TODO - Replace with min/max values
}

def type78_generator(sato_type):
    if not sato_type:
        return None
    else:
        generators[sato_type]