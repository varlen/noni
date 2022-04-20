from faker import Faker
import random

fake = Faker(['pt_BR','en_US'])

specific_generators = {
    'address': lambda : fake.street_address(),
    'age': lambda : str(random.uniform(21, 99)),
    'album': lambda : fake.sentence(nb_words = 4, variable_nb_words = True),
    'area': lambda : str(random.uniform(21, 99)),
    'artist': lambda : f"The {fake.word().capitalize()}s",
    'birthDate': lambda : fake.date(),
    'birthPlace': lambda : fake.city(),
    'brand': lambda : fake.company(),
    'city': lambda : fake.city(),
    'company': lambda : fake.company(),
    'continent': lambda : random.choice(['South America', 'North America', 'Africa', 'Europe', 'Central America', 'Asia', 'Australia']),
    'country': lambda : fake.country(),
    'creator': lambda : fake.name(),
    'credit': lambda : fake.credit_card_number(),
    'currency': lambda : fake.currency_name(),
    'description': lambda : fake.paragraphs(nb=4),
    'director': lambda : fake.name(),
    'family': lambda : fake.last_name(),
    'fileSize': lambda : fake.numerify('### KB' if fake.pybool() else '### MB'),
    'isbn': lambda : fake.isbn13(),
    'language': lambda : fake.locale(),
    'location': lambda : fake.city(),
    'manufacturer': lambda : fake.company(),
    'name': lambda : fake.name(),
    'operator': lambda : fake.name(),
    'organisation': fake.company(),
    'owner': lambda : fake.name(),
    'person': lambda : fake.name(),
    'position': lambda : fake.job(),
    'weight': lambda : str(random.uniform(1, 150))
}

def type78_generator(type):
    if not type in specific_generators:
        return None
    else:
        return specific_generators[type]