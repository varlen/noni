from faker import Faker
import random

fake = Faker(['pt_BR','en_US'])
# affiliate, affiliation, category, class, status, rank, type missing
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
    'code': lambda : fake.postcode(),
    'region' : lambda : fake.city(),
    'company': lambda : fake.company(),
    'continent': lambda : random.choice(['South America', 'North America', 'Africa', 'Europe', 'Central America', 'Asia', 'Australia']),
    'country': lambda : fake.country(),
    'creator': lambda : fake.name(),
    'credit': lambda : fake.credit_card_number(),
    'currency': lambda : fake.currency_name(),
    'description': lambda : '\n'.join(fake.paragraphs(nb=4)),
    'director': lambda : fake.name(),
    'family': lambda : fake.last_name(),
    'fileSize': lambda : fake.numerify('### KB' if fake.pybool() else '### MB'),
    'isbn': lambda : fake.isbn13(),
    'language': lambda : fake.language_name(),
    'location': lambda : fake.city(),
    'manufacturer': lambda : fake.company(),
    'name': lambda : fake.name(),
    'notes': lambda : '\n'.join(fake.paragraphs(nb=4)),
    'operator': lambda : fake.name(),
    'organisation': fake.company(),
    'owner': lambda : fake.name(),
    'person': lambda : fake.name(),
    'position': lambda : fake.job(),
    'rank': lambda : str(random.uniform(1, 150)),
    'weight': lambda : str(random.uniform(1, 150))
}

extended_generators = {
    'cpf': lambda : fake.cpf(),
    'email': lambda : fake.email(),
    'cnpj': lambda : fake.cnpj(),
    'creditcardnumber': lambda : fake.credit_card_number(),
    'phonenumber': lambda: fake.phone_number(),
    'licenseplate': lambda: fake.license_plate_mercosur()
}

def type78_generator(type):
    if not type in specific_generators:
        return None
    else:
        return specific_generators[type]

def regex_generator(type):
    if not type in extended_generators:
        return None
    else:
        return extended_generators[type]