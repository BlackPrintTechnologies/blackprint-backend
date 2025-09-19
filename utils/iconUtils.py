import json


class IconMapper:
    CATEGORY_ICON_MAP = {
        'active_life': 'active+life.svg',
        'arts_and_entertainment': 'arts+and+entertainment.svg',
        'attractions_and_activities': 'attractions+and+activities.svg',
        'automotive': 'automotive.svg',
        'eat_and_drink': 'food.svg',
        'education': 'education.svg',
        'financial_service': 'financial+service.svg',
        'health_and_medical': 'health+and+medical.svg',
        'public_service_and_government': 'public+service+and+gov.svg',
        'retail': 'retail.svg',
        'travel': 'travel.svg',
        'mass_media': 'mass+media.svg',
        'beauty_and_spa': 'beauty+and+spa.svg',
        'accommodation': 'accomodation.svg',
        'religious_organization': 'religious+organization.svg',
        'professional_services': 'professional+services.svg',
        'private_establishments_and_corporates': 'private+corporates.svg',
        'home_service': 'home+service.svg',
        'structure_and_geography': 'geography.svg',
        'business_to_business': 'b2b.svg',
        'real_estate': 'Real+Estate+Agent+Icon.svg',
        'pets': 'pets.svg',
        # New category mappings
        'healthcare': 'health+and+medical.svg',
        'civil_society': 'public+service+and+gov.svg',
        'dining': 'food.svg',
        'beauty': 'beauty+and+spa.svg',
        'convenience_and_grocery_stores': 'retail.svg',
        'automotive_services': 'automotive.svg',
        'transportation_services': 'travel.svg',
        'entertainment_and_recreation': 'arts+and+entertainment.svg',
        'home_services': 'home+service.svg',
        'bars': 'food.svg',
        'uncategorized': 'default.svg'
    }

    # Logo mapper for the brands
    LOGO_ICON_MAP = {
        "oxxo": "OXXO.png",
        "western_union": "Western_Union.png",
        "el_asturiano": "Tiendas_El_Asturiano.png",
        "pemex": "Pemex.png",
        "gym": "Gym.png",
        "la_michoacana": "La_Michoacana.png",
        "tiendas_six": "Tiendas_Six.png",
        "banamexmexico": "Banamex.png",
        "modelorama": "Modelorama.png",
        "grupo_financiero_banorte": "Banorte.png",
        "farmacias_guadalajara": "Farmacias_Guadalajara.png",
        "bodega_aurrera": "Bodega_Aurrera.png",
        "farmacias_gi": "Farmacias_Gi.png",
        "allpoint": "Allpoint.png",
        "herbalife": "Herbalife.png",
        "amazon_locker": "Amazon_Locker.png",
        "eatbarbacoa": "Barbacoa.png",
        "subway": "SUBWAY.png",
        "dominos": "Domino's.png",
        "apteekki": "Heinäveden_apteekki.png",
        "mobil": "Mobil.png",
        "att": "AT&T.png",
        "walmart": "Walmart.png",
        "banco_azteca": "Banco_Azteca.png",
        "coppel": "Coppel.png",
        "fedex": "FedEx_Corporation.png",
        "shell": "Shell.png",
        "imss": "IMSS.png",
        "tiendas_3b": "Tiendas_3B.png",
        "amazon": "Amazon.png",
        "starbucks": "Starbucks.png",
        "santander": "Santander_Bank.png",
        "scotiabank": "Scotiabank.png",
        "bbva": "BBVA.png",
        "crossfit": "CrossFit.png",
        "western_union_moneygram_ria_transfer": "Western_Union_|_MoneyGram_|_Ria.png",
        "tesla": "Tesla_Stores_+_Service.png",
        "bp": "BP.png",
        "farmacias_del_ahorro": "Farmacias_del_Ahorro.png",
        "tim_hortons": "Tim_Hortons.png",
        "ups": "UPS.png",
        "mcdonalds": "McDonald's.png",
        "game": "GAME.png",
        "bkkw": "Burger_King.png",
        "pollofelizmexico": "Pollo_Feliz_Mexico.png",
        "kfc": "KFC.png",
        "laboratorio_medico_del_chopo": "Laboratorio_Medico_del_Chopo.png",
        "tortilla": "Tortilla.png",
        "cemex": "CEMEX.png",
        "benavidesmexico": "Farmacias_Benavides.png",
        "elektra": "Elektra.png",
        "hsbc": "HSBC_Bank_USA.png",
        "smart_fit": "Smart_Fit.png",
        "little_caesars_pizza": "Little_Caesars.png",
        "krispy_kreme": "Krispy_Kreme.png",
        "carls_jr": "Carl's_Jr..png",
        "a_express": "AEXPRESS.png",
        "bbvacompassbancshares": "BBVA_Compass_Bancshares.png",
        "honda": "Honda.png",
        "pizza_hut": "Pizza_Hut.png",
        "century_21": "Century_21.png",
        "chedraui": "Chedraui.png",
        "metlifeinsurance": "MetLife.png",
        "remax": "default.png",
        "heb": "H-E-B_Convenience_Store.png",
        "nutrisa": "Nutrisa.png",
        "first_cash": "First_Cash_Pawn.png",
        "sherwin_williams": "Sherwin-Williams.png",
        "devlyn": "Devlyn.png",
        "circle_k": "Circle_K.png",
        "mr_jeff": "Mr_Jeff.png",
        "natura": "Natura.png",
        "the_home_depot": "Home_Depot.png",
        "dhl": "DHL.png",
        "grupo_financiero_multiva": "Grupo_Financiero_Multiva.png",
        "sally_beauty": "Sally_Beauty.png",
        "cheve_fria": "Cheve_Fria.png",
        "suspiros_pastelerias": "Suspiros_Pastelerias.png",
        "nissan": "Nissan.png",
        "pharmacy": "Pharmacy.png",
        "soriana": "Soriana.png",
        "centro_de_estética": "Centro_De_Estética.png",
        "escuela": "Escuela.png",
        "issste": "ISSSTE.png",
        "banca_mifel": "Banca_Mifel.png",
        "oficina_de_correos": "oficina_de_correos.png",
        "gnc": "GNC_Canada_(General_Nutrition_Centers).png",
        "levi_strauss_and_co": "Levi_Strauss_&_Co..png",
        "cementos_fortaleza": "Cementos_Fortaleza.png",
        "anytime_fitness": "Anytime_Fitness.png",
        "biblioteca_publica_municipal": "Biblioteca_Pública_Municipal.png",
        "la_nueva_panaderia": "La_nueva_panadería.png",
        "farmacia_san_pablo": "Farmacia_San_Pablo.png",
        "clínica_dental": "Clínica_dental.png",
        "office_depot": "Office_Depot.png",
        "kumon": "Kumon_Institute_of_Education.png",
        "grupo_bimbo": "default.png",
        "ford": "Ford_Motor_Company.png",
        "universityofphoenix": "default.png",
        "chilis": "Chili's_Canada.png"
    }

    S3_BUCKET = 'black-print-assets'
    S3_PREFIX = 'pois/'
    S3_PREFIX_LOGO = 'queretaro_brands_icon/'

    @classmethod
    def get_icon_url(cls, category):
        """Get the S3 URL for an icon based on the category"""
        icon_file = cls.CATEGORY_ICON_MAP.get(category, 'default.svg')
        return f"https://{cls.S3_BUCKET}.s3.us-west-1.amazonaws.com/{cls.S3_PREFIX}{icon_file}"

    @classmethod
    def get_brand_url(cls, brand):
        """Get the S3 URL for a brand logo based on the brand name"""
        logo_file = cls.LOGO_ICON_MAP.get(brand, 'default.png')
        return f"https://{cls.S3_BUCKET}.s3.us-west-1.amazonaws.com/{cls.S3_PREFIX_LOGO}{logo_file}"


# Example Usage
icon_url = IconMapper.get_icon_url('active_life')
brand_url = IconMapper.get_brand_url('opticalux')
print(f"Category icon: {icon_url}")
print(f"Brand logo: {brand_url}")
