from psycopg2.extras import RealDictCursor
from utils.dbUtils import Database, RedshiftDatabase
from utils.iconUtils import IconMapper
from utils.streetViewUtils import get_street_view_metadata_cached
import logging
import json
logger = logging.getLogger(__name__)
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Secret key for JWT
BASE_URL = config['BASE_URL']




def get_commercial_growth_json(result):
    result = result[0]
    return {
        "commercial_growth": {
            "block": {
                "total_businesses_2010": result["total_businesses_2010_ageb"],
                "total_businesses_2015": result["total_businesses_2015_ageb"],
                "economic_growth_2015": result["economic_growth_2015_ageb"],
                "total_businesses_2017": result["total_businesses_2017_ageb"],
                "economic_growth_2017": result["economic_growth_2017_ageb"],
                "total_businesses_2020": result["total_businesses_2020_ageb"],
                "economic_growth_2020": result["economic_growth_2020_ageb"],
                "total_businesses_2023": result["total_businesses_2023_ageb"],
                "economic_growth_2023": result["economic_growth_2023_ageb"]
            },
            "alcaldia": {
                "total_businesses_2010": result["total_businesses_2010_municipal"],
                "total_businesses_2015": result["total_businesses_2015_municipal"],
                "economic_growth_2015": result["economic_growth_2015_municipal"],
                "total_businesses_2017": result["total_businesses_2017_municipal"],
                "economic_growth_2017": result["economic_growth_2017_municipal"],
                "total_businesses_2020": result["total_businesses_2020_municipal"],
                "economic_growth_2020": result["economic_growth_2020_municipal"],
                "total_businesses_2023": result["total_businesses_2023_municipal"],
                "economic_growth_2023": result["economic_growth_2023_municipal"]
            },
            "colonia": {
                "total_businesses_2010": result["total_businesses_2010_entidad"],
                "total_businesses_2015": result["total_businesses_2015_entidad"],
                "economic_growth_2015": result["economic_growth_2015_entidad"],
                "total_businesses_2017": result["total_businesses_2017_entidad"],
                "economic_growth_2017": result["economic_growth_2017_entidad"],
                "total_businesses_2020": result["total_businesses_2020_entidad"],
                "economic_growth_2020": result["economic_growth_2020_entidad"],
                "total_businesses_2023": result["total_businesses_2023_entidad"],
                "economic_growth_2023": result["economic_growth_2023_entidad"]
            }
        },
        "categories": {
            "EAT_AND_DRINK": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_eat_and_drink_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_eat_and_drink_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_eat_and_drink_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_eat_and_drink_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_eat_and_drink_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_eat_and_drink_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_eat_and_drink_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_eat_and_drink_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_eat_and_drink_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_eat_and_drink_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_eat_and_drink_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_eat_and_drink_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_eat_and_drink_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_eat_and_drink_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_eat_and_drink_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_eat_and_drink_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_eat_and_drink_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_eat_and_drink_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_eat_and_drink_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_eat_and_drink_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_eat_and_drink_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_eat_and_drink_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_eat_and_drink_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_eat_and_drink_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_eat_and_drink_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_eat_and_drink_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_eat_and_drink_entidad"]
                }
            },
            "HEALTH_AND_MEDICAL": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_health_and_medical_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_health_and_medical_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_health_and_medical_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_health_and_medical_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_health_and_medical_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_health_and_medical_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_health_and_medical_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_health_and_medical_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_health_and_medical_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_health_and_medical_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_health_and_medical_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_health_and_medical_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_health_and_medical_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_health_and_medical_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_health_and_medical_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_health_and_medical_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_health_and_medical_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_health_and_medical_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_health_and_medical_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_health_and_medical_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_health_and_medical_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_health_and_medical_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_health_and_medical_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_health_and_medical_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_health_and_medical_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_health_and_medical_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_health_and_medical_entidad"]
                }
            },
            "BEAUTY_AND_SPA": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_beauty_and_spa_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_beauty_and_spa_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_beauty_and_spa_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_beauty_and_spa_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_beauty_and_spa_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_beauty_and_spa_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_beauty_and_spa_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_beauty_and_spa_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_beauty_and_spa_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_beauty_and_spa_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_beauty_and_spa_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_beauty_and_spa_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_beauty_and_spa_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_beauty_and_spa_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_beauty_and_spa_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_beauty_and_spa_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_beauty_and_spa_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_beauty_and_spa_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_beauty_and_spa_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_beauty_and_spa_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_beauty_and_spa_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_beauty_and_spa_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_beauty_and_spa_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_beauty_and_spa_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_beauty_and_spa_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_beauty_and_spa_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_beauty_and_spa_entidad"]
                }
            },
            "FINANCIAL_SERVICE": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_financial_service_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_financial_service_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_financial_service_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_financial_service_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_financial_service_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_financial_service_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_financial_service_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_financial_service_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_financial_service_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_financial_service_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_financial_service_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_financial_service_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_financial_service_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_financial_service_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_financial_service_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_financial_service_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_financial_service_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_financial_service_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_financial_service_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_financial_service_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_financial_service_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_financial_service_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_financial_service_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_financial_service_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_financial_service_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_financial_service_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_financial_service_entidad"]
                }
            },
            "ARTS_AND_ENTERTAINMENT": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_arts_and_entertainment_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_arts_and_entertainment_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_arts_and_entertainment_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_arts_and_entertainment_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_arts_and_entertainment_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_arts_and_entertainment_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_arts_and_entertainment_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_arts_and_entertainment_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_arts_and_entertainment_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_arts_and_entertainment_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_arts_and_entertainment_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_arts_and_entertainment_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_arts_and_entertainment_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_arts_and_entertainment_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_arts_and_entertainment_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_arts_and_entertainment_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_arts_and_entertainment_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_arts_and_entertainment_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_arts_and_entertainment_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_arts_and_entertainment_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_arts_and_entertainment_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_arts_and_entertainment_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_arts_and_entertainment_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_arts_and_entertainment_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_arts_and_entertainment_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_arts_and_entertainment_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_arts_and_entertainment_entidad"]
                }
            },
            "ACTIVE_LIFE": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_active_life_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_active_life_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_active_life_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_active_life_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_active_life_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_active_life_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_active_life_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_active_life_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_active_life_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_active_life_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_active_life_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_active_life_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_active_life_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_active_life_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_active_life_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_active_life_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_active_life_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_active_life_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_active_life_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_active_life_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_active_life_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_active_life_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_active_life_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_active_life_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_active_life_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_active_life_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_active_life_entidad"]
                }
            },
            "RETAIL": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_retail_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_retail_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_retail_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_retail_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_retail_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_retail_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_retail_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_retail_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_retail_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_retail_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_retail_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_retail_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_retail_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_retail_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_retail_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_retail_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_retail_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_retail_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_retail_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_retail_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_retail_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_retail_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_retail_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_retail_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_retail_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_retail_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_retail_entidad"]
                }
            },
            "PETS": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_pets_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_pets_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_pets_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_pets_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_pets_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_pets_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_pets_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_pets_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_pets_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_pets_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_pets_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_pets_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_pets_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_pets_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_pets_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_pets_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_pets_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_pets_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_pets_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_pets_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_pets_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_pets_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_pets_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_pets_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_pets_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_pets_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_pets_entidad"]
                }
            },
            "ATTRACTIONS_AND_ACTIVITIES": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_attractions_and_activities_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_attractions_and_activities_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_attractions_and_activities_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_attractions_and_activities_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_attractions_and_activities_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_attractions_and_activities_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_attractions_and_activities_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_attractions_and_activities_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_attractions_and_activities_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_attractions_and_activities_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_attractions_and_activities_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_attractions_and_activities_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_attractions_and_activities_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_attractions_and_activities_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_attractions_and_activities_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_attractions_and_activities_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_attractions_and_activities_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_attractions_and_activities_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_attractions_and_activities_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_attractions_and_activities_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_attractions_and_activities_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_attractions_and_activities_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_attractions_and_activities_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_attractions_and_activities_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_attractions_and_activities_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_attractions_and_activities_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_attractions_and_activities_entidad"]
                }
            },
            "EDUCATION": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_education_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_education_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_education_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_education_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_education_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_education_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_education_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_education_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_education_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_education_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_education_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_education_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_education_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_education_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_education_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_education_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_education_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_education_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_education_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_education_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_education_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_education_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_education_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_education_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_education_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_education_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_education_entidad"]
                }
            },
            "OTHERS": {
                "block": {
                    "total_businesses_2010": result["total_businesses_2010_others_ageb"],
                    "total_businesses_2015": result["total_businesses_2015_others_ageb"],
                    "economic_growth_2015": result["economic_growth_2015_others_ageb"],
                    "total_businesses_2017": result["total_businesses_2017_others_ageb"],
                    "economic_growth_2017": result["economic_growth_2017_others_ageb"],
                    "total_businesses_2020": result["total_businesses_2020_others_ageb"],
                    "economic_growth_2020": result["economic_growth_2020_others_ageb"],
                    "total_businesses_2023": result["total_businesses_2023_others_ageb"],
                    "economic_growth_2023": result["economic_growth_2023_others_ageb"]
                },
                "alcaldia": {
                    "total_businesses_2010": result["total_businesses_2010_others_municipal"],
                    "total_businesses_2015": result["total_businesses_2015_others_municipal"],
                    "economic_growth_2015": result["economic_growth_2015_others_municipal"],
                    "total_businesses_2017": result["total_businesses_2017_others_municipal"],
                    "economic_growth_2017": result["economic_growth_2017_others_municipal"],
                    "total_businesses_2020": result["total_businesses_2020_others_municipal"],
                    "economic_growth_2020": result["economic_growth_2020_others_municipal"],
                    "total_businesses_2023": result["total_businesses_2023_others_municipal"],
                    "economic_growth_2023": result["economic_growth_2023_others_municipal"]
                },
                "colonia": {
                    "total_businesses_2010": result["total_businesses_2010_others_entidad"],
                    "total_businesses_2015": result["total_businesses_2015_others_entidad"],
                    "economic_growth_2015": result["economic_growth_2015_others_entidad"],
                    "total_businesses_2017": result["total_businesses_2017_others_entidad"],
                    "economic_growth_2017": result["economic_growth_2017_others_entidad"],
                    "total_businesses_2020": result["total_businesses_2020_others_entidad"],
                    "economic_growth_2020": result["economic_growth_2020_others_entidad"],
                    "total_businesses_2023": result["total_businesses_2023_others_entidad"],
                    "economic_growth_2023": result["economic_growth_2023_others_entidad"]
                }
            }
        }
    } 
    
    
#Hepler controller function for the get property json
class HelperController:
    def __init__(self):
        self.redshift_connection = RedshiftDatabase()

    def _fetch_inmuebles24_images(self, ids_market_data_inmuebles24):
        """Fetch images from inmuebles24 table for the given id from Redshift."""
        connection = None
        cursor = None
        images = []
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = '''SELECT pictures FROM presentation.dim_market_data_inmuebles24 WHERE id_market_data_inmuebles24 = %s LIMIT 1'''
            cursor.execute(query, (ids_market_data_inmuebles24,))
            row = cursor.fetchone()
            if row and row.get('pictures'):
                # pictures is expected to be a JSON array or comma-separated string
                try:
                    # Try to parse as JSON
                    images = json.loads(row['pictures']) if isinstance(row['pictures'], str) else row['pictures']
                    if isinstance(images, str):
                        # If still a string, split by comma
                        images = [img.strip() for img in images.split(',') if img.strip()]
                except Exception:
                    # Fallback: split by comma
                    images = [img.strip() for img in row['pictures'].split(',') if img.strip()]
        except Exception as e:
            logger.error(f"Error fetching inmuebles24 images: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
        return images

    def _fetch_spot2_images(self, ids_market_data_spot2):
        """Fetch images from spot2 table for the given id from Redshift."""
        connection = None
        cursor = None
        images = []
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = '''SELECT pictures FROM presentation.dim_market_data_spot2 WHERE id_market_data_spot2 = %s LIMIT 1'''
            cursor.execute(query, (ids_market_data_spot2,))
            row = cursor.fetchone()
            
            if row and row.get('pictures') and row.get('pictures') != 'None':
                # pictures is expected to be a JSON array or comma-separated string
                try:
                    # Try to parse as JSON
                    images = json.loads(row['pictures']) if isinstance(row['pictures'], str) else row['pictures']
                    if isinstance(images, str):
                        # If still a string, split by comma
                        images = [img.strip() for img in images.split(',') if img.strip()]
                except Exception:
                    # Fallback: split by comma
                    images = [img.strip() for img in row['pictures'].split(',') if img.strip()]
        except Exception as e:
            logger.error(f"Error fetching spot2 images: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
        return images

    def _fetch_propiedades_images(self, ids_market_data_propiedades):
        """Fetch images from propiedades table for the given id from Redshift."""
        connection = None
        cursor = None
        images = []
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = '''SELECT image_1, image_2, image_3, image_4, image_5 
                      FROM presentation.dim_market_data_propiedades 
                      WHERE id_market_data_propiedades = %s 
                      LIMIT 1'''
            cursor.execute(query, (ids_market_data_propiedades,))
            row = cursor.fetchone()
            if row:
                # Collect all non-null image URLs
                for i in range(1, 6):
                    image_url = row.get(f'image_{i}')
                    if image_url and isinstance(image_url, str) and image_url.strip():
                        images.append(image_url.strip())
        except Exception as e:
            logger.error(f"Error fetching propiedades images: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
        return images
    
    # helper for the get property json

    def helper_get_property_json(self,results):
            resp = []
            try:
                logger.info("Processing property JSON for %d results", len(results))
                for result in results:
                    property_details = {
                        "fid": result["fid"],
                        "lat": json.loads(result['centroid'])['coordinates'][1] if result['centroid'] else None,
                        "lng" : json.loads(result['centroid'])['coordinates'][0] if result['centroid'] else None,
                        "is_on_market": result["is_on_market"],
                        "total_surface_area": result["total_surface_area"],
                        "total_construction_area": result["total_construction_area"],
                        "total_built_perm": result['total_built_perm'],
                        "total_units": result['property_count_per_lot'],
                        "street_address": result["street_address"],
                        "year_built": result["year_built"],
                        "special_facilities": result["special_facilities"],
                        "unit_land_value": result["unit_land_value"],
                        "land_value": result["land_value"],
                        "usage_desc": result["usage_desc"],
                        "key_vus": result["key_vus"],
                        "predominant_level": result["predominant_level"],
                        "h3_indexes": result["h3_indexes"],
                        "height": result.get("height", None),
                        "cos": result.get("cos", None),
                        "cus": result.get("cus", None),
                        "min_housing": result.get("min_housing", None),
                        "ids_market_data_inmuebles24": result.get("ids_market_data_inmuebles24", None),
                        "ids_market_data_propiedades": result.get("ids_market_data_propiedades", None),
                        "crecimiento_promedio_municipal": result.get("crecimiento_promedio_municipal", None),
                        "crecimiento_promedio_entidad": result.get("crecimiento_promedio_entidad", None),
                        "crecimiento_promedio_ageb": result.get("crecimiento_promedio_ageb", None)
                    }
                    # --- Set street_images based on priority: inmuebles24 -> spot2 -> propiedades -> street view ---
                    street_images = []

                    # Pre-process all IDs to get valid ones (excluding -1)
                    valid_inmuebles24_id = None
                    if property_details.get("ids_market_data_inmuebles24"):
                        inmuebles24_ids = [id_.strip() for id_ in str(property_details["ids_market_data_inmuebles24"]).split(',') if id_.strip()]
                        valid_ids = [id_ for id_ in inmuebles24_ids if id_ != '-1']
                        if valid_ids:
                            valid_inmuebles24_id = valid_ids[0]

                    valid_spot2_id = None
                    if result.get("ids_market_data_spot2"):
                        spot2_ids = [id_.strip() for id_ in str(result["ids_market_data_spot2"]).split(',') if id_.strip()]
                        valid_ids = [id_ for id_ in spot2_ids if id_ != '-1']
                        if valid_ids:
                            valid_spot2_id = valid_ids[0]

                    valid_propiedades_id = None
                    if result.get("ids_market_data_propiedades"):
                        print("ids_market_data_propiedades", result["ids_market_data_propiedades"])
                        propiedades_ids = [id_.strip() for id_ in str(result["ids_market_data_propiedades"]).split(',') if id_.strip()]
                        valid_ids = [id_ for id_ in propiedades_ids if id_ != '-1']
                        if valid_ids:
                            valid_propiedades_id = valid_ids[0]
                    print("valid id", valid_ids)
                    # Now use the valid IDs in the if-elif ladder
                    if valid_inmuebles24_id:
                        images = self._fetch_inmuebles24_images(valid_inmuebles24_id)
                        print("INMUEBLES24 IMAGES", images)
                        if images:
                            # Resize images to 600x300
                            street_images = [
                                img.replace("1200x1200", "600x300") if "1200x1200" in img else img
                                for img in images[:6]
                            ]

                    # If no inmuebles24 images, try spot2
                    elif valid_spot2_id:
                        spot2_images = self._fetch_spot2_images(valid_spot2_id)
                        print("SPOT2 IMAGES", spot2_images)
                        if spot2_images:
                            street_images = spot2_images[:6]

                    # If no spot2 images, try propiedades
                    elif valid_propiedades_id:
                        propiedades_images = self._fetch_propiedades_images(valid_propiedades_id)
                        print("PROPIEDADES IMAGES", propiedades_images)
                        if propiedades_images:
                            street_images = propiedades_images[:6]

                    # If no marketplace images found, try street view
                    if not street_images and property_details.get("lat") and property_details.get("lng"):
                        prop_lat = property_details["lat"]
                        prop_lng = property_details["lng"]
                        pano_id = get_street_view_metadata_cached(float(prop_lat), float(prop_lng))
                        if pano_id:
                            headings = [0, 45, 90, 135, 180, 225, 270, 315]
                            fov = 90
                            size = "600x300"
                            street_images = [
                                f"{BASE_URL}/properties/street_view_image?pano_id={pano_id}&heading={heading}&fov={fov}&size={size}"
                                for heading in headings
                            ]
                    # Filter out None values 
                    street_images = [img for img in street_images if img and img.lower() != "none"]
                    property_details["street_images"] = street_images

                    market_info = {
                        "ids_market_data_spot2" : result["ids_market_data_spot2"],
                        "ids_market_data_inmuebles24" : result["ids_market_data_inmuebles24"],
                        "ids_market_data_propiedades" : result["ids_market_data_propiedades"],
                        "rent_price_spot2": result["rent_price_spot2"],
                        "rent_price_per_m2_spot2": result["rent_price_per_m2_spot2"],
                        "buy_price_spot2": result["buy_price_spot2"],
                        "buy_price_per_m2_spot2": result["buy_price_per_m2_spot2"],
                        "total_area_spot2": result["total_area_spot2"],
                        "property_type_spot2": result["property_type_spot2"],
                        "rent_price_inmuebles24": result["rent_price_inmuebles24"],
                        "rent_price_per_m2_inmuebles24": result["rent_price_per_m2_inmuebles24"],
                        "buy_price_inmuebles24": result["buy_price_inmuebles24"],
                        "buy_price_per_m2_inmuebles24": result["buy_price_per_m2_inmuebles24"],
                        "total_area_inmuebles24": result["total_area_inmuebles24"],
                        "property_type_inmuebles24": result["property_type_inmuebles24"],
                        "rent_price_propiedades": result["rent_price_propiedades"],
                        "rent_price_per_m2_propiedades": result["rent_price_per_m2_propiedades"],
                        "buy_price_propiedades": result["buy_price_propiedades"],
                        "buy_price_per_m2_propiedades": result["buy_price_per_m2_propiedades"],
                        "total_area_propiedades": result["total_area_propiedades"],
                        "block_type": result["block_type"],
                        "density_d": result["density_d"],
                        "scope": result["scope"],
                        "floor_levels": result["floor_levels"],
                        "open_space" : result["open_space"],
                        "id_land_use": result['id_land_use'],
                        "id_municipality": result["id_municipality"],
                        "id_city_blocks": result["id_city_blocks"],
                        "total_houses": result["total_houses"],
                        "locality_size": result["locality_size"],
                        "city_link": result["city_link"]
                    }

                    pois = {
                        #add category here for icon image
                        "category": {
                            category: IconMapper.get_icon_url(category) 
                            for category in IconMapper.CATEGORY_ICON_MAP
                        },
                        "front" : {
                            "brands_active_life_front": result["brands_active_life_front"],
                            "brands_arts_and_entertainment_front": result["brands_arts_and_entertainment_front"],
                            "brands_attractions_and_activities_front": result["brands_attractions_and_activities_front"],
                            "brands_automotive_front": result["brands_automotive_front"],
                            "brands_eat_and_drink_front": result["brands_eat_and_drink_front"],
                            "brands_education_front": result["brands_education_front"],
                            "brands_financial_service_front": result["brands_financial_service_front"],
                            "brands_health_and_medical_front": result["brands_health_and_medical_front"],
                            "brands_public_service_and_government_front": result["brands_public_service_and_government_front"],
                            "brands_retail_front": result["brands_retail_front"],
                        },
                        "500" : {
                            "brands_active_life_500m": result["brands_active_life_500m"],
                            "brands_arts_and_entertainment_500m": result["brands_arts_and_entertainment_500m"],
                            "brands_attractions_and_activities_500m": result["brands_attractions_and_activities_500m"],
                            "brands_automotive_500m": result["brands_automotive_500m"],
                            "brands_eat_and_drink_500m": result["brands_eat_and_drink_500m"],
                            "brands_education_500m": result["brands_education_500m"],
                            "brands_financial_service_500m": result["brands_financial_service_500m"],
                            "brands_health_and_medical_500m": result["brands_health_and_medical_500m"],
                            "brands_public_service_and_government_500m": result["brands_public_service_and_government_500m"],
                            "brands_retail_500m": result["brands_retail_500m"],
                        },
                        "1000" : {
                            "brands_active_life_1km": result["brands_active_life_1km"],
                            "brands_arts_and_entertainment_1km": result["brands_arts_and_entertainment_1km"],
                            "brands_attractions_and_activities_1km": result["brands_attractions_and_activities_1km"],
                            "brands_automotive_1km": result["brands_automotive_1km"],
                            "brands_eat_and_drink_1km": result["brands_eat_and_drink_1km"],
                            "brands_education_1km": result["brands_education_1km"],
                            "brands_financial_service_1km": result["brands_financial_service_1km"],
                            "brands_health_and_medical_1km": result["brands_health_and_medical_1km"],
                            "brands_public_service_and_government_1km": result["brands_public_service_and_government_1km"],
                            "brands_retail_1km": result["brands_retail_1km"]
                        },  
                    }

                    traffic = {
                        "front" : {
                        "at_rest_avg_x_hour_0_front": result["at_rest_avg_x_hour_0_front"],
                        "pedestrian_avg_x_hour_0_front": result["pedestrian_avg_x_hour_0_front"],
                        "motor_vehicle_avg_x_hour_0_front": result["motor_vehicle_avg_x_hour_0_front"],
                        "at_rest_avg_x_hour_1_front": result["at_rest_avg_x_hour_1_front"],
                        "pedestrian_avg_x_hour_1_front": result["pedestrian_avg_x_hour_1_front"],
                        "motor_vehicle_avg_x_hour_1_front": result["motor_vehicle_avg_x_hour_1_front"],
                        "at_rest_avg_x_hour_2_front": result["at_rest_avg_x_hour_2_front"],
                        "pedestrian_avg_x_hour_2_front": result["pedestrian_avg_x_hour_2_front"],
                        "motor_vehicle_avg_x_hour_2_front": result["motor_vehicle_avg_x_hour_2_front"],
                        "at_rest_avg_x_hour_3_front": result["at_rest_avg_x_hour_3_front"],
                        "pedestrian_avg_x_hour_3_front": result["pedestrian_avg_x_hour_3_front"],
                        "motor_vehicle_avg_x_hour_3_front": result["motor_vehicle_avg_x_hour_3_front"],
                        "at_rest_avg_x_hour_4_front": result["at_rest_avg_x_hour_4_front"],
                        "pedestrian_avg_x_hour_4_front": result["pedestrian_avg_x_hour_4_front"],
                        "motor_vehicle_avg_x_hour_4_front": result["motor_vehicle_avg_x_hour_4_front"],
                        "at_rest_avg_x_hour_5_front": result["at_rest_avg_x_hour_5_front"],
                        "pedestrian_avg_x_hour_5_front": result["pedestrian_avg_x_hour_5_front"],
                        "motor_vehicle_avg_x_hour_5_front": result["motor_vehicle_avg_x_hour_5_front"],
                        "at_rest_avg_x_hour_6_front": result["at_rest_avg_x_hour_6_front"],
                        "pedestrian_avg_x_hour_6_front": result["pedestrian_avg_x_hour_6_front"],
                        "motor_vehicle_avg_x_hour_6_front": result["motor_vehicle_avg_x_hour_6_front"],
                        "at_rest_avg_x_hour_7_front": result["at_rest_avg_x_hour_7_front"],
                        "pedestrian_avg_x_hour_7_front": result["pedestrian_avg_x_hour_7_front"],
                        "motor_vehicle_avg_x_hour_7_front": result["motor_vehicle_avg_x_hour_7_front"],
                        "at_rest_avg_x_hour_8_front": result["at_rest_avg_x_hour_8_front"],
                        "pedestrian_avg_x_hour_8_front": result["pedestrian_avg_x_hour_8_front"],
                        "motor_vehicle_avg_x_hour_8_front": result["motor_vehicle_avg_x_hour_8_front"],
                        "at_rest_avg_x_hour_9_front": result["at_rest_avg_x_hour_9_front"],
                        "pedestrian_avg_x_hour_9_front": result["pedestrian_avg_x_hour_9_front"],
                        "motor_vehicle_avg_x_hour_9_front": result["motor_vehicle_avg_x_hour_9_front"],
                        "at_rest_avg_x_hour_10_front": result["at_rest_avg_x_hour_10_front"],
                        "pedestrian_avg_x_hour_10_front": result["pedestrian_avg_x_hour_10_front"],
                        "motor_vehicle_avg_x_hour_10_front": result["motor_vehicle_avg_x_hour_10_front"],
                        "at_rest_avg_x_hour_11_front": result["at_rest_avg_x_hour_11_front"],
                        "pedestrian_avg_x_hour_11_front": result["pedestrian_avg_x_hour_11_front"],
                        "motor_vehicle_avg_x_hour_11_front": result["motor_vehicle_avg_x_hour_11_front"],
                        "at_rest_avg_x_hour_12_front": result["at_rest_avg_x_hour_12_front"],
                        "pedestrian_avg_x_hour_12_front": result["pedestrian_avg_x_hour_12_front"],
                        "motor_vehicle_avg_x_hour_12_front": result["motor_vehicle_avg_x_hour_12_front"],
                        "at_rest_avg_x_hour_13_front": result["at_rest_avg_x_hour_13_front"],
                        "pedestrian_avg_x_hour_13_front": result["pedestrian_avg_x_hour_13_front"],
                        "motor_vehicle_avg_x_hour_13_front": result["motor_vehicle_avg_x_hour_13_front"],
                        "at_rest_avg_x_hour_14_front": result["at_rest_avg_x_hour_14_front"],
                        "pedestrian_avg_x_hour_14_front": result["pedestrian_avg_x_hour_14_front"],
                        "motor_vehicle_avg_x_hour_14_front": result["motor_vehicle_avg_x_hour_14_front"],
                        "at_rest_avg_x_hour_15_front": result["at_rest_avg_x_hour_15_front"],
                        "pedestrian_avg_x_hour_15_front": result["pedestrian_avg_x_hour_15_front"],
                        "motor_vehicle_avg_x_hour_15_front": result["motor_vehicle_avg_x_hour_15_front"],
                        "at_rest_avg_x_hour_16_front": result["at_rest_avg_x_hour_16_front"],
                        "pedestrian_avg_x_hour_16_front": result["pedestrian_avg_x_hour_16_front"],
                        "motor_vehicle_avg_x_hour_16_front": result["motor_vehicle_avg_x_hour_16_front"],
                        "at_rest_avg_x_hour_17_front": result["at_rest_avg_x_hour_17_front"],
                        "pedestrian_avg_x_hour_17_front": result["pedestrian_avg_x_hour_17_front"],
                        "motor_vehicle_avg_x_hour_17_front": result["motor_vehicle_avg_x_hour_17_front"],
                        "at_rest_avg_x_hour_18_front": result["at_rest_avg_x_hour_18_front"],
                        "pedestrian_avg_x_hour_18_front": result["pedestrian_avg_x_hour_18_front"],
                        "motor_vehicle_avg_x_hour_18_front": result["motor_vehicle_avg_x_hour_18_front"],
                        "at_rest_avg_x_hour_19_front": result["at_rest_avg_x_hour_19_front"],
                        "pedestrian_avg_x_hour_19_front": result["pedestrian_avg_x_hour_19_front"],
                        "motor_vehicle_avg_x_hour_19_front": result["motor_vehicle_avg_x_hour_19_front"],
                        "at_rest_avg_x_hour_20_front": result["at_rest_avg_x_hour_20_front"],
                        "pedestrian_avg_x_hour_20_front": result["pedestrian_avg_x_hour_20_front"],
                        "motor_vehicle_avg_x_hour_20_front": result["motor_vehicle_avg_x_hour_20_front"],
                        "at_rest_avg_x_hour_21_front": result["at_rest_avg_x_hour_21_front"],
                        "pedestrian_avg_x_hour_21_front": result["pedestrian_avg_x_hour_21_front"],
                        "motor_vehicle_avg_x_hour_21_front": result["motor_vehicle_avg_x_hour_21_front"],
                        "at_rest_avg_x_hour_22_front": result["at_rest_avg_x_hour_22_front"],
                        "pedestrian_avg_x_hour_22_front": result["pedestrian_avg_x_hour_22_front"],
                        "motor_vehicle_avg_x_hour_22_front": result["motor_vehicle_avg_x_hour_22_front"],
                        "at_rest_avg_x_hour_23_front": result["at_rest_avg_x_hour_23_front"],
                        "pedestrian_avg_x_hour_23_front": result["pedestrian_avg_x_hour_23_front"],
                        "motor_vehicle_avg_x_hour_23_front": result["motor_vehicle_avg_x_hour_23_front"],
                        "at_rest_avg_x_day_of_week_1_front": result["at_rest_avg_x_day_of_week_1_front"],
                        "pedestrian_avg_x_day_of_week_1_front": result["pedestrian_avg_x_day_of_week_1_front"],
                        "motor_vehicle_avg_x_day_of_week_1_front": result["motor_vehicle_avg_x_day_of_week_1_front"],
                        "at_rest_avg_x_day_of_week_2_front": result["at_rest_avg_x_day_of_week_2_front"],
                        "pedestrian_avg_x_day_of_week_2_front": result["pedestrian_avg_x_day_of_week_2_front"],
                        "motor_vehicle_avg_x_day_of_week_2_front": result["motor_vehicle_avg_x_day_of_week_2_front"],
                        "at_rest_avg_x_day_of_week_3_front": result["at_rest_avg_x_day_of_week_3_front"],
                        "pedestrian_avg_x_day_of_week_3_front": result["pedestrian_avg_x_day_of_week_3_front"],
                        "motor_vehicle_avg_x_day_of_week_3_front": result["motor_vehicle_avg_x_day_of_week_3_front"],
                        "at_rest_avg_x_day_of_week_4_front": result["at_rest_avg_x_day_of_week_4_front"],
                        "pedestrian_avg_x_day_of_week_4_front": result["pedestrian_avg_x_day_of_week_4_front"],
                        "motor_vehicle_avg_x_day_of_week_4_front": result["motor_vehicle_avg_x_day_of_week_4_front"],
                        "at_rest_avg_x_day_of_week_5_front": result["at_rest_avg_x_day_of_week_5_front"],
                        "pedestrian_avg_x_day_of_week_5_front": result["pedestrian_avg_x_day_of_week_5_front"],
                        "motor_vehicle_avg_x_day_of_week_5_front": result["motor_vehicle_avg_x_day_of_week_5_front"],
                        "at_rest_avg_x_day_of_week_6_front": result["at_rest_avg_x_day_of_week_6_front"],
                        "pedestrian_avg_x_day_of_week_6_front": result["pedestrian_avg_x_day_of_week_6_front"],
                        "motor_vehicle_avg_x_day_of_week_6_front": result["motor_vehicle_avg_x_day_of_week_6_front"],
                        "at_rest_avg_x_day_of_week_7_front": result["at_rest_avg_x_day_of_week_7_front"],
                        "pedestrian_avg_x_day_of_week_7_front": result["pedestrian_avg_x_day_of_week_7_front"],
                        "motor_vehicle_avg_x_day_of_week_7_front": result["motor_vehicle_avg_x_day_of_week_7_front"],
                        } ,
                        "500" : {
                        "at_rest_avg_x_day_of_week_1_500m": result["at_rest_avg_x_day_of_week_1_500m"],
                        "pedestrian_avg_x_day_of_week_1_500m": result["pedestrian_avg_x_day_of_week_1_500m"],
                        "motor_vehicle_avg_x_day_of_week_1_500m": result["motor_vehicle_avg_x_day_of_week_1_500m"],
                        "at_rest_avg_x_day_of_week_2_500m": result["at_rest_avg_x_day_of_week_2_500m"],
                        "pedestrian_avg_x_day_of_week_2_500m": result["pedestrian_avg_x_day_of_week_2_500m"],
                        "motor_vehicle_avg_x_day_of_week_2_500m": result["motor_vehicle_avg_x_day_of_week_2_500m"],
                        "at_rest_avg_x_day_of_week_3_500m": result["at_rest_avg_x_day_of_week_3_500m"],
                        "pedestrian_avg_x_day_of_week_3_500m": result["pedestrian_avg_x_day_of_week_3_500m"],
                        "motor_vehicle_avg_x_day_of_week_3_500m": result["motor_vehicle_avg_x_day_of_week_3_500m"],
                        "at_rest_avg_x_day_of_week_4_500m": result["at_rest_avg_x_day_of_week_4_500m"],
                        "pedestrian_avg_x_day_of_week_4_500m": result["pedestrian_avg_x_day_of_week_4_500m"],
                        "motor_vehicle_avg_x_day_of_week_4_500m": result["motor_vehicle_avg_x_day_of_week_4_500m"],
                        "at_rest_avg_x_day_of_week_5_500m": result["at_rest_avg_x_day_of_week_5_500m"],
                        "pedestrian_avg_x_day_of_week_5_500m": result["pedestrian_avg_x_day_of_week_5_500m"],
                        "motor_vehicle_avg_x_day_of_week_5_500m": result["motor_vehicle_avg_x_day_of_week_5_500m"],
                        "at_rest_avg_x_day_of_week_6_500m": result["at_rest_avg_x_day_of_week_6_500m"],
                        "pedestrian_avg_x_day_of_week_6_500m": result["pedestrian_avg_x_day_of_week_6_500m"],
                        "motor_vehicle_avg_x_day_of_week_6_500m": result["motor_vehicle_avg_x_day_of_week_6_500m"],
                        "at_rest_avg_x_day_of_week_7_500m": result["at_rest_avg_x_day_of_week_7_500m"],
                        "pedestrian_avg_x_day_of_week_7_500m": result["pedestrian_avg_x_day_of_week_7_500m"],
                        "motor_vehicle_avg_x_day_of_week_7_500m": result["motor_vehicle_avg_x_day_of_week_7_500m"],
                        "at_rest_avg_x_hour_0_500m": result["at_rest_avg_x_hour_0_500m"],
                        "pedestrian_avg_x_hour_0_500m": result["pedestrian_avg_x_hour_0_500m"],
                        "motor_vehicle_avg_x_hour_0_500m": result["motor_vehicle_avg_x_hour_0_500m"],
                        "at_rest_avg_x_hour_1_500m": result["at_rest_avg_x_hour_1_500m"],
                        "pedestrian_avg_x_hour_1_500m": result["pedestrian_avg_x_hour_1_500m"],
                        "motor_vehicle_avg_x_hour_1_500m": result["motor_vehicle_avg_x_hour_1_500m"],
                        "at_rest_avg_x_hour_2_500m": result["at_rest_avg_x_hour_2_500m"],
                        "pedestrian_avg_x_hour_2_500m": result["pedestrian_avg_x_hour_2_500m"],
                        "motor_vehicle_avg_x_hour_2_500m": result["motor_vehicle_avg_x_hour_2_500m"],
                        "at_rest_avg_x_hour_3_500m": result["at_rest_avg_x_hour_3_500m"],
                        "pedestrian_avg_x_hour_3_500m": result["pedestrian_avg_x_hour_3_500m"],
                        "motor_vehicle_avg_x_hour_3_500m": result["motor_vehicle_avg_x_hour_3_500m"],
                        "at_rest_avg_x_hour_4_500m": result["at_rest_avg_x_hour_4_500m"],
                        "pedestrian_avg_x_hour_4_500m": result["pedestrian_avg_x_hour_4_500m"],
                        "motor_vehicle_avg_x_hour_4_500m": result["motor_vehicle_avg_x_hour_4_500m"],
                        "at_rest_avg_x_hour_5_500m": result["at_rest_avg_x_hour_5_500m"],
                        "pedestrian_avg_x_hour_5_500m": result["pedestrian_avg_x_hour_5_500m"],
                        "motor_vehicle_avg_x_hour_5_500m": result["motor_vehicle_avg_x_hour_5_500m"],
                        "at_rest_avg_x_hour_6_500m": result["at_rest_avg_x_hour_6_500m"],
                        "pedestrian_avg_x_hour_6_500m": result["pedestrian_avg_x_hour_6_500m"],
                        "motor_vehicle_avg_x_hour_6_500m": result["motor_vehicle_avg_x_hour_6_500m"],
                        "at_rest_avg_x_hour_7_500m": result["at_rest_avg_x_hour_7_500m"],
                        "pedestrian_avg_x_hour_7_500m": result["pedestrian_avg_x_hour_7_500m"],
                        "motor_vehicle_avg_x_hour_7_500m": result["motor_vehicle_avg_x_hour_7_500m"],
                        "at_rest_avg_x_hour_8_500m": result["at_rest_avg_x_hour_8_500m"],
                        "pedestrian_avg_x_hour_8_500m": result["pedestrian_avg_x_hour_8_500m"],
                        "motor_vehicle_avg_x_hour_8_500m": result["motor_vehicle_avg_x_hour_8_500m"],
                        "at_rest_avg_x_hour_9_500m": result["at_rest_avg_x_hour_9_500m"],
                        "pedestrian_avg_x_hour_9_500m": result["pedestrian_avg_x_hour_9_500m"],
                        "motor_vehicle_avg_x_hour_9_500m": result["motor_vehicle_avg_x_hour_9_500m"],
                        "at_rest_avg_x_hour_10_500m": result["at_rest_avg_x_hour_10_500m"],
                        "pedestrian_avg_x_hour_10_500m": result["pedestrian_avg_x_hour_10_500m"],
                        "motor_vehicle_avg_x_hour_10_500m": result["motor_vehicle_avg_x_hour_10_500m"],
                        "at_rest_avg_x_hour_11_500m": result["at_rest_avg_x_hour_11_500m"],
                        "pedestrian_avg_x_hour_11_500m": result["pedestrian_avg_x_hour_11_500m"],
                        "motor_vehicle_avg_x_hour_11_500m": result["motor_vehicle_avg_x_hour_11_500m"],
                        "at_rest_avg_x_hour_12_500m": result["at_rest_avg_x_hour_12_500m"],
                        "pedestrian_avg_x_hour_12_500m": result["pedestrian_avg_x_hour_12_500m"],
                        "motor_vehicle_avg_x_hour_12_500m": result["motor_vehicle_avg_x_hour_12_500m"],
                        "at_rest_avg_x_hour_13_500m": result["at_rest_avg_x_hour_13_500m"],
                        "pedestrian_avg_x_hour_13_500m": result["pedestrian_avg_x_hour_13_500m"],
                        "motor_vehicle_avg_x_hour_13_500m": result["motor_vehicle_avg_x_hour_13_500m"],
                        "at_rest_avg_x_hour_14_500m": result["at_rest_avg_x_hour_14_500m"],
                        "pedestrian_avg_x_hour_14_500m": result["pedestrian_avg_x_hour_14_500m"],
                        "motor_vehicle_avg_x_hour_14_500m": result["motor_vehicle_avg_x_hour_14_500m"],
                        "at_rest_avg_x_hour_15_500m": result["at_rest_avg_x_hour_15_500m"],
                        "pedestrian_avg_x_hour_15_500m": result["pedestrian_avg_x_hour_15_500m"],
                        "motor_vehicle_avg_x_hour_15_500m": result["motor_vehicle_avg_x_hour_15_500m"],
                        "at_rest_avg_x_hour_16_500m": result["at_rest_avg_x_hour_16_500m"],
                        "pedestrian_avg_x_hour_16_500m": result["pedestrian_avg_x_hour_16_500m"],
                        "motor_vehicle_avg_x_hour_16_500m": result["motor_vehicle_avg_x_hour_16_500m"],
                        "at_rest_avg_x_hour_17_500m": result["at_rest_avg_x_hour_17_500m"],
                        "pedestrian_avg_x_hour_17_500m": result["pedestrian_avg_x_hour_17_500m"],
                        "motor_vehicle_avg_x_hour_17_500m": result["motor_vehicle_avg_x_hour_17_500m"],
                        "at_rest_avg_x_hour_18_500m": result["at_rest_avg_x_hour_18_500m"],
                        "pedestrian_avg_x_hour_18_500m": result["pedestrian_avg_x_hour_18_500m"],
                        "motor_vehicle_avg_x_hour_18_500m": result["motor_vehicle_avg_x_hour_18_500m"],
                        "at_rest_avg_x_hour_19_500m": result["at_rest_avg_x_hour_19_500m"],
                        "pedestrian_avg_x_hour_19_500m": result["pedestrian_avg_x_hour_19_500m"],
                        "motor_vehicle_avg_x_hour_19_500m": result["motor_vehicle_avg_x_hour_19_500m"],
                        "at_rest_avg_x_hour_20_500m": result["at_rest_avg_x_hour_20_500m"],
                        "pedestrian_avg_x_hour_20_500m": result["pedestrian_avg_x_hour_20_500m"],
                        "motor_vehicle_avg_x_hour_20_500m": result["motor_vehicle_avg_x_hour_20_500m"],
                        "at_rest_avg_x_hour_21_500m": result["at_rest_avg_x_hour_21_500m"],
                        "pedestrian_avg_x_hour_21_500m": result["pedestrian_avg_x_hour_21_500m"],
                        "motor_vehicle_avg_x_hour_21_500m": result["motor_vehicle_avg_x_hour_21_500m"],
                        "at_rest_avg_x_hour_22_500m": result["at_rest_avg_x_hour_22_500m"],
                        "pedestrian_avg_x_hour_22_500m": result["pedestrian_avg_x_hour_22_500m"],
                        "motor_vehicle_avg_x_hour_22_500m": result["motor_vehicle_avg_x_hour_22_500m"],
                        "at_rest_avg_x_hour_23_500m": result["at_rest_avg_x_hour_23_500m"],
                        "pedestrian_avg_x_hour_23_500m": result["pedestrian_avg_x_hour_23_500m"],
                        "motor_vehicle_avg_x_hour_23_500m": result["motor_vehicle_avg_x_hour_23_500m"]
                        } 
                        }
                    
                    resp.append( {
                                "property_details": property_details,
                                "market_info": market_info,
                                "pois": pois,
                                "traffic": traffic
                            })
                return resp
            except Exception as e:
                logger.error("Error processing property JSON: %s", str(e), exc_info=True)
                raise e







